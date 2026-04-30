#!/usr/bin/env python3
"""
Complete render_worker.py

- Polls AWS SQS for render jobs.
- Downloads input images from S3 URLs, S3 keys, or HTTP/HTTPS URLs.
- Pre-scales large images to output size.
- Creates a slideshow MP4 with FFmpeg.
- Uses safer dynamic FFmpeg timeouts.
- Reports job and per-image progress to RENDER_CALLBACK_URL.
- Uploads the final MP4 to S3.
- Deletes SQS message only after successful render/upload/final callback.

Required env:
  SQS_QUEUE_URL
  OUTPUT_BUCKET or job output.bucket

Common env:
  AWS_DEFAULT_REGION=us-east-1
  OUTPUT_PREFIX=bigpicture360.net/renders
  RENDER_CALLBACK_URL=https://...
  RENDER_WORKER_SECRET=...
  POLL_INTERVAL=5
  MAX_CONCURRENT=1
  VISIBILITY_TIMEOUT=3600
  MIN_FREE_DISK_GB=2
  LOG_LEVEL=INFO
  FFMPEG_MIN_TIMEOUT_SECONDS=1800
  FFMPEG_MAX_TIMEOUT_SECONDS=43200
  FFMPEG_VISIBILITY_BUFFER_SECONDS=900
"""

from __future__ import annotations

import concurrent.futures
import hashlib
import json
import logging
import math
import mimetypes
import os
import re
import selectors
import shutil
import subprocess
import sys
import tempfile
import time
import traceback
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlparse, unquote

import boto3
import requests
from botocore.exceptions import ClientError


AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
SQS_QUEUE_URL = os.getenv("SQS_QUEUE_URL", "").strip()

OUTPUT_BUCKET = os.getenv("OUTPUT_BUCKET", os.getenv("S3_OUTPUT_BUCKET", "")).strip()
OUTPUT_PREFIX = os.getenv("OUTPUT_PREFIX", os.getenv("S3_OUTPUT_PREFIX", "bigpicture360.net/renders")).strip().strip("/")

RENDER_CALLBACK_URL = os.getenv("RENDER_CALLBACK_URL", "").strip()
RENDER_WORKER_SECRET = os.getenv("RENDER_WORKER_SECRET", "").strip()

POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "5"))
MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT", "1"))
VISIBILITY_TIMEOUT = int(os.getenv("VISIBILITY_TIMEOUT", "3600"))
MIN_FREE_DISK_GB = float(os.getenv("MIN_FREE_DISK_GB", "2"))

FFMPEG_MIN_TIMEOUT_SECONDS = int(os.getenv("FFMPEG_MIN_TIMEOUT_SECONDS", "1800"))
FFMPEG_MAX_TIMEOUT_SECONDS = int(os.getenv("FFMPEG_MAX_TIMEOUT_SECONDS", "43200"))
FFMPEG_VISIBILITY_BUFFER_SECONDS = int(os.getenv("FFMPEG_VISIBILITY_BUFFER_SECONDS", "900"))
PROGRESS_CALLBACK_MIN_INTERVAL_SECONDS = float(os.getenv("PROGRESS_CALLBACK_MIN_INTERVAL_SECONDS", "2.0"))

# Persistent worker storage. Do not use /tmp for long-running renders/caches because
# some EC2 images mount /tmp as tmpfs (RAM-backed).
RENDER_WORK_DIR = Path(os.getenv("RENDER_WORK_DIR", "/mnt/render-work")).expanduser()
RENDER_CACHE_DIR = Path(os.getenv("RENDER_CACHE_DIR", "/mnt/render-cache")).expanduser()
IMAGE_CACHE_ENABLED = os.getenv("IMAGE_CACHE_ENABLED", "true").strip().lower() not in {"0", "false", "no", "off"}
IMAGE_CACHE_MAX_AGE_DAYS = int(os.getenv("IMAGE_CACHE_MAX_AGE_DAYS", "14"))
IMAGE_CACHE_MAX_GB = float(os.getenv("IMAGE_CACHE_MAX_GB", "30"))
CACHE_SCHEMA_VERSION = "v2"

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("render-worker")

sqs = boto3.client("sqs", region_name=AWS_DEFAULT_REGION)
s3 = boto3.client("s3", region_name=AWS_DEFAULT_REGION)


def log_event(event: str, **fields: Any) -> None:
    logger.info("%s | %s", event, json.dumps(fields, default=str, ensure_ascii=False))


def safe_int(value: Any, default: int) -> int:
    try:
        if value is None or value == "":
            return default
        return int(value)
    except Exception:
        return default


def safe_float(value: Any, default: float) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


@dataclass
class MotionSpec:
    effect: str
    duration: float
    easing: str

def normalize_transition_style(settings: dict[str, Any]) -> str:
    if truthy_setting(os.getenv("DISABLE_XFADE_TRANSITIONS", "true")):
        return "none"

def post_render_callback(payload: dict[str, Any]) -> None:
    if not RENDER_CALLBACK_URL:
        return

    headers = {"Content-Type": "application/json"}
    if RENDER_WORKER_SECRET:
        headers["x-worker-secret"] = RENDER_WORKER_SECRET

    try:
        requests.post(RENDER_CALLBACK_URL, json=payload, headers=headers, timeout=15).raise_for_status()
    except Exception as exc:
        log_event("progress_callback_failed", error=str(exc), payload_preview=str(payload)[:1000])


class ProgressReporter:
    def __init__(self, job_id: str, total_images: int):
        self.job_id = job_id
        self.total_images = max(1, int(total_images))
        self.last_callback_at = 0.0
        self.last_payload_key = ""

    def emit(
        self,
        *,
        stage: str,
        status: str = "processing",
        progress_percent: Optional[int] = None,
        image_index: Optional[int] = None,
        filename: str = "",
        image_status: str = "",
        image_percent: Optional[int] = None,
        message: str = "",
        error: str = "",
        force: bool = False,
        extra: Optional[dict[str, Any]] = None,
    ) -> None:
        now = time.time()

        if progress_percent is None:
            progress_percent = self._estimate_overall_percent(stage, image_index, image_percent)

        payload: dict[str, Any] = {
            "jobId": self.job_id,
            "status": status,
            "stage": stage,
            "progress_percent": int(max(0, min(100, progress_percent))),
            "message": message,
            "updated_at": int(now),
        }

        if image_index is not None:
            payload["image_progress"] = {
                "image_index": int(image_index),
                "total_images": self.total_images,
                "filename": filename,
                "stage": stage,
                "status": image_status or status,
                "percent": int(max(0, min(100, image_percent if image_percent is not None else 0))),
                "message": message,
                "error": error,
            }

        if error:
            payload["error"] = error
        if extra:
            payload.update(extra)

        payload_key = json.dumps(payload, sort_keys=True, default=str)
        should_emit = force or (
            payload_key != self.last_payload_key
            and (now - self.last_callback_at) >= PROGRESS_CALLBACK_MIN_INTERVAL_SECONDS
        )
        if not should_emit:
            return

        self.last_callback_at = now
        self.last_payload_key = payload_key
        log_event("render_progress", **payload)
        post_render_callback(payload)

    def image(
        self,
        *,
        image_index: int,
        filename: str,
        stage: str,
        image_status: str,
        image_percent: int,
        message: str = "",
        error: str = "",
        force: bool = False,
    ) -> None:
        self.emit(
            stage=stage,
            status="failed" if image_status == "failed" else "processing",
            image_index=image_index,
            filename=filename,
            image_status=image_status,
            image_percent=image_percent,
            message=message,
            error=error,
            force=force,
        )

    def _estimate_overall_percent(self, stage: str, image_index: Optional[int], image_percent: Optional[int]) -> int:
        idx = max(0, (image_index or 1) - 1)
        img_pct = max(0, min(100, image_percent if image_percent is not None else 0)) / 100.0
        image_fraction = (idx + img_pct) / self.total_images
        if stage in ("queued", "starting"):
            return 1
        if stage in ("downloading", "downloaded", "image_download"):
            return int(2 + image_fraction * 28)
        if stage in ("processing", "processed", "image_processing"):
            return int(30 + image_fraction * 20)
        if stage in ("encoding", "ffmpeg_encoding"):
            return int(50 + image_fraction * 45)
        if stage in ("uploading", "finalizing"):
            return 96
        if stage in ("complete", "succeeded"):
            return 100
        if stage == "failed":
            return 0
        return int(max(0, min(95, image_fraction * 95)))


@dataclass
class RenderJob:
    job_id: str
    images: list[Any]
    settings: dict[str, Any]
    per_image_settings: list[dict[str, Any]]
    output_bucket: str
    output_key: str
    raw: dict[str, Any]


def parse_message_body(body: str) -> dict[str, Any]:
    try:
        data = json.loads(body)
    except json.JSONDecodeError:
        raise ValueError(f"SQS message body is not valid JSON: {body[:500]}")
    if isinstance(data, dict) and "Message" in data and isinstance(data["Message"], str):
        try:
            return json.loads(data["Message"])
        except Exception:
            pass
    if not isinstance(data, dict):
        raise ValueError("SQS message body must be a JSON object")
    return data


MOTION_SETTING_KEYS = {
    "effect",
    "motion",
    "motion_sequence",
    "motionSequence",
    "motions",
    "camera_motion",
    "cameraMotion",
    "motion_type",
    "motionType",
    "ffmpeg_motion_filter_required",
    "ffmpeg_motion_filter_chain",
}


def has_motion_settings(value: Any) -> bool:
    return isinstance(value, dict) and any(key in value for key in MOTION_SETTING_KEYS)


def collect_motion_settings_by_image(*roots: Any) -> dict[str, dict[str, Any]]:
    by_id: dict[str, dict[str, Any]] = {}
    container_keys = {
        "image_settings",
        "imageSettings",
        "image_overrides",
        "imageOverrides",
        "per_image_settings",
        "perImageSettings",
        "per_image_motion_settings",
        "perImageMotionSettings",
        "motion_settings_by_image",
        "motionSettingsByImage",
        "image_motion_settings",
        "imageMotionSettings",
        "motions_by_image",
        "motionsByImage",
    }

    def add_map(mapping: Any) -> None:
        if not isinstance(mapping, dict):
            return
        for key, value in mapping.items():
            if has_motion_settings(value):
                by_id[str(key)] = dict(value)

    for root in roots:
        if not isinstance(root, dict):
            continue
        add_map(root)
        for key in container_keys:
            add_map(root.get(key))
        for list_key in ("slides", "items", "imageItems", "image_items"):
            value = root.get(list_key)
            if not isinstance(value, list):
                continue
            for item in value:
                if not has_motion_settings(item):
                    continue
                image_id = item.get("image_id") or item.get("imageId") or item.get("id") or item.get("uuid")
                if image_id:
                    by_id[str(image_id)] = dict(item)

    return by_id


def image_ref_ids(image_ref: Any, index: int) -> list[str]:
    ids = [str(index), str(index - 1)]
    if isinstance(image_ref, dict):
        for key in ("image_id", "imageId", "id", "uuid", "asset_id", "assetId", "file_id", "fileId"):
            value = image_ref.get(key)
            if value:
                ids.append(str(value))
        for key in ("storage_path", "s3_key", "object_key", "key", "path", "url", "imageUrl", "image_url", "src"):
            value = image_ref.get(key)
            if value:
                text = str(value)
                ids.extend([text, Path(text).name, Path(text).stem])
    elif isinstance(image_ref, str):
        ids.extend([image_ref, Path(image_ref).name, Path(image_ref).stem])
    return [item for item in dict.fromkeys(ids) if item]


def resolve_per_image_settings(images: list[Any], settings: dict[str, Any], raw: dict[str, Any]) -> list[dict[str, Any]]:
    by_id = collect_motion_settings_by_image(settings, raw)
    ordered_motion_settings = list(by_id.values())
    resolved: list[dict[str, Any]] = []
    for idx, image_ref in enumerate(images, start=1):
        merged: dict[str, Any] = {}
        if has_motion_settings(image_ref):
            merged.update(dict(image_ref))
        matched_by_id = False
        for image_id in image_ref_ids(image_ref, idx):
            if image_id in by_id:
                merged.update(by_id[image_id])
                matched_by_id = True
                break
        if not matched_by_id and not merged and len(ordered_motion_settings) == len(images):
            merged.update(ordered_motion_settings[idx - 1])
        resolved.append(merged)
    return resolved


def merge_settings(base: dict[str, Any], override: Optional[dict[str, Any]]) -> dict[str, Any]:
    if not override:
        return dict(base)
    merged = dict(base)
    merged.update(override)
    return merged


def settings_with_per_image_motion(base: dict[str, Any], per_image_settings: list[dict[str, Any]]) -> dict[str, Any]:
    merged = dict(base)
    if normalize_motion_sequence(merged, safe_float(merged.get("seconds_per_image") or merged.get("duration_per_image") or merged.get("image_duration"), 3.0)):
        return merged
    for item in per_image_settings:
        if has_motion_settings(item):
            merged.update(item)
            break
    return merged


def extract_job(data: dict[str, Any]) -> RenderJob:
    job_id = data.get("jobId") or data.get("job_id") or data.get("id") or str(uuid.uuid4())

    settings = data.get("settings") or data.get("renderSettings") or {}
    if not isinstance(settings, dict):
        settings = {}

    payload_keys = sorted(data.keys()) if isinstance(data, dict) else []
    logger.info(
        "received_job_payload_keys | %s",
        json.dumps({
            "jobId": str(job_id),
            "keys": payload_keys,
            "has_images": isinstance(data.get("images"), list) and len(data.get("images", [])) > 0,
            "has_imageUrls": isinstance(data.get("imageUrls"), list) and len(data.get("imageUrls", [])) > 0,
            "has_image_urls": isinstance(data.get("image_urls"), list) and len(data.get("image_urls", [])) > 0,
            "has_files": isinstance(data.get("files"), list) and len(data.get("files", [])) > 0,
            "has_input_images": isinstance(data.get("input_images"), list) and len(data.get("input_images", [])) > 0,
        })
    )

    image_fields = ["images", "imageUrls", "image_urls", "files", "input_images"]
    images = []
    found_image_field = None

    for field in image_fields:
        value = data.get(field)

        if isinstance(value, str) and value.strip():
            images = [value]
            found_image_field = field
            break

        if isinstance(value, list) and len(value) > 0:
            images = value
            found_image_field = field
            break

    if not images:
        raise ValueError(
            "Job payload does not contain a usable image list. "
            f"Expected one of: {', '.join(image_fields)}. "
            f"Received payload keys: {payload_keys}"
        )

    logger.info(
        "resolved_job_image_field | %s",
        json.dumps({
            "jobId": str(job_id),
            "field": found_image_field,
            "image_count": len(images),
        })
    )

    per_image_settings = resolve_per_image_settings(images, settings, data)
    per_image_motion_count = sum(1 for item in per_image_settings if has_motion_settings(item))
    if per_image_motion_count:
        log_event(
            "resolved_per_image_motion_settings",
            job_id=str(job_id),
            image_count=len(images),
            per_image_motion_count=per_image_motion_count,
        )

    output = data.get("output") if isinstance(data.get("output"), dict) else {}
    output_bucket = output.get("bucket") or data.get("outputBucket") or data.get("output_bucket") or OUTPUT_BUCKET
    output_key = output.get("key") or data.get("outputKey") or data.get("output_key") or f"{OUTPUT_PREFIX}/{job_id}.mp4"

    if not output_bucket:
        raise ValueError("Missing output bucket. Set OUTPUT_BUCKET env or provide output.bucket in job.")

    return RenderJob(str(job_id), images, settings, per_image_settings, str(output_bucket), str(output_key).lstrip("/"), data)


def parse_s3_uri(uri: str) -> tuple[str, str]:
    parsed = urlparse(uri)
    if parsed.scheme != "s3":
        raise ValueError(f"Not an s3 URI: {uri}")
    bucket = parsed.netloc
    key = unquote(parsed.path.lstrip("/"))
    if not bucket or not key:
        raise ValueError(f"Invalid s3 URI: {uri}")
    return bucket, key


def parse_s3_https_url(url: str) -> Optional[tuple[str, str]]:
    parsed = urlparse(url)
    host = parsed.netloc
    path = unquote(parsed.path.lstrip("/"))
    m = re.match(r"^(.+)\.s3(?:[.-][a-z0-9-]+)?\.amazonaws\.com$", host)
    if m and path:
        return m.group(1), path
    m = re.match(r"^s3(?:[.-][a-z0-9-]+)?\.amazonaws\.com$", host)
    if m and "/" in path:
        bucket, key = path.split("/", 1)
        return bucket, key
    return None


def stable_json_hash(value: Any) -> str:
    data = json.dumps(value, sort_keys=True, default=str, ensure_ascii=False)
    return hashlib.sha256(data.encode("utf-8")).hexdigest()


def image_ref_display_name(image_ref: Any, index: int) -> str:
    if isinstance(image_ref, dict):
        for key in ("filename", "name", "file_name", "original_filename"):
            value = image_ref.get(key)
            if value:
                return str(value)
        for key in ("storage_path", "s3_key", "object_key", "key", "path", "url", "imageUrl"):
            value = image_ref.get(key)
            if value:
                return Path(str(value)).name or f"{index - 1:05d}.JPG"
    if isinstance(image_ref, str):
        return Path(image_ref).name or f"{index - 1:05d}.JPG"
    return f"{index - 1:05d}.JPG"


def image_cache_identity(image_ref: Any) -> dict[str, Any]:
    """
    Build a stable identity for the source image content.

    Preferred validation fields from backend:
      image_id/id, storage_path/s3_key, updated_at, etag/content_hash, file_size.
    Reordering does not affect this identity because sort_order is deliberately excluded.
    """
    if isinstance(image_ref, dict):
        source = (
            image_ref.get("storage_path")
            or image_ref.get("s3_key")
            or image_ref.get("object_key")
            or image_ref.get("key")
            or image_ref.get("path")
            or image_ref.get("render_source_path")
            or image_ref.get("s3Uri")
            or image_ref.get("s3_uri")
            or image_ref.get("url")
            or image_ref.get("imageUrl")
            or image_ref.get("image_url")
            or image_ref.get("downloadUrl")
            or image_ref.get("download_url")
            or image_ref.get("src")
        )
        return {
            "schema": CACHE_SCHEMA_VERSION,
            "image_id": image_ref.get("image_id") or image_ref.get("imageId") or image_ref.get("id"),
            "source": source,
            "bucket": image_ref.get("bucket") or image_ref.get("s3_bucket") or OUTPUT_BUCKET,
            "updated_at": image_ref.get("updated_at") or image_ref.get("updatedAt") or image_ref.get("modified_at") or image_ref.get("last_modified"),
            "etag": image_ref.get("etag") or image_ref.get("eTag") or image_ref.get("content_hash") or image_ref.get("hash"),
            "file_size": image_ref.get("file_size") or image_ref.get("fileSize") or image_ref.get("size") or image_ref.get("size_bytes"),
        }

    return {
        "schema": CACHE_SCHEMA_VERSION,
        "source": str(image_ref),
        "bucket": OUTPUT_BUCKET,
    }


def image_cache_key(image_ref: Any) -> str:
    return stable_json_hash(image_cache_identity(image_ref))


def ensure_worker_storage() -> None:
    RENDER_WORK_DIR.mkdir(parents=True, exist_ok=True)
    if IMAGE_CACHE_ENABLED:
        (RENDER_CACHE_DIR / "images").mkdir(parents=True, exist_ok=True)
        (RENDER_CACHE_DIR / "processed").mkdir(parents=True, exist_ok=True)
        cleanup_render_cache()


def cache_paths_for_image(cache_key: str, suffix: str = ".jpg") -> tuple[Path, Path]:
    suffix = suffix if suffix.startswith(".") else f".{suffix}"
    image_path = RENDER_CACHE_DIR / "images" / f"{cache_key}{suffix.lower()}"
    meta_path = RENDER_CACHE_DIR / "images" / f"{cache_key}.json"
    return image_path, meta_path


def preprocess_target_size(settings: dict[str, Any]) -> tuple[int, int, str]:
    output_width, output_height = parse_output_size(settings)
    try:
        default_seconds = safe_float(settings.get("seconds_per_image") or settings.get("duration_per_image") or settings.get("image_duration"), 3.0)
        motion_enabled = bool(normalize_motion_sequence(settings, default_seconds))
    except Exception:
        motion_enabled = False

    if not motion_enabled:
        return output_width, output_height, "output"

    scale = safe_float(settings.get("motion_work_scale") or settings.get("motionWorkScale"), 0.0)
    if scale <= 0:
        scale = safe_float(os.getenv("MOTION_WORK_SCALE"), 2.0)
    scale = max(1.0, min(scale, 4.0))

    max_width = safe_int(settings.get("motion_max_width") or settings.get("motionMaxWidth"), 0)
    max_height = safe_int(settings.get("motion_max_height") or settings.get("motionMaxHeight"), 0)
    if max_width <= 0:
        max_width = safe_int(os.getenv("MOTION_MAX_WORK_WIDTH"), 4096)
    if max_height <= 0:
        max_height = safe_int(os.getenv("MOTION_MAX_WORK_HEIGHT"), 2304)

    target_width = min(int(round(output_width * scale)), max_width)
    target_height = min(int(round(output_height * scale)), max_height)
    target_width = max(output_width, target_width)
    target_height = max(output_height, target_height)
    target_width += target_width % 2
    target_height += target_height % 2
    return target_width, target_height, f"motion-{scale:g}x"


def processed_cache_path(cache_key: str, settings: dict[str, Any]) -> Path:
    preprocess_width, preprocess_height, preprocess_mode = preprocess_target_size(settings)
    processed_identity = {
        "schema": CACHE_SCHEMA_VERSION,
        "source_cache_key": cache_key,
        "preprocess_width": preprocess_width,
        "preprocess_height": preprocess_height,
        "preprocess_mode": preprocess_mode,
        "preprocess": "scale-decrease-pad-setsar-q2-lanczos",
    }
    processed_key = stable_json_hash(processed_identity)
    return RENDER_CACHE_DIR / "processed" / f"{processed_key}.jpg"


def write_cache_metadata(meta_path: Path, identity: dict[str, Any], cached_file: Path) -> None:
    payload = {
        "identity": identity,
        "cached_file": str(cached_file),
        "size_bytes": cached_file.stat().st_size if cached_file.exists() else 0,
        "created_at": int(time.time()),
        "last_used_at": int(time.time()),
    }
    meta_path.write_text(json.dumps(payload, sort_keys=True, default=str), encoding="utf-8")


def touch_cache_metadata(meta_path: Path) -> None:
    try:
        payload = json.loads(meta_path.read_text(encoding="utf-8"))
        payload["last_used_at"] = int(time.time())
        meta_path.write_text(json.dumps(payload, sort_keys=True, default=str), encoding="utf-8")
    except Exception:
        pass


def valid_cached_image(image_ref: Any, cached_file: Path, meta_path: Path) -> bool:
    if not IMAGE_CACHE_ENABLED or not cached_file.exists() or not meta_path.exists() or cached_file.stat().st_size <= 0:
        return False
    try:
        payload = json.loads(meta_path.read_text(encoding="utf-8"))
        return payload.get("identity") == image_cache_identity(image_ref)
    except Exception:
        return False


def cleanup_render_cache() -> None:
    if not IMAGE_CACHE_ENABLED or IMAGE_CACHE_MAX_GB <= 0:
        return

    now = time.time()
    max_age_seconds = max(1, IMAGE_CACHE_MAX_AGE_DAYS) * 86400
    cache_files = []
    for folder in (RENDER_CACHE_DIR / "images", RENDER_CACHE_DIR / "processed"):
        if not folder.exists():
            continue
        for path in folder.iterdir():
            if not path.is_file():
                continue
            try:
                stat = path.stat()
            except FileNotFoundError:
                continue
            if now - stat.st_mtime > max_age_seconds:
                try:
                    path.unlink()
                    continue
                except Exception:
                    pass
            cache_files.append((path, stat.st_size, stat.st_mtime))

    max_bytes = int(IMAGE_CACHE_MAX_GB * 1024 * 1024 * 1024)
    total = sum(size for _, size, _ in cache_files)
    if total <= max_bytes:
        return

    for path, size, _mtime in sorted(cache_files, key=lambda item: item[2]):
        try:
            path.unlink()
            total -= size
            if total <= max_bytes:
                break
        except Exception:
            pass


def download_one_image(image_ref, dest):
    if isinstance(image_ref, dict):
        url_ref = (
            image_ref.get("url")
            or image_ref.get("imageUrl")
            or image_ref.get("image_url")
            or image_ref.get("downloadUrl")
            or image_ref.get("download_url")
            or image_ref.get("src")
        )
        if url_ref:
            download_one_image(str(url_ref), dest)
            return

        s3_uri = image_ref.get("s3Uri") or image_ref.get("s3_uri")
        if s3_uri:
            download_one_image(str(s3_uri), dest)
            return

        s3_key = (
            image_ref.get("s3_key")
            or image_ref.get("object_key")
            or image_ref.get("key")
            or image_ref.get("path")
            or image_ref.get("render_source_path")
            or image_ref.get("storage_path")
        )
        bucket = image_ref.get("bucket") or image_ref.get("s3_bucket") or OUTPUT_BUCKET
        if s3_key and bucket:
            s3.download_file(bucket, str(s3_key).replace(f"s3://{bucket}/", ""), str(dest))
            return

    if isinstance(image_ref, str):
        ref = image_ref.strip()

        if ref.startswith("s3://"):
            without_scheme = ref[5:]
            bucket, key = without_scheme.split("/", 1)
            s3.download_file(bucket, key, str(dest))
            return

        if ref.startswith(("http://", "https://")):
            s3_ref = parse_s3_https_url(ref)
            if s3_ref:
                bucket, key = s3_ref
                s3.download_file(bucket, key, str(dest))
                return

            r = requests.get(ref, timeout=120)
            r.raise_for_status()
            Path(dest).write_bytes(r.content)
            return

        if OUTPUT_BUCKET:
            s3.download_file(OUTPUT_BUCKET, ref, str(dest))
            return

    raise ValueError(f"Unsupported image reference: {str(image_ref)[:200]}")

def upload_file_to_s3(local_path: Path, bucket: str, key: str) -> str:
    s3.upload_file(str(local_path), bucket, key, ExtraArgs={"ContentType": "video/mp4"})
    return f"s3://{bucket}/{key}"


def ensure_free_disk(path: Path) -> None:
    usage = shutil.disk_usage(str(path))
    free_gb = usage.free / (1024 ** 3)
    if free_gb < MIN_FREE_DISK_GB:
        raise RuntimeError(f"Not enough free disk: {free_gb:.2f} GB available, need {MIN_FREE_DISK_GB:.2f} GB")


def change_message_visibility(receipt_handle: str, timeout_seconds: int, job_id: str) -> None:
    try:
        sqs.change_message_visibility(QueueUrl=SQS_QUEUE_URL, ReceiptHandle=receipt_handle, VisibilityTimeout=int(timeout_seconds))
        log_event("message_visibility_extended", job_id=job_id, visibility_timeout=timeout_seconds)
    except Exception as exc:
        log_event("message_visibility_extend_failed", job_id=job_id, error=str(exc))


def estimate_render_timeout_seconds(n_images: int, settings: dict[str, Any], per_image_settings: Optional[list[dict[str, Any]]] = None) -> int:
    n_images = max(1, int(n_images))
    if per_image_settings:
        settings = settings_with_per_image_motion(settings, per_image_settings)
    min_timeout = FFMPEG_MIN_TIMEOUT_SECONDS
    worker_max_timeout = FFMPEG_MAX_TIMEOUT_SECONDS
    frontend_max_timeout = safe_int(settings.get("max_timeout_seconds"), worker_max_timeout)
    max_timeout = min(frontend_max_timeout, worker_max_timeout)

    requested = safe_int(settings.get("timeout_seconds"), 0)
    if requested > 0:
        return max(min_timeout, min(requested, max_timeout))

    seconds_per_image = safe_float(settings.get("seconds_per_image") or settings.get("duration_per_image") or settings.get("image_duration"), 3.0)
    hold_seconds, move_seconds, timing_seconds_per_image = resolve_motion_timing(settings, seconds_per_image)
    if hold_seconds + move_seconds > 0:
        seconds_per_image = timing_seconds_per_image
    motion_sequence = normalize_motion_sequence(settings, seconds_per_image)
    if motion_sequence:
        seconds_per_image = motion_duration_for_settings(settings, motion_sequence, seconds_per_image)
    fps = safe_int(settings.get("fps") or settings.get("output_fps"), 30)
    output_width = safe_int(settings.get("output_width") or settings.get("width"), 1920)
    output_height = safe_int(settings.get("output_height") or settings.get("height"), 1080)
    source_width = safe_int(settings.get("source_width") or settings.get("max_source_width"), output_width)
    source_height = safe_int(settings.get("source_height") or settings.get("max_source_height"), output_height)

    video_seconds = max(1.0, n_images * seconds_per_image)
    timeout_multiplier = safe_float(settings.get("timeout_multiplier"), 12.0)

    output_pixels = max(1, output_width * output_height)
    source_pixels = max(output_pixels, source_width * source_height)
    pixels_factor = max(1.0, math.sqrt(source_pixels / output_pixels))
    if not (settings.get("source_width") or settings.get("max_source_width")):
        pixels_factor = max(pixels_factor, safe_float(settings.get("source_pixels_factor"), 4.0))

    fps_factor = max(1.0, fps / 30.0)
    effect_factor = 1.0
    if motion_sequence or settings.get("ken_burns", True) or settings.get("motion") or settings.get("motion_enabled"):
        effect_factor *= 1.5
    if settings.get("parallax") or settings.get("zoom_blur") or settings.get("stabilize"):
        effect_factor *= 1.5
    if normalize_color_grade(settings) != "none" or truthy_setting(settings.get("vignette")) or truthy_setting(settings.get("film_grain")) or truthy_setting(settings.get("filmGrain")):
        effect_factor *= 1.15

    timeout = int(video_seconds * timeout_multiplier * pixels_factor * fps_factor * effect_factor)
    return min(max_timeout, max(min_timeout, timeout))


def get_ffmpeg_bin() -> str:
    return os.getenv("FFMPEG_BIN", "ffmpeg")


def truthy_setting(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on", "enabled"}
    return bool(value)


def parse_output_size(settings: dict[str, Any]) -> tuple[int, int]:
    resolution = str(settings.get("resolution") or "").strip().lower()
    if "x" in resolution:
        try:
            width_text, height_text = resolution.split("x", 1)
            width = int(width_text.strip())
            height = int(height_text.strip())
            if width > 0 and height > 0:
                return width, height
        except Exception:
            pass

    return (
        safe_int(settings.get("output_width") or settings.get("width"), 1920),
        safe_int(settings.get("output_height") or settings.get("height"), 1080),
    )


def normalize_motion_effect(settings: dict[str, Any]) -> str:
    candidates = [
        settings.get("effect"),
        settings.get("camera_motion"),
        settings.get("cameraMotion"),
        settings.get("motion_type"),
        settings.get("motionType"),
        settings.get("motion"),
        settings.get("animation"),
        settings.get("animation_type"),
    ]

    for value in candidates:
        if isinstance(value, str) and value.strip():
            normalized = re.sub(r"[^a-z0-9]+", "_", value.strip().lower()).strip("_")
            aliases = {
                "none": "static",
                "off": "static",
                "false": "static",
                "still": "static",
                "static": "static",
                "blurredfill": "blurred_fill",
                "blurred_fill": "blurred_fill",
                "blur_fill": "blurred_fill",
                "pushin": "push_in",
                "push_in": "push_in",
                "push": "push_in",
                "zoomin": "push_in",
                "zoom_in": "push_in",
                "pullout": "pull_out",
                "pull_out": "pull_out",
                "pushout": "pull_out",
                "push_out": "pull_out",
                "zoomout": "pull_out",
                "zoom_out": "pull_out",
                "kenburns": "ken_burns",
                "ken_burns": "ken_burns",
                "ken_burns_in": "push_in",
                "ken_burns_out": "pull_out",
                "pan": "pan",
                "panleft": "pan_left",
                "pan_left": "pan_left",
                "panright": "pan_right",
                "pan_right": "pan_right",
                "panup": "pan_up",
                "pan_up": "pan_up",
                "pandown": "pan_down",
                "pan_down": "pan_down",
                "diagonalpan": "diagonal_pan",
                "diagonal_pan": "diagonal_pan",
                "panzoom": "pan_zoom",
                "pan_zoom": "pan_zoom",
                "whippan": "whip_pan",
                "whip_pan": "whip_pan",
                "holdmove": "hold_then_move",
                "hold_move": "hold_then_move",
                "hold_then_move": "hold_then_move",
                "movehold": "move_then_hold",
                "move_hold": "move_then_hold",
                "move_then_hold": "move_then_hold",
                "rotationdrift": "rotation_drift",
                "rotation_drift": "rotation_drift",
                "handhelddrift": "handheld_drift",
                "handheld_drift": "handheld_drift",
                "zoom_pulse": "zoom_pulse",
                "zoompulse": "zoom_pulse",
            }
            effect = aliases.get(normalized, normalized)
            if effect == "ken_burns":
                direction = str(settings.get("ken_burns_direction") or settings.get("kenBurnsDirection") or settings.get("direction") or "random").strip().lower()
                if direction in {"in", "push_in", "push-in", "zoom_in", "zoom-in"}:
                    return "push_in"
                if direction in {"out", "push_out", "push-out", "pull_out", "pull-out", "zoom_out", "zoom-out"}:
                    return "pull_out"
                return "ken_burns"
            if effect == "pan":
                direction = str(settings.get("pan_direction") or settings.get("panDirection") or settings.get("direction") or "").strip().lower()
                direction = re.sub(r"[^a-z0-9]+", "_", direction).strip("_")
                if direction in {"left", "right", "up", "down"}:
                    return f"pan_{direction}"
            return effect

    if truthy_setting(settings.get("ken_burns")) or truthy_setting(settings.get("kenBurns")):
        direction = str(settings.get("ken_burns_direction") or settings.get("kenBurnsDirection") or "in").strip().lower()
        if direction in {"in", "push_in", "push-in", "zoom_in", "zoom-in"}:
            return "push_in"
        if direction in {"out", "push_out", "push-out", "pull_out", "pull-out", "zoom_out", "zoom-out"}:
            return "pull_out"
        return "ken_burns"

    if truthy_setting(settings.get("motion_enabled")) or truthy_setting(settings.get("motionEnabled")):
        return "ken_burns"

    return "static"


def normalize_motion_easing(settings: dict[str, Any]) -> str:
    candidates = [
        settings.get("motion_easing"),
        settings.get("motionEasing"),
        settings.get("easing"),
        settings.get("ease"),
        settings.get("motion_ease"),
        settings.get("motionEase"),
    ]
    for value in candidates:
        if isinstance(value, str) and value.strip():
            normalized = re.sub(r"[^a-z0-9]+", "_", value.strip().lower()).strip("_")
            aliases = {
                "none": "linear",
                "constant": "linear",
                "linear": "linear",
                "ease": "ease_in_out",
                "smooth": "ease_in_out",
                "smooth_move": "ease_in_out",
                "cinematic": "ease_in_out",
                "easein": "ease_in",
                "ease_in": "ease_in",
                "easeout": "ease_out",
                "ease_out": "ease_out",
                "easeinout": "ease_in_out",
                "ease_in_out": "ease_in_out",
                "ease_inout": "ease_in_out",
            }
            return aliases.get(normalized, normalized)
    return "ease_in_out"


def normalize_motion_sequence(settings: dict[str, Any], default_seconds_per_image: float) -> list[MotionSpec]:
    raw_motions = (
        settings.get("motions")
        or settings.get("motion_sequence")
        or settings.get("motionSequence")
        or settings.get("camera_motions")
        or settings.get("cameraMotions")
    )

    if isinstance(raw_motions, str) and raw_motions.strip():
        try:
            raw_motions = json.loads(raw_motions)
        except Exception:
            raw_motions = None

    if isinstance(raw_motions, list) and raw_motions:
        specs: list[MotionSpec] = []
        fallback_duration = max(0.1, default_seconds_per_image)
        for item in raw_motions:
            motion_settings = dict(settings)
            if isinstance(item, dict):
                motion_settings.update(item)
                duration_value = (
                    item.get("duration")
                    or item.get("seconds")
                    or item.get("duration_seconds")
                    or item.get("durationSeconds")
                )
            else:
                motion_settings["effect"] = str(item)
                duration_value = None

            effect = normalize_motion_effect(motion_settings)
            if effect in {"static", "none", ""}:
                continue

            duration = safe_float(duration_value, fallback_duration)
            if duration <= 0:
                duration = fallback_duration

            specs.append(MotionSpec(effect=effect, duration=max(0.1, duration), easing=normalize_motion_easing(motion_settings)))

        return specs

    effect = normalize_motion_effect(settings)
    if effect in {"static", "none", ""}:
        return []

    return [MotionSpec(effect=effect, duration=max(0.1, default_seconds_per_image), easing=normalize_motion_easing(settings))]


def motion_sequence_total_duration(motions: list[MotionSpec], default_seconds_per_image: float) -> float:
    if not motions:
        return max(0.1, default_seconds_per_image)
    return max(0.1, max(max(0.1, motion.duration) for motion in motions))


def motion_duration_for_settings(settings: dict[str, Any], motions: list[MotionSpec], default_seconds_per_image: float) -> float:
    requested_total = safe_float(settings.get("motion_sequence_total_duration") or settings.get("motionSequenceTotalDuration"), 0.0)
    if requested_total > 0:
        return requested_total
    combination_mode = str(settings.get("motion_combination_mode") or settings.get("motionCombinationMode") or "").strip().lower()
    if combination_mode == "sequence" and motions:
        return max(0.1, sum(max(0.1, motion.duration) for motion in motions))
    return motion_sequence_total_duration(motions, default_seconds_per_image)


def motion_progress_expr(hold_frames: int, move_frames: int, easing: str) -> str:
    hold = max(0, int(hold_frames))
    move = max(1, int(move_frames))
    t = f"min(1,max(0,(on-{hold})/{move}))"

    if easing == "linear":
        eased = t
    elif easing == "ease_in":
        eased = f"pow({t},2)"
    elif easing == "ease_out":
        eased = f"1-pow(1-({t}),2)"
    else:
        eased = f"0.5-0.5*cos(PI*({t}))"

    return f"if(lt(on,{hold}),0,{eased})"


def resolve_motion_timing(settings: dict[str, Any], default_seconds_per_image: float) -> tuple[float, float, float]:
    hold_seconds = safe_float(settings.get("hold_duration") or settings.get("holdDuration"), 0.0)
    move_seconds = safe_float(settings.get("move_duration") or settings.get("moveDuration"), 0.0)

    preset = settings.get("timing_preset") or settings.get("timingPreset") or settings.get("motion_timing_preset")
    if isinstance(preset, str) and preset.strip() and hold_seconds <= 0 and move_seconds <= 0:
        normalized = re.sub(r"[^a-z0-9]+", "_", preset.strip().lower()).strip("_")
        presets = {
            "short_hold": (0.25, 2.5),
            "smooth_move": (0.5, 2.5),
            "cinematic": (1.0, 3.5),
        }
        hold_seconds, move_seconds = presets.get(normalized, (hold_seconds, move_seconds))

    if hold_seconds + move_seconds <= 0:
        return 0.0, max(0.1, default_seconds_per_image), max(0.1, default_seconds_per_image)

    hold_seconds = max(0.0, hold_seconds)
    move_seconds = max(0.1, move_seconds)
    return hold_seconds, move_seconds, hold_seconds + move_seconds


def motion_filter_for_effect(
    effect: str,
    idx: int,
    frames_per_image: int,
    fps: int,
    seconds_per_image: float,
    output_width: int,
    output_height: int,
    hold_frames: int,
    move_frames: int,
    easing: str,
) -> tuple[str, str]:
    effect = effect or "push_in"
    if effect == "ken_burns":
        effect = "push_in" if idx % 2 == 0 else "pull_out"
    if effect == "pan":
        effect = "pan_left" if idx % 2 == 0 else "pan_right"

    center_x = "'floor((iw-iw/zoom)/2)'"
    center_y = "'floor((ih-ih/zoom)/2)'"    
    
    n = max(1, frames_per_image)
    hold = max(0, min(hold_frames, n - 1))
    move_n = max(1, min(move_frames, n - hold))
    progress = motion_progress_expr(hold, move_n, easing)
    immediate_progress = motion_progress_expr(0, n, easing)

    zoom = f"'1.0+0.15*({progress})'"
    
    x = center_x
    y = center_y
    post_filter = ""

    if effect == "pull_out":
        zoom = f"'1.15-0.15*({progress})'"
    elif effect == "zoom_pulse":
        zoom = f"'1.0+0.06*sin(({progress})*PI)'"
    elif effect in {"pan_left", "pan_right", "pan_up", "pan_down"}:
        zoom = "'1.12'"
        if effect == "pan_left":
            x = f"'(iw-iw/zoom)*(1-({progress}))'"
        elif effect == "pan_right":
            x = f"'floor((iw-iw/zoom)*({progress}))'"
        elif effect == "pan_up":
            y = f"'floor((ih-ih/zoom)*(1-({progress})))'"
        else:
            y = f"'(ih-ih/zoom)*({progress})'"
    elif effect == "diagonal_pan":
        zoom = "'1.14'"
        x = f"'floor((iw-iw/zoom)*({progress}))'"
        y = f"'(ih-ih/zoom)*({progress})'"
    elif effect == "pan_zoom":
        zoom = f"'1.06+0.10*({progress})'"
        x = f"'floor((iw-iw/zoom)*({progress}))'"
        y = center_y
    elif effect == "whip_pan":
        zoom = "'1.22'"
        x = f"'(iw-iw/zoom)*pow({immediate_progress},0.35)'"
        y = center_y
    elif effect == "hold_then_move":
        zoom = f"'1.0+0.15*({progress})'"
    elif effect == "move_then_hold":
        move_then_hold_progress = motion_progress_expr(0, move_n, easing)
        zoom = f"'1.0+0.15*({move_then_hold_progress})'"
    elif effect == "rotation_drift":
        zoom = f"'1.03+0.07*({progress})'"
        post_filter = f",rotate=0.012*sin(2*PI*t/{max(0.1, seconds_per_image):.3f}):ow={output_width}:oh={output_height}:fillcolor=black"
    elif effect == "handheld_drift":
        zoom = "'1.08'"
        x = f"'iw/2-(iw/zoom/2)+8*sin(on/9)'"
        y = f"'ih/2-(ih/zoom/2)+6*cos(on/11)'"

    return f"zoompan=z={zoom}:x={x}:y={y}:d={n}:s={output_width}x{output_height}:fps={fps}{post_filter}", effect


def motion_filter_for_effects(
    motions: list[MotionSpec],
    idx: int,
    frames_per_image: int,
    fps: int,
    seconds_per_image: float,
    output_width: int,
    output_height: int,
    hold_frames: int,
    move_frames: int,
) -> tuple[str, str]:
    if len(motions) == 1:
        motion = motions[0]
        return motion_filter_for_effect(
            motion.effect,
            idx,
            frames_per_image,
            fps,
            seconds_per_image,
            output_width,
            output_height,
            hold_frames,
            move_frames,
            motion.easing,
        )

    center_x = "floor((iw-iw/zoom)/2)"
    center_y = "floor((ih-ih/zoom)/2)"
    n = max(1, frames_per_image)
    zoom = "1.0"
    x = center_x
    y = center_y
    post_filters: list[str] = []
    effects: list[str] = []
    has_zoom_motion = False
    pan_min_zoom = 1.0

    for motion_idx, motion in enumerate(motions):
        effect = motion.effect or "push_in"
        if effect == "ken_burns":
            effect = "push_in" if (idx + motion_idx) % 2 == 0 else "pull_out"
        if effect == "pan":
            effect = "pan_left" if (idx + motion_idx) % 2 == 0 else "pan_right"

        move_n = max(1, min(n, int(round(max(0.1, motion.duration) * fps))))
        progress = motion_progress_expr(0, move_n, motion.easing)
        immediate_progress = motion_progress_expr(0, n, motion.easing)
        effects.append(effect)

        if effect == "push_in":
            zoom = f"1.0+0.15*({progress})"
            has_zoom_motion = True
        elif effect == "pull_out":
            zoom = f"1.15-0.15*({progress})"
            has_zoom_motion = True
        elif effect == "zoom_pulse":
            zoom = f"1.0+0.06*sin(({progress})*PI)"
            has_zoom_motion = True
        elif effect == "hold_then_move":
            zoom = f"1.0+0.15*({progress})"
            has_zoom_motion = True
        elif effect == "move_then_hold":
            zoom = f"1.0+0.15*({progress})"
            has_zoom_motion = True
        elif effect == "rotation_drift":
            zoom = f"1.03+0.07*({progress})"
            has_zoom_motion = True
            post_filters.append(f"rotate=0.012*sin(2*PI*t/{max(0.1, seconds_per_image):.3f}):ow={output_width}:oh={output_height}:fillcolor=black")
        elif effect == "pan_zoom":
            zoom = f"1.06+0.10*({progress})"
            x = f"floor((iw-iw/zoom)*({progress}))"
            y = center_y
            has_zoom_motion = True
        elif effect in {"pan_left", "pan_right", "pan_up", "pan_down"}:
            pan_min_zoom = max(pan_min_zoom, 1.12)
            if effect == "pan_left":
                x = f"(iw-iw/zoom)*(1-({progress}))"
            elif effect == "pan_right":
                x = f"floor((iw-iw/zoom)*({progress}))"
            elif effect == "pan_up":
                y = f"floor((ih-ih/zoom)*(1-({progress})))"
            else:
                y = f"(ih-ih/zoom)*({progress})"
        elif effect == "diagonal_pan":
            pan_min_zoom = max(pan_min_zoom, 1.14)
            x = f"floor((iw-iw/zoom)*({progress}))"
            y = f"(ih-ih/zoom)*({progress})"
        elif effect == "whip_pan":
            pan_min_zoom = max(pan_min_zoom, 1.22)
            x = f"(iw-iw/zoom)*pow({immediate_progress},0.35)"
            y = center_y
        elif effect == "handheld_drift":
            pan_min_zoom = max(pan_min_zoom, 1.08)
            x = "iw/2-(iw/zoom/2)+8*sin(on/9)"
            y = "ih/2-(ih/zoom/2)+6*cos(on/11)"

    if not has_zoom_motion and pan_min_zoom > 1.0:
        zoom = f"{pan_min_zoom:.2f}"

    post_filter = f",{','.join(post_filters)}" if post_filters else ""
    return f"zoompan=z='{zoom}':x='{x}':y='{y}':d={n}:s={output_width}x{output_height}:fps={fps}{post_filter}", "+".join(effects)


def normalize_transition_style(settings: dict[str, Any]) -> str:
    candidates = [
        settings.get("transition"),
        settings.get("transition_style"),
        settings.get("transitionStyle"),
        settings.get("transition_type"),
        settings.get("transitionType"),
    ]
    for value in candidates:
        if isinstance(value, str) and value.strip():
            normalized = re.sub(r"[^a-z0-9]+", "_", value.strip().lower()).strip("_")
            aliases = {
                "none": "none",
                "off": "none",
                "false": "none",
                "cut": "none",
                "hard_cut": "none",
                "crossfade": "fade",
                "cross_fade": "fade",
                "fade": "fade",
                "wipeleft": "wipeleft",
                "wipe_left": "wipeleft",
                "wiperight": "wiperight",
                "wipe_right": "wiperight",
                "radialwipe": "radial",
                "radial_wipe": "radial",
                "wipe_radial": "radial",
                "radial": "radial",
                "fadetoblack": "fadeblack",
                "fade_to_black": "fadeblack",
                "fadeblack": "fadeblack",
                "fade_black": "fadeblack",
                "fadetowhite": "fadewhite",
                "fade_to_white": "fadewhite",
                "fadewhite": "fadewhite",
                "fade_white": "fadewhite",
                "zoom": "zoomin",
                "zoomin": "zoomin",
                "zoom_in": "zoomin",
                "slide": "slideleft",
                "slidepush": "slideleft",
                "slide_push": "slideleft",
                "slideleft": "slideleft",
                "slide_left": "slideleft",
            }
            return aliases.get(normalized, normalized)
    return "none"


def combine_video_labels(
    filters: list[str],
    labels: list[str],
    seconds_per_image: float | list[float],
    transition_style: str,
    transition_duration: float,
    fps: int = 30,
) -> str:
    """
    Combine prepared per-image video labels into one output stream.

    FFmpeg's xfade requires constant-frame-rate inputs. Image and zoompan streams can
    lose that metadata, so each segment and chained transition output is normalized.
    Set ENABLE_XFADE_TRANSITIONS=false to force hard-cut concat for troubleshooting.
    """
    if not labels:
        raise ValueError("No video labels to combine")

    fps = max(1, int(fps or 30))
    cfr_filter = f"settb=AVTB,setpts=PTS-STARTPTS,fps={fps},format=yuv420p"
    if isinstance(seconds_per_image, list):
        durations = [max(0.1, float(duration or 3.0)) for duration in seconds_per_image]
        if len(durations) < len(labels):
            durations.extend([durations[-1] if durations else 3.0] * (len(labels) - len(durations)))
        durations = durations[:len(labels)]
    else:
        duration = max(0.1, float(seconds_per_image or 3.0))
        durations = [duration for _label in labels]
    transition_style = transition_style or "none"

    # Normalize every segment into a clean constant-frame-rate clip before combining.
    cfr_labels: list[str] = []
    for idx, label in enumerate(labels):
        out = f"cfr{idx}"
        filters.append(
            f"[{label}]"
            f"trim=duration={durations[idx]:.3f},{cfr_filter}"
            f"[{out}]"
        )
        cfr_labels.append(out)

    if len(cfr_labels) == 1:
        filters.append(
            f"[{cfr_labels[0]}]"
            f"{cfr_filter}[v]"
        )
        return "v"

    allow_xfade = os.getenv("ENABLE_XFADE_TRANSITIONS", "true").strip().lower() in {"1", "true", "yes", "on"}

    # Safe troubleshooting path: concat renders successfully but intentionally ignores transitions.
    if transition_style == "none" or not allow_xfade:
        if transition_style != "none" and not allow_xfade:
            log_event(
                "xfade_disabled_using_concat",
                transition=transition_style,
                reason="ENABLE_XFADE_TRANSITIONS=false",
            )
        concat_inputs = "".join(f"[{label}]" for label in cfr_labels)
        filters.append(
            f"{concat_inputs}concat=n={len(cfr_labels)}:v=1:a=0,"
            f"{cfr_filter}[v]"
        )
        return "v"

    transition_duration = max(0.0, float(transition_duration or 0.0))
    max_transition = max(0.0, min(durations) - 0.05)
    transition_duration = min(transition_duration, max_transition)
    if transition_duration <= 0:
        concat_inputs = "".join(f"[{label}]" for label in cfr_labels)
        filters.append(
            f"{concat_inputs}concat=n={len(cfr_labels)}:v=1:a=0,"
            f"{cfr_filter}[v]"
        )
        return "v"

    current = cfr_labels[0]
    offset = max(0.05, durations[0] - transition_duration)
    for idx, label in enumerate(cfr_labels[1:], start=1):
        left = f"xfl{idx}"
        right = f"xfr{idx}"
        raw_out = f"xfraw{idx}"
        out = f"xf{idx}"

        filters.append(f"[{current}]{cfr_filter}[{left}]")
        filters.append(f"[{label}]{cfr_filter}[{right}]")
        filters.append(
            f"[{left}][{right}]"
            f"xfade=transition={transition_style}:duration={transition_duration:.3f}:offset={offset:.3f}"
            f"[{raw_out}]"
        )
        filters.append(f"[{raw_out}]{cfr_filter}[{out}]")
        current = out
        if idx < len(durations) - 1:
            offset += max(0.05, durations[idx] - transition_duration)

    filters.append(f"[{current}]{cfr_filter}[v]")
    return "v"

def normalize_color_grade(settings: dict[str, Any]) -> str:
    candidates = [
        settings.get("color_grade"),
        settings.get("colorGrade"),
        settings.get("color_style"),
        settings.get("colorStyle"),
        settings.get("grade"),
    ]
    for value in candidates:
        if isinstance(value, str) and value.strip():
            normalized = re.sub(r"[^a-z0-9]+", "_", value.strip().lower()).strip("_")
            aliases = {
                "": "none",
                "none": "none",
                "off": "none",
                "false": "none",
                "original": "none",
                "natural": "none",
                "warm": "warm",
                "cool": "cool",
                "cinematic": "cinematic",
                "bw": "bw",
                "b_w": "bw",
                "black_white": "bw",
                "black_and_white": "bw",
                "monochrome": "bw",
                "grayscale": "bw",
                "greyscale": "bw",
                "sepia": "sepia",
                "vintage": "vintage",
            }
            return aliases.get(normalized, normalized)
    return "none"


def color_style_filters(settings: dict[str, Any]) -> list[str]:
    grade = normalize_color_grade(settings)
    grade_filters = {
        "warm": "colortemperature=temperature=6800,eq=saturation=1.15",
        "cool": "colortemperature=temperature=4500,eq=saturation=1.1",
        "cinematic": "curves=preset=cross_process,eq=contrast=1.1:saturation=0.9",
        "bw": "hue=s=0",
        "sepia": "colorchannelmixer=.393:.769:.189:0:.349:.686:.168:0:.272:.534:.131",
        "vintage": "curves=preset=vintage,eq=saturation=0.8:contrast=1.05",
    }

    filters: list[str] = []
    grade_filter = grade_filters.get(grade)
    if grade_filter:
        filters.append(grade_filter)
    if truthy_setting(settings.get("vignette")):
        filters.append("vignette=PI/4")
    if truthy_setting(settings.get("film_grain")) or truthy_setting(settings.get("filmGrain")):
        filters.append("noise=alls=12:allf=t+u")
    return filters


def apply_color_style_to_label(filters: list[str], label: str, settings: dict[str, Any]) -> str:
    style_filters = color_style_filters(settings)
    if not style_filters:
        return label
    out = "styled"
    filters.append(f"[{label}]{','.join(style_filters)},format=yuv420p[{out}]")
    return out


def preprocess_images(job_id: str, image_files: list[Path], processed_dir: Path, settings: dict[str, Any], reporter: ProgressReporter, cache_keys: Optional[list[str]] = None) -> list[Path]:
    processed_dir.mkdir(parents=True, exist_ok=True)
    preprocess_width, preprocess_height, preprocess_mode = preprocess_target_size(settings)
    processed_files: list[Path] = []
    log_event(
        "image_preprocess_target",
        job_id=job_id,
        width=preprocess_width,
        height=preprocess_height,
        mode=preprocess_mode,
    )

    for idx, src in enumerate(image_files, start=1):
        dest = processed_dir / f"{idx - 1:05d}.jpg"
        cache_key = cache_keys[idx - 1] if cache_keys and idx - 1 < len(cache_keys) else stable_json_hash(str(src))

        if IMAGE_CACHE_ENABLED:
            cached_processed = processed_cache_path(cache_key, settings)
            if cached_processed.exists() and cached_processed.stat().st_size > 0:
                shutil.copy2(cached_processed, dest)
                processed_files.append(dest)
                reporter.image(
                    image_index=idx,
                    filename=src.name,
                    stage="image_processing",
                    image_status="complete",
                    image_percent=100,
                    message=f"Using cached processed image {idx}/{len(image_files)}",
                    force=True,
                )
                log_event("processed_image_cache_hit", job_id=job_id, image_index=idx, cache_key=cache_key, cached_file=str(cached_processed))
                continue

        reporter.image(image_index=idx, filename=src.name, stage="image_processing", image_status="processing", image_percent=5, message=f"Processing image {idx}/{len(image_files)}", force=True)
        vf = (
            f"scale={preprocess_width}:{preprocess_height}:force_original_aspect_ratio=decrease:flags=lanczos,"
            f"pad={preprocess_width}:{preprocess_height}:(ow-iw)/2:(oh-ih)/2,setsar=1"
        )
        cmd = [get_ffmpeg_bin(), "-hide_banner", "-y", "-i", str(src), "-vf", vf, "-frames:v", "1", "-q:v", "2", str(dest)]
        try:
            subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=300)
        except subprocess.CalledProcessError as exc:
            raise RuntimeError(f"Image preprocess failed for {src.name}: returncode={exc.returncode}; stderr={exc.stderr[-3000:] if exc.stderr else ''}")
        except subprocess.TimeoutExpired:
            raise RuntimeError(f"Image preprocess timed out for {src.name}")

        if not dest.exists() or dest.stat().st_size == 0:
            raise RuntimeError(f"Processed image is missing/empty: {dest}")

        if IMAGE_CACHE_ENABLED:
            try:
                cached_processed = processed_cache_path(cache_key, settings)
                cached_processed.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(dest, cached_processed)
                log_event("processed_image_cache_saved", job_id=job_id, image_index=idx, cache_key=cache_key, cached_file=str(cached_processed))
            except Exception as exc:
                log_event("processed_image_cache_save_failed", job_id=job_id, image_index=idx, error=str(exc))

        processed_files.append(dest)
        reporter.image(image_index=idx, filename=src.name, stage="image_processing", image_status="complete", image_percent=100, message=f"Processed image {idx}/{len(image_files)}", force=True)

    log_event("image_preprocess_complete", job_id=job_id, count=len(processed_files))
    return processed_files

def build_ffmpeg_command(image_files: list[Path], output_path: Path, settings: dict[str, Any], per_image_settings: Optional[list[dict[str, Any]]] = None) -> tuple[list[str], float]:
    stage_settings = settings_with_per_image_motion(settings, per_image_settings or [])
    fps = safe_int(settings.get("fps") or settings.get("output_fps"), 30)
    seconds_per_image = safe_float(stage_settings.get("seconds_per_image") or stage_settings.get("duration_per_image") or stage_settings.get("image_duration"), 3.0)
    hold_seconds, move_seconds, timing_seconds_per_image = resolve_motion_timing(stage_settings, seconds_per_image)
    if hold_seconds + move_seconds > 0:
        seconds_per_image = timing_seconds_per_image
    crf = safe_int(settings.get("crf"), 20)
    preset = str(settings.get("preset") or os.getenv("FFMPEG_PRESET", "veryfast"))
    output_width, output_height = parse_output_size(stage_settings)
    motion_input_width, motion_input_height, motion_input_mode = preprocess_target_size(stage_settings)
    motion_sequence = normalize_motion_sequence(stage_settings, seconds_per_image)
    if motion_sequence:
        seconds_per_image = motion_duration_for_settings(stage_settings, motion_sequence, seconds_per_image)
    motion_effect = "combined" if len(motion_sequence) > 1 else (motion_sequence[0].effect if motion_sequence else "static")
    motion_easing = "mixed" if len({motion.easing for motion in motion_sequence}) > 1 else (motion_sequence[0].easing if motion_sequence else normalize_motion_easing(stage_settings))
    transition_style = normalize_transition_style(stage_settings)
    transition_duration = safe_float(
        stage_settings.get("transition_duration") or stage_settings.get("transitionDuration"),
        0.75 if transition_style != "none" else 0.0,
    )

    if motion_sequence:
        cmd = [get_ffmpeg_bin(), "-hide_banner", "-y"]
        image_settings_list = [
            merge_settings(stage_settings, per_image_settings[idx] if per_image_settings and idx < len(per_image_settings) else None)
            for idx in range(len(image_files))
        ]
        image_durations: list[float] = []
        for image_settings in image_settings_list:
            image_seconds = safe_float(image_settings.get("seconds_per_image") or image_settings.get("duration_per_image") or image_settings.get("image_duration"), seconds_per_image)
            image_hold_seconds, image_move_seconds, image_timing_seconds = resolve_motion_timing(image_settings, image_seconds)
            if image_hold_seconds + image_move_seconds > 0:
                image_seconds = image_timing_seconds
            image_motion_sequence = normalize_motion_sequence(image_settings, image_seconds) or motion_sequence
            if image_motion_sequence:
                image_seconds = motion_duration_for_settings(image_settings, image_motion_sequence, image_seconds)
            image_durations.append(max(0.1, image_seconds))

        for img, image_seconds in zip(image_files, image_durations):
            cmd.extend(["-loop", "1", "-t", f"{image_seconds:.3f}", "-i", str(img)])

        filters: list[str] = []
        motion_labels: list[str] = []
        hold_frames = int(round(max(0.0, hold_seconds) * fps))
        move_frames = max(1, int(round(max(0.1, move_seconds) * fps)))
        for idx, _img in enumerate(image_files):
            image_settings = image_settings_list[idx]
            image_seconds = image_durations[idx]
            image_motion_sequence = normalize_motion_sequence(image_settings, image_seconds) or motion_sequence
            image_hold_seconds, image_move_seconds, _image_timing_seconds = resolve_motion_timing(image_settings, image_seconds)
            image_hold_frames = int(round(max(0.0, image_hold_seconds) * fps))
            image_move_frames = max(1, int(round(max(0.1, image_move_seconds or image_seconds) * fps)))
            base = f"base{idx}"
            out = f"m{idx}"
            if len(image_motion_sequence) == 1 and image_motion_sequence[0].effect == "blurred_fill":
                bg = f"bg{idx}"
                fg = f"fg{idx}"
                filters.append(f"[{idx}:v]split=2[{bg}][{fg}]")
                filters.append(
                    f"[{bg}]scale={output_width}:{output_height}:force_original_aspect_ratio=increase,"
                    f"crop={output_width}:{output_height},boxblur=24:2,setsar=1[bgx{idx}]"
                )
                filters.append(
                    f"[{fg}]scale={output_width}:{output_height}:force_original_aspect_ratio=decrease,"
                    f"setsar=1[fgx{idx}]"
                )
                filters.append(
                    f"[bgx{idx}][fgx{idx}]overlay=(W-w)/2:(H-h)/2,fps={fps},"
                    f"trim=duration={image_seconds:.3f},setpts=PTS-STARTPTS,format=yuv420p[{out}]"
                )
            else:
                filters.append(
                    f"[{idx}:v]"
                    f"scale={motion_input_width}:{motion_input_height}:force_original_aspect_ratio=increase:flags=lanczos,"
                    f"crop={motion_input_width}:{motion_input_height},setsar=1,fps={fps}"
                    f"[{base}]"
                )
                motion_filter, effect = motion_filter_for_effects(
                    image_motion_sequence,
                    idx,
                    max(1, int(round(image_seconds * fps))),
                    fps,
                    image_seconds,
                    output_width,
                    output_height,
                    image_hold_frames if image_hold_seconds > 0 else hold_frames,
                    image_move_frames if image_move_seconds > 0 else move_frames,
                )
                filters.append(
                    f"[{base}]{motion_filter},"
                    f"trim=duration={image_seconds:.3f},setpts=PTS-STARTPTS,format=yuv420p"
                    f"[{out}]"
                )
            motion_labels.append(out)

        final_label = combine_video_labels(filters, motion_labels, image_durations, transition_style, transition_duration, fps)
        final_label = apply_color_style_to_label(filters, final_label, stage_settings)

        cmd.extend([
            "-filter_complex", ";".join(filters),
            "-map", f"[{final_label}]",
            "-c:v", "libx264", "-preset", preset, "-crf", str(crf),
            "-pix_fmt", "yuv420p", "-r", str(fps), "-movflags", "+faststart",
            "-progress", "pipe:1", "-nostats",
            str(output_path),
        ])
        average_image_seconds = sum(image_durations) / max(1, len(image_durations))
        log_event("ffmpeg_motion_enabled", effect=motion_effect, motions=[motion.__dict__ for motion in motion_sequence], per_image_motion_count=sum(1 for item in (per_image_settings or []) if has_motion_settings(item)), motion_easing=motion_easing, hold_seconds=hold_seconds, move_seconds=move_seconds, motion_input_width=motion_input_width, motion_input_height=motion_input_height, motion_input_mode=motion_input_mode, transition=transition_style, transition_duration=transition_duration, color_grade=normalize_color_grade(stage_settings), vignette=truthy_setting(stage_settings.get("vignette")), film_grain=truthy_setting(stage_settings.get("film_grain")) or truthy_setting(stage_settings.get("filmGrain")), fps=fps, seconds_per_image=average_image_seconds, image_durations=image_durations, images=len(image_files))
        return cmd, average_image_seconds

    if transition_style != "none" and len(image_files) > 1:
        cmd = [get_ffmpeg_bin(), "-hide_banner", "-y"]
        for img in image_files:
            cmd.extend(["-loop", "1", "-t", f"{seconds_per_image:.3f}", "-i", str(img)])

        filters: list[str] = []
        labels: list[str] = []
        for idx, _img in enumerate(image_files):
            label = f"s{idx}"
            filters.append(
                f"[{idx}:v]"
                f"scale={output_width}:{output_height}:force_original_aspect_ratio=decrease,"
                f"pad={output_width}:{output_height}:(ow-iw)/2:(oh-ih)/2,setsar=1,fps={fps},"
                f"trim=duration={seconds_per_image:.3f},setpts=PTS-STARTPTS,format=yuv420p"
                f"[{label}]"
            )
            labels.append(label)

        final_label = combine_video_labels(filters, labels, seconds_per_image, transition_style, transition_duration, fps)
        final_label = apply_color_style_to_label(filters, final_label, settings)
        cmd.extend([
            "-filter_complex", ";".join(filters),
            "-map", f"[{final_label}]",
            "-c:v", "libx264", "-preset", preset, "-crf", str(crf),
            "-pix_fmt", "yuv420p", "-r", str(fps), "-movflags", "+faststart",
            "-progress", "pipe:1", "-nostats",
            str(output_path),
        ])
        log_event("ffmpeg_transition_enabled", transition=transition_style, transition_duration=transition_duration, color_grade=normalize_color_grade(settings), vignette=truthy_setting(settings.get("vignette")), film_grain=truthy_setting(settings.get("film_grain")) or truthy_setting(settings.get("filmGrain")), fps=fps, seconds_per_image=seconds_per_image, images=len(image_files))
        return cmd, seconds_per_image

    list_path = output_path.parent / "concat_images.txt"

    with open(list_path, "w", encoding="utf-8") as f:
        for img in image_files:
            escaped = str(img).replace("'", "'\\''")
            f.write(f"file '{escaped}'\n")
            f.write(f"duration {seconds_per_image:.3f}\n")
        escaped_last = str(image_files[-1]).replace("'", "'\\''")
        f.write(f"file '{escaped_last}'\n")

    static_filters = [f"fps={fps}", *color_style_filters(settings), "format=yuv420p"]
    cmd = [
        get_ffmpeg_bin(), "-hide_banner", "-y",
        "-f", "concat", "-safe", "0", "-i", str(list_path),
        "-vf", ",".join(static_filters),
        "-c:v", "libx264", "-preset", preset, "-crf", str(crf),
        "-pix_fmt", "yuv420p", "-r", str(fps), "-movflags", "+faststart",
        "-progress", "pipe:1", "-nostats",
        str(output_path),
    ]
    return cmd, seconds_per_image


def parse_ffmpeg_progress_block(block: str) -> dict[str, str]:
    result: dict[str, str] = {}
    for line in block.splitlines():
        if "=" in line:
            key, value = line.split("=", 1)
            result[key.strip()] = value.strip()
    return result


def ffmpeg_out_time_seconds(progress: dict[str, str]) -> Optional[float]:
    try:
        if progress.get("out_time_ms"):
            return float(progress["out_time_ms"]) / 1_000_000.0
        if progress.get("out_time_us"):
            return float(progress["out_time_us"]) / 1_000_000.0
    except Exception:
        return None
    return None


def image_index_from_out_time(out_time_seconds: float, image_stage_seconds: float, total_images: int) -> tuple[int, int]:
    total_images = max(1, total_images)
    image_stage_seconds = max(0.1, image_stage_seconds)
    raw_index = int(out_time_seconds // image_stage_seconds)
    image_index = max(1, min(total_images, raw_index + 1))
    local_time = out_time_seconds - (raw_index * image_stage_seconds)
    image_percent = int(max(0, min(100, (local_time / image_stage_seconds) * 100)))
    if image_index == total_images and out_time_seconds >= image_stage_seconds * total_images:
        image_percent = 100
    return image_index, image_percent


def run_ffmpeg_with_progress(cmd: list[str], job_id: str, timeout_seconds: int, total_images: int, image_stage_seconds: float, reporter: ProgressReporter, image_files: Optional[list[Path]] = None) -> None:
    started_at = time.time()
    last_progress: dict[str, str] = {}
    last_stderr_tail: list[str] = []
    last_image_index: Optional[int] = None
    log_event("job_ffmpeg_start", job_id=job_id, timeout_seconds=timeout_seconds, total_images=total_images, command_preview=" ".join(cmd[:80]) + (" ..." if len(cmd) > 80 else ""))
    reporter.emit(stage="ffmpeg_encoding", status="processing", progress_percent=50, message="Starting FFmpeg encoding", force=True)

    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1, universal_newlines=True)
    sel = selectors.DefaultSelector()
    assert proc.stdout is not None and proc.stderr is not None
    sel.register(proc.stdout, selectors.EVENT_READ, data="stdout")
    sel.register(proc.stderr, selectors.EVENT_READ, data="stderr")
    progress_lines: list[str] = []

    try:
        while True:
            elapsed = time.time() - started_at
            if elapsed > timeout_seconds:
                try:
                    proc.kill()
                    proc.wait(timeout=10)
                except Exception:
                    pass
                raise RuntimeError(f"FFmpeg timed out after {timeout_seconds}s; elapsed={elapsed:.1f}s; last_progress={json.dumps(last_progress, default=str)[:1500]}; stderr_tail={' | '.join(last_stderr_tail[-30:])[:5000]}")

            for key, _ in sel.select(timeout=1.0):
                stream_name = key.data
                line = key.fileobj.readline()
                if not line:
                    continue
                line = line.rstrip("\n")
                if stream_name == "stderr":
                    if line:
                        last_stderr_tail.append(line)
                        last_stderr_tail = last_stderr_tail[-100:]
                        if "deprecated pixel format used" in line:
                            log_event("ffmpeg_warning", job_id=job_id, warning=line[:1000])
                    continue

                progress_lines.append(line)
                if line.startswith("progress="):
                    block = "\n".join(progress_lines)
                    progress_lines.clear()
                    progress = parse_ffmpeg_progress_block(block)
                    last_progress = progress
                    out_sec = ffmpeg_out_time_seconds(progress)
                    speed = progress.get("speed", "")
                    frame = progress.get("frame", "")
                    progress_state = progress.get("progress", "")
                    extra = {"ffmpeg": {"out_time_seconds": out_sec, "speed": speed, "frame": frame, "progress": progress_state, "elapsed_seconds": round(elapsed, 1)}}
                    if out_sec is not None:
                        image_index, image_percent = image_index_from_out_time(out_sec, image_stage_seconds, total_images)
                        filename = image_files[image_index - 1].name if image_files and 1 <= image_index <= len(image_files) else ""
                        force = image_index != last_image_index
                        last_image_index = image_index
                        reporter.emit(stage="ffmpeg_encoding", status="processing", image_index=image_index, filename=filename, image_status="encoding", image_percent=image_percent, message=f"Encoding image {image_index}/{total_images}", extra=extra, force=force)
                    log_event("ffmpeg_progress_state", job_id=job_id, value=progress_state, frame=frame, out_time_seconds=out_sec, speed=speed, elapsed_seconds=round(elapsed, 1))

            returncode = proc.poll()
            if returncode is not None:
                break

        returncode = proc.wait(timeout=10)
        log_event("ffmpeg_finished", job_id=job_id, returncode=returncode, elapsed_seconds=round(time.time() - started_at, 2), last_progress=last_progress)
        if returncode != 0:
            raise RuntimeError(f"FFmpeg failed with return code {returncode}; last_progress={json.dumps(last_progress, default=str)[:1500]}; stderr_tail={' | '.join(last_stderr_tail[-40:])[:5000]}")
        reporter.emit(stage="ffmpeg_encoding", status="processing", progress_percent=95, image_index=total_images, filename=image_files[-1].name if image_files else "", image_status="complete", image_percent=100, message="FFmpeg encoding complete", force=True)
    finally:
        try:
            sel.close()
        except Exception:
            pass
        if proc.poll() is None:
            try:
                proc.kill()
            except Exception:
                pass


def is_xfade_filter_error(exc: Exception) -> bool:
    text = str(exc).lower()
    return "xfade" in text and (
        "constant frame rate" in text
        or "failed to configure output pad" in text
        or "error reinitializing filters" in text
        or "invalid argument" in text
    )


def download_images(job: RenderJob, work_dir: Path, reporter: ProgressReporter) -> tuple[list[Path], list[str]]:
    images_dir = work_dir / "images"
    images_dir.mkdir(parents=True, exist_ok=True)
    image_files: list[Path] = []
    cache_keys: list[str] = []

    for idx, image_ref in enumerate(job.images, start=1):
        display_name = image_ref_display_name(image_ref, idx)
        suffix = Path(display_name).suffix or ".jpg"
        dest = images_dir / f"{idx - 1:05d}{suffix}"
        cache_key = image_cache_key(image_ref)
        cache_keys.append(cache_key)

        cached_file, cached_meta = cache_paths_for_image(cache_key, suffix)
        if valid_cached_image(image_ref, cached_file, cached_meta):
            reporter.image(
                image_index=idx,
                filename=dest.name,
                stage="cached",
                image_status="complete",
                image_percent=100,
                message=f"Using cached image {idx}/{len(job.images)}",
                force=True,
            )
            shutil.copy2(cached_file, dest)
            touch_cache_metadata(cached_meta)
            log_event("image_cache_hit", job_id=job.job_id, image_index=idx, cache_key=cache_key, cached_file=str(cached_file))
            image_files.append(dest)
            continue

        reporter.image(image_index=idx, filename=dest.name, stage="downloading", image_status="processing", image_percent=5, message=f"Downloading image {idx}/{len(job.images)}", force=True)
        try:
            download_one_image(image_ref, dest)
        except Exception as exc:
            reporter.image(image_index=idx, filename=dest.name, stage="downloading", image_status="failed", image_percent=0, message=f"Failed downloading image {idx}/{len(job.images)}", error=str(exc), force=True)
            raise

        if not dest.exists() or dest.stat().st_size == 0:
            raise RuntimeError(f"Downloaded image is missing/empty: {dest}")

        if IMAGE_CACHE_ENABLED:
            try:
                cached_file.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(dest, cached_file)
                write_cache_metadata(cached_meta, image_cache_identity(image_ref), cached_file)
                log_event("image_cache_saved", job_id=job.job_id, image_index=idx, cache_key=cache_key, cached_file=str(cached_file))
            except Exception as exc:
                log_event("image_cache_save_failed", job_id=job.job_id, image_index=idx, error=str(exc))

        image_files.append(dest)
        reporter.image(image_index=idx, filename=dest.name, stage="downloaded", image_status="complete", image_percent=100, message=f"Downloaded image {idx}/{len(job.images)}", force=True)

    log_event("images_downloaded", job_id=job.job_id, count=len(image_files))
    return image_files, cache_keys

def render_job(job: RenderJob, receipt_handle: Optional[str] = None) -> dict[str, Any]:
    reporter = ProgressReporter(job_id=job.job_id, total_images=len(job.images))
    reporter.emit(stage="starting", status="processing", progress_percent=1, message="Render job started", force=True)
    ensure_worker_storage()
    ensure_free_disk(RENDER_WORK_DIR)
    render_settings = settings_with_per_image_motion(job.settings, job.per_image_settings)
    timeout_seconds = estimate_render_timeout_seconds(len(job.images), render_settings, job.per_image_settings)
    log_event("job_timeout_calculated", job_id=job.job_id, timeout_seconds=timeout_seconds, min_timeout_seconds=FFMPEG_MIN_TIMEOUT_SECONDS, max_timeout_seconds=FFMPEG_MAX_TIMEOUT_SECONDS, frontend_timeout_seconds=job.settings.get("timeout_seconds"), timeout_multiplier=job.settings.get("timeout_multiplier"), source_pixels_factor=job.settings.get("source_pixels_factor"), max_timeout_seconds_setting=job.settings.get("max_timeout_seconds"))
    if receipt_handle:
        dynamic_visibility = min(43200, max(VISIBILITY_TIMEOUT, timeout_seconds + FFMPEG_VISIBILITY_BUFFER_SECONDS))
        change_message_visibility(receipt_handle, dynamic_visibility, job.job_id)

    with tempfile.TemporaryDirectory(prefix=f"render-{job.job_id[:8]}-", dir=str(RENDER_WORK_DIR)) as tmp:
        work_dir = Path(tmp)
        image_files, cache_keys = download_images(job, work_dir, reporter)
        processed_files = preprocess_images(job.job_id, image_files, work_dir / "processed", render_settings, reporter, cache_keys)
        output_path = work_dir / "render_raw.mp4"
        cmd, image_stage_seconds = build_ffmpeg_command(processed_files, output_path, render_settings, job.per_image_settings)
        try:
            run_ffmpeg_with_progress(cmd, job.job_id, timeout_seconds, len(processed_files), image_stage_seconds, reporter, processed_files)
        except RuntimeError as exc:
            if not is_xfade_filter_error(exc):
                raise
            log_event("ffmpeg_xfade_failed_retrying_without_transition", job_id=job.job_id, error=str(exc)[:2000])
            reporter.emit(stage="ffmpeg_encoding", status="processing", progress_percent=50, message="Retrying FFmpeg without transitions", force=True)
            fallback_settings = dict(render_settings)
            fallback_settings["transition"] = "none"
            fallback_settings["transition_style"] = "none"
            fallback_settings["transitionStyle"] = "none"
            cmd, image_stage_seconds = build_ffmpeg_command(processed_files, output_path, fallback_settings, job.per_image_settings)
            run_ffmpeg_with_progress(cmd, job.job_id, timeout_seconds, len(processed_files), image_stage_seconds, reporter, processed_files)
        if not output_path.exists() or output_path.stat().st_size == 0:
            raise RuntimeError("Rendered output file missing or empty")
        reporter.emit(stage="uploading", status="processing", progress_percent=96, message="Uploading rendered video", force=True)
        s3_uri = upload_file_to_s3(output_path, job.output_bucket, job.output_key)
        result = {"jobId": job.job_id, "status": "completed", "bucket": job.output_bucket, "key": job.output_key, "s3Uri": s3_uri, "output_url": s3_uri, "size_bytes": output_path.stat().st_size}
        reporter.emit(stage="complete", status="completed", progress_percent=100, message="Render completed", extra=result, force=True)
        log_event("job_completed", **result)
        return result


def process_sqs_message(message: dict[str, Any]) -> None:
    receipt_handle = message["ReceiptHandle"]
    message_id = message.get("MessageId", "")
    try:
        payload = parse_message_body(message.get("Body", ""))
        job = extract_job(payload)
        log_event("message_received", message_id=message_id, job_id=job.job_id)
        render_job(job, receipt_handle=receipt_handle)
        sqs.delete_message(QueueUrl=SQS_QUEUE_URL, ReceiptHandle=receipt_handle)
        log_event("message_deleted", job_id=job.job_id, message_id=message_id)
    except Exception as exc:
        err = str(exc)
        log_event("job_failed_do_not_delete_sqs_message", message_id=message_id, error=err, traceback=traceback.format_exc()[-8000:])
        try:
            payload = parse_message_body(message.get("Body", ""))
            job_id = str(payload.get("jobId") or payload.get("job_id") or payload.get("id") or message_id)
            ProgressReporter(job_id=job_id, total_images=1).emit(stage="failed", status="failed", progress_percent=0, message="Render failed", error=err, force=True)
        except Exception:
            pass
        try:
            sqs.change_message_visibility(QueueUrl=SQS_QUEUE_URL, ReceiptHandle=receipt_handle, VisibilityTimeout=60)
        except Exception:
            pass


def poll_once(executor: concurrent.futures.Executor, futures: set[concurrent.futures.Future]) -> None:
    done = {f for f in futures if f.done()}
    for f in done:
        futures.remove(f)
        try:
            f.result()
        except Exception as exc:
            log_event("worker_future_error", error=str(exc), traceback=traceback.format_exc()[-4000:])

    available_slots = max(0, MAX_CONCURRENT - len(futures))
    if available_slots <= 0:
        return

    response = sqs.receive_message(QueueUrl=SQS_QUEUE_URL, MaxNumberOfMessages=min(10, available_slots), WaitTimeSeconds=20, VisibilityTimeout=VISIBILITY_TIMEOUT, MessageAttributeNames=["All"], AttributeNames=["All"])
    messages = response.get("Messages", [])
    if not messages:
        log_event("poll_empty")
        return
    for message in messages:
        futures.add(executor.submit(process_sqs_message, message))


def validate_startup() -> None:
    log_event("worker_starting", region=AWS_DEFAULT_REGION, queue_configured=bool(SQS_QUEUE_URL), output_bucket=OUTPUT_BUCKET, output_prefix=OUTPUT_PREFIX, max_concurrent=MAX_CONCURRENT, visibility_timeout=VISIBILITY_TIMEOUT, ffmpeg_min_timeout=FFMPEG_MIN_TIMEOUT_SECONDS, ffmpeg_max_timeout=FFMPEG_MAX_TIMEOUT_SECONDS)
    if not SQS_QUEUE_URL:
        raise RuntimeError("Missing SQS_QUEUE_URL environment variable")
    ensure_worker_storage()
    log_event("worker_storage_ready", work_dir=str(RENDER_WORK_DIR), cache_dir=str(RENDER_CACHE_DIR), image_cache_enabled=IMAGE_CACHE_ENABLED, cache_max_gb=IMAGE_CACHE_MAX_GB, cache_max_age_days=IMAGE_CACHE_MAX_AGE_DAYS)
    try:
        result = subprocess.run([get_ffmpeg_bin(), "-version"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=15)
        if result.returncode != 0:
            raise RuntimeError(result.stderr[:1000])
        log_event("ffmpeg_available", version=result.stdout.splitlines()[0] if result.stdout else "unknown")
    except Exception as exc:
        raise RuntimeError(f"FFmpeg is not available: {exc}")
    try:
        sqs.get_queue_attributes(QueueUrl=SQS_QUEUE_URL, AttributeNames=["QueueArn", "ApproximateNumberOfMessages"])
        log_event("sqs_access_ok")
    except ClientError as exc:
        raise RuntimeError(f"SQS access failed: {exc}") from exc


def main() -> None:
    validate_startup()
    futures: set[concurrent.futures.Future] = set()
    log_event("worker_started", status="alive")
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_CONCURRENT) as executor:
        last_heartbeat = 0.0
        while True:
            now = time.time()
            if now - last_heartbeat > 60:
                last_heartbeat = now
                log_event("worker_heartbeat", status="alive", active_jobs=len(futures))
            try:
                poll_once(executor, futures)
            except Exception as exc:
                log_event("poll_error", error=str(exc), traceback=traceback.format_exc()[-4000:])
                time.sleep(POLL_INTERVAL)
            time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        log_event("worker_fatal_error", error=str(exc), traceback=traceback.format_exc()[-8000:])
        raise
