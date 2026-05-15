#!/usr/bin/env python3
"""Dedicated worker for `kind: "video_branding"` SQS jobs."""

from __future__ import annotations

import concurrent.futures
import json
import logging
import mimetypes
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import traceback
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlparse

import boto3
import requests
from botocore.exceptions import ClientError

from render_worker import (
    build_watermark_post_assembly_command,
    get_ffmpeg_bin,
    normalize_watermark_spec,
    run_postprocess_command,
    truthy_setting,
)


AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
SQS_QUEUE_URL = os.getenv("VIDEO_BRANDING_SQS_QUEUE_URL", "").strip()

RENDER_CALLBACK_URL = os.getenv("RENDER_CALLBACK_URL", "").strip()
RENDER_WORKER_SECRET = os.getenv("RENDER_WORKER_SECRET", "").strip()

POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "5"))
MAX_CONCURRENT = int(os.getenv("VIDEO_BRANDING_MAX_CONCURRENT", os.getenv("MAX_CONCURRENT", "1")))
VISIBILITY_TIMEOUT = int(os.getenv("VIDEO_BRANDING_VISIBILITY_TIMEOUT", os.getenv("VISIBILITY_TIMEOUT", "3600")))
MIN_FREE_DISK_GB = float(os.getenv("MIN_FREE_DISK_GB", "2"))
RENDER_WORK_DIR = Path(os.getenv("VIDEO_BRANDING_WORK_DIR", os.getenv("RENDER_WORK_DIR", "/mnt/render-work"))).expanduser()

FFMPEG_TIMEOUT_SECONDS = int(os.getenv("VIDEO_BRANDING_FFMPEG_TIMEOUT_SECONDS", "7200"))
FFMPEG_VISIBILITY_BUFFER_SECONDS = int(os.getenv("FFMPEG_VISIBILITY_BUFFER_SECONDS", "900"))

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("video-branding-worker")

sqs = boto3.client("sqs", region_name=AWS_DEFAULT_REGION)
s3 = boto3.client("s3", region_name=AWS_DEFAULT_REGION)


def log_event(event: str, **fields: Any) -> None:
    logger.info("%s | %s", event, json.dumps(fields, default=str, ensure_ascii=False))


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(float(value))
    except Exception:
        return default


def ffmpeg_escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace(":", "\\:").replace("'", "\\'")


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


def emit_callback(job_id: str, status: str, stage: str, progress_percent: int, **extra: Any) -> None:
    payload: dict[str, Any] = {
        "kind": "video_branding",
        "jobId": job_id,
        "status": status,
        "stage": stage,
        "progress_percent": max(0, min(100, int(progress_percent))),
        "updated_at": int(time.time()),
    }
    payload.update(extra)
    log_event("video_branding_progress", **payload)
    if not RENDER_CALLBACK_URL:
        return
    headers = {"Content-Type": "application/json"}
    if RENDER_WORKER_SECRET:
        headers["x-worker-secret"] = RENDER_WORKER_SECRET
    try:
        requests.post(RENDER_CALLBACK_URL, json=payload, headers=headers, timeout=15).raise_for_status()
    except Exception as exc:
        log_event("video_branding_callback_failed", job_id=job_id, error=str(exc), payload_preview=str(payload)[:1000])


def ensure_worker_storage() -> None:
    RENDER_WORK_DIR.mkdir(parents=True, exist_ok=True)
    usage = shutil.disk_usage(str(RENDER_WORK_DIR))
    free_gb = usage.free / (1024 ** 3)
    if free_gb < MIN_FREE_DISK_GB:
        raise RuntimeError(f"Not enough free disk: {free_gb:.2f} GB available, need {MIN_FREE_DISK_GB:.2f} GB")


def change_message_visibility(receipt_handle: str, timeout_seconds: int, job_id: str) -> None:
    try:
        sqs.change_message_visibility(QueueUrl=SQS_QUEUE_URL, ReceiptHandle=receipt_handle, VisibilityTimeout=int(timeout_seconds))
        log_event("message_visibility_extended", job_id=job_id, visibility_timeout=timeout_seconds)
    except Exception as exc:
        log_event("message_visibility_extend_failed", job_id=job_id, error=str(exc))


def s3_ref_from_https(url: str) -> Optional[tuple[str, str]]:
    parsed = urlparse(url)
    host = parsed.netloc
    path = parsed.path.lstrip("/")
    if not host or not path:
        return None
    if host.endswith(".s3.amazonaws.com"):
        return host.split(".s3.amazonaws.com", 1)[0], path
    match = re.match(r"^s3[.-][a-z0-9-]+\.amazonaws\.com$", host)
    if match and "/" in path:
        bucket, key = path.split("/", 1)
        return bucket, key
    return None


def download_ref(ref: Any, dest: Path, default_bucket: str = "") -> None:
    if isinstance(ref, dict):
        url = ref.get("url") or ref.get("download_url") or ref.get("downloadUrl") or ref.get("src")
        if url:
            download_ref(str(url), dest, default_bucket)
            return
        bucket = str(ref.get("bucket") or ref.get("s3_bucket") or default_bucket or "").strip()
        key = str(ref.get("key") or ref.get("s3_key") or ref.get("object_key") or ref.get("path") or ref.get("storage_path") or "").strip()
        if bucket and key:
            s3.download_file(bucket, key.replace(f"s3://{bucket}/", ""), str(dest))
            return
    if isinstance(ref, str):
        text = ref.strip()
        if text.startswith("s3://"):
            without_scheme = text[5:]
            bucket, key = without_scheme.split("/", 1)
            s3.download_file(bucket, key, str(dest))
            return
        if text.startswith(("http://", "https://")):
            s3_ref = s3_ref_from_https(text)
            if s3_ref:
                s3.download_file(s3_ref[0], s3_ref[1], str(dest))
                return
            response = requests.get(text, timeout=120)
            response.raise_for_status()
            dest.write_bytes(response.content)
            return
        if default_bucket:
            s3.download_file(default_bucket, text, str(dest))
            return
    raise ValueError(f"Unsupported source reference: {str(ref)[:200]}")


def download_s3_object(bucket: str, key: str, dest: Path) -> None:
    if not bucket or not key:
        raise ValueError("S3 bucket/key are required")
    s3.download_file(bucket, key, str(dest))
    if not dest.exists() or dest.stat().st_size <= 0:
        raise RuntimeError(f"Downloaded S3 object is missing or empty: s3://{bucket}/{key}")


def upload_file_to_s3(local_path: Path, bucket: str, key: str) -> str:
    content_type = mimetypes.guess_type(str(local_path))[0] or "video/mp4"
    s3.upload_file(str(local_path), bucket, key, ExtraArgs={"ContentType": content_type})
    return f"s3://{bucket}/{key}"


def ffprobe_json(path: Path) -> dict[str, Any]:
    cmd = [
        os.getenv("FFPROBE_BIN", "ffprobe"),
        "-v", "error",
        "-print_format", "json",
        "-show_format",
        "-show_streams",
        str(path),
    ]
    result = subprocess.run(cmd, check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=120)
    if result.returncode != 0:
        raise RuntimeError(f"ffprobe failed: {(result.stderr or '')[-2000:]}")
    return json.loads(result.stdout or "{}")


def probe_video(path: Path) -> dict[str, Any]:
    info = ffprobe_json(path)
    streams = info.get("streams") if isinstance(info.get("streams"), list) else []
    video = next((item for item in streams if item.get("codec_type") == "video"), None)
    if not isinstance(video, dict):
        raise RuntimeError("Source video has no video stream")
    has_audio = any(isinstance(item, dict) and item.get("codec_type") == "audio" for item in streams)
    width = safe_int(video.get("width"), 0)
    height = safe_int(video.get("height"), 0)
    duration = safe_float(video.get("duration"), 0.0) or safe_float((info.get("format") or {}).get("duration"), 0.0)
    fps_text = str(video.get("avg_frame_rate") or video.get("r_frame_rate") or "30/1")
    fps = 30.0
    if "/" in fps_text:
        left, right = fps_text.split("/", 1)
        denom = safe_float(right, 1.0)
        fps = safe_float(left, 30.0) / denom if denom else 30.0
    else:
        fps = safe_float(fps_text, 30.0)
    if width <= 0 or height <= 0 or duration <= 0:
        raise RuntimeError("Source video probe returned invalid width/height/duration")
    return {"width": width, "height": height, "duration": duration, "fps": max(1, min(120, int(round(fps)))), "has_audio": has_audio}


def slot_enabled(slot: Any) -> bool:
    return isinstance(slot, dict) and truthy_setting(slot.get("enabled")) and bool(slot.get("preset_id"))


def slot_duration(slot: dict[str, Any], fallback: float = 0.0) -> float:
    return max(0.0, safe_float(slot.get("duration_seconds"), fallback))


def animation_timings(animation: Any) -> tuple[str, float]:
    name = str(animation or "fade").strip().lower()
    if name == "slide":
        return name, 0.35
    if name == "scale":
        return name, 0.30
    return "fade", 0.25


def overlay_asset_filters(
    filters: list[str],
    base_label: str,
    image_input: int,
    output_label: str,
    width: int,
    height: int,
    fps: int,
    start: float,
    end: float,
    animation: Any,
    prefix: str,
) -> None:
    animation_name, transition = animation_timings(animation)
    transition = min(transition, max(0.05, (end - start) / 2.0))
    asset_label = f"{prefix}_asset"
    alpha_label = f"{prefix}_alpha"
    filters.append(
        f"[{image_input}:v]scale={width}:{height}:force_original_aspect_ratio=decrease:flags=lanczos,"
        f"pad={width}:{height}:(ow-iw)/2:(oh-ih)/2:color=black@0,setsar=1,fps={fps},format=rgba[{asset_label}]"
    )
    filters.append(
        f"[{asset_label}]fade=t=in:st={start:.3f}:d={transition:.3f}:alpha=1,"
        f"fade=t=out:st={max(start, end - transition):.3f}:d={transition:.3f}:alpha=1[{alpha_label}]"
    )
    enable = f"between(t\\,{start:.3f}\\,{end:.3f})"
    x_expr = "0"
    y_expr = "0"
    overlay_label = alpha_label
    if animation_name == "slide":
        x_expr = (
            f"if(lt(t\\,{start + transition:.3f})\\,{width}-{width}*(t-{start:.3f})/{transition:.3f}\\,"
            f"if(gt(t\\,{end - transition:.3f})\\,{width}*(t-{end - transition:.3f})/{transition:.3f}\\,0))"
        )
    elif animation_name == "scale":
        scaled_label = f"{prefix}_scaled"
        progress = (
            f"if(lt(t\\,{start + transition:.3f})\\,(t-{start:.3f})/{transition:.3f}\\,"
            f"if(gt(t\\,{end - transition:.3f})\\,1-(t-{end - transition:.3f})/{transition:.3f}\\,1))"
        )
        scale_expr = f"0.92+0.08*({progress})"
        filters.append(
            f"[{alpha_label}]scale=w='trunc(iw*({scale_expr})/2)*2':h='trunc(ih*({scale_expr})/2)*2':eval=frame,format=rgba[{scaled_label}]"
        )
        overlay_label = scaled_label
        x_expr = f"({width}-w)/2"
        y_expr = f"({height}-h)/2"
    filters.append(
        f"[{base_label}][{overlay_label}]overlay=x='{x_expr}':y='{y_expr}':enable='{enable}':format=auto,"
        f"fps={fps},format=yuv420p[{output_label}]"
    )


def require_slot_asset(slot: dict[str, Any], output_bucket: str) -> tuple[str, str]:
    key = str(slot.get("asset_key") or "").strip()
    if not key:
        raise RuntimeError(f"Enabled branding slot '{slot.get('slot')}' is missing asset_key")
    return output_bucket, key


def add_slot_asset_input(cmd: list[str], slot: dict[str, Any], output_bucket: str, work_dir: Path, duration: float, index: int) -> int:
    bucket, key = require_slot_asset(slot, output_bucket)
    suffix = Path(key).suffix if re.match(r"^\.[A-Za-z0-9]+$", Path(key).suffix or "") else ".png"
    local_path = work_dir / f"slot_{slot.get('slot') or index}{suffix.lower()}"
    download_s3_object(bucket, key, local_path)
    cmd.extend(["-loop", "1", "-t", f"{max(0.1, duration):.3f}", "-i", str(local_path)])
    return index


def build_overlay_existing_command(
    source_path: Path,
    output_path: Path,
    payload: dict[str, Any],
    branding: dict[str, Any],
    work_dir: Path,
    probe: dict[str, Any],
) -> list[str]:
    width, height, duration, fps = probe["width"], probe["height"], probe["duration"], probe["fps"]
    output_bucket = str((payload.get("output") or {}).get("bucket") or "")
    intro = branding.get("intro") if isinstance(branding.get("intro"), dict) else {}
    exit_slot = branding.get("exit") if isinstance(branding.get("exit"), dict) else {}
    all_time = branding.get("all_time") if isinstance(branding.get("all_time"), dict) else {}
    timing = branding.get("all_time_timing") if isinstance(branding.get("all_time_timing"), dict) else {}

    intro_duration = min(duration, slot_duration(intro)) if slot_enabled(intro) else 0.0
    exit_duration = min(duration, slot_duration(exit_slot)) if slot_enabled(exit_slot) else 0.0
    intro_window = (0.0, intro_duration)
    exit_window = (max(0.0, duration - exit_duration), duration)
    all_start = intro_window[1] if truthy_setting(timing.get("start_after_intro", True)) and slot_enabled(intro) else 0.0
    all_end = exit_window[0] if truthy_setting(timing.get("end_before_exit", True)) and slot_enabled(exit_slot) else duration
    if all_end < all_start:
        all_start, all_end = 0.0, duration

    cmd = [get_ffmpeg_bin(), "-hide_banner", "-y", "-i", str(source_path)]
    filters = [f"[0:v]setsar=1,fps={fps},format=yuv420p[base0]"]
    current = "base0"
    next_input = 1
    for slot, window in ((intro, intro_window), (all_time, (all_start, all_end)), (exit_slot, exit_window)):
        if not slot_enabled(slot) or window[1] <= window[0]:
            continue
        input_index = add_slot_asset_input(cmd, slot, output_bucket, work_dir, duration, next_input)
        next_input += 1
        out = f"ov{len(filters)}"
        overlay_asset_filters(filters, current, input_index, out, width, height, fps, window[0], window[1], slot.get("animation"), f"{slot.get('slot')}_{len(filters)}")
        current = out

    if current == "base0":
        raise RuntimeError("Branding is enabled, but no enabled branding slots were found")

    cmd.extend([
        "-filter_complex", ";".join(filters),
        "-map", f"[{current}]",
        "-map", "0:a?",
        "-c:v", "libx264", "-preset", os.getenv("FFMPEG_PRESET", "veryfast"), "-crf", os.getenv("VIDEO_BRANDING_CRF", "20"),
        "-pix_fmt", "yuv420p", "-r", str(fps),
        "-c:a", "copy",
        "-movflags", "+faststart",
        str(output_path),
    ])
    return cmd


def build_prepend_append_command(
    source_path: Path,
    output_path: Path,
    payload: dict[str, Any],
    branding: dict[str, Any],
    work_dir: Path,
    probe: dict[str, Any],
) -> tuple[list[str], float]:
    width, height, source_duration, fps = probe["width"], probe["height"], probe["duration"], probe["fps"]
    output_bucket = str((payload.get("output") or {}).get("bucket") or "")
    intro = branding.get("intro") if isinstance(branding.get("intro"), dict) else {}
    exit_slot = branding.get("exit") if isinstance(branding.get("exit"), dict) else {}
    all_time = branding.get("all_time") if isinstance(branding.get("all_time"), dict) else {}
    timing = branding.get("all_time_timing") if isinstance(branding.get("all_time_timing"), dict) else {}
    intro_enabled = slot_enabled(intro)
    exit_enabled = slot_enabled(exit_slot)
    all_time_enabled = slot_enabled(all_time)
    intro_duration = slot_duration(intro) if intro_enabled else 0.0
    exit_duration = slot_duration(exit_slot) if exit_enabled else 0.0
    total_duration = source_duration + intro_duration + exit_duration

    cmd = [get_ffmpeg_bin(), "-hide_banner", "-y", "-i", str(source_path)]
    filters = [
        f"[0:v]setsar=1,fps={fps},format=yuv420p[srcbase]",
    ]
    next_input = 1
    video_labels: list[str] = []
    audio_labels: list[str] = []

    if intro_enabled:
        input_index = add_slot_asset_input(cmd, intro, output_bucket, work_dir, intro_duration, next_input)
        next_input += 1
        filters.append(f"color=c=black:s={width}x{height}:r={fps}:d={intro_duration:.3f},format=yuv420p[introbase]")
        overlay_asset_filters(filters, "introbase", input_index, "introvideo", width, height, fps, 0.0, intro_duration, intro.get("animation"), "intro")
        filters.append(f"anullsrc=channel_layout=stereo:sample_rate=48000:d={intro_duration:.3f}[introaudio]")
        video_labels.append("introvideo")
        audio_labels.append("introaudio")

    source_label = "srcbase"
    if all_time_enabled:
        all_start = 0.0
        all_end = source_duration
        if truthy_setting(timing.get("start_after_intro", True)):
            all_start = 0.0
        if truthy_setting(timing.get("end_before_exit", True)):
            all_end = source_duration
        input_index = add_slot_asset_input(cmd, all_time, output_bucket, work_dir, source_duration, next_input)
        next_input += 1
        overlay_asset_filters(filters, source_label, input_index, "sourcebranded", width, height, fps, all_start, all_end, all_time.get("animation"), "alltime")
        source_label = "sourcebranded"
    video_labels.append(source_label)
    if probe["has_audio"]:
        filters.append("[0:a]aresample=48000,aformat=sample_fmts=fltp:channel_layouts=stereo[sourceaudio]")
    else:
        filters.append(f"anullsrc=channel_layout=stereo:sample_rate=48000:d={source_duration:.3f}[sourceaudio]")
    audio_labels.append("sourceaudio")

    if exit_enabled:
        input_index = add_slot_asset_input(cmd, exit_slot, output_bucket, work_dir, exit_duration, next_input)
        next_input += 1
        filters.append(f"color=c=black:s={width}x{height}:r={fps}:d={exit_duration:.3f},format=yuv420p[exitbase]")
        overlay_asset_filters(filters, "exitbase", input_index, "exitvideo", width, height, fps, 0.0, exit_duration, exit_slot.get("animation"), "exit")
        filters.append(f"anullsrc=channel_layout=stereo:sample_rate=48000:d={exit_duration:.3f}[exitaudio]")
        video_labels.append("exitvideo")
        audio_labels.append("exitaudio")

    concat_inputs = "".join(f"[{v}][{a}]" for v, a in zip(video_labels, audio_labels))
    filters.append(f"{concat_inputs}concat=n={len(video_labels)}:v=1:a=1[outv][outa]")
    cmd.extend([
        "-filter_complex", ";".join(filters),
        "-map", "[outv]",
        "-map", "[outa]",
        "-c:v", "libx264", "-preset", os.getenv("FFMPEG_PRESET", "veryfast"), "-crf", os.getenv("VIDEO_BRANDING_CRF", "20"),
        "-pix_fmt", "yuv420p", "-r", str(fps),
        "-c:a", "aac", "-b:a", os.getenv("VIDEO_BRANDING_AUDIO_BITRATE", "192k"),
        "-movflags", "+faststart",
        str(output_path),
    ])
    return cmd, total_duration


def run_ffmpeg(cmd: list[str], job_id: str, stage: str, timeout_seconds: int) -> None:
    started_at = time.time()
    log_event(f"{stage}_start", job_id=job_id, command_preview=" ".join(cmd[:100]) + (" ..." if len(cmd) > 100 else ""))
    try:
        result = subprocess.run(cmd, check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=timeout_seconds)
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(f"{stage} timed out after {round(time.time() - started_at, 2)}s") from exc
    log_event(f"{stage}_finished", job_id=job_id, returncode=result.returncode, elapsed_seconds=round(time.time() - started_at, 2))
    if result.returncode != 0:
        raise RuntimeError(f"{stage} failed with return code {result.returncode}; stderr_tail={(result.stderr or '')[-4000:]}")


def process_video_branding_job(payload: dict[str, Any], receipt_handle: Optional[str] = None) -> dict[str, Any]:
    if payload.get("kind") != "video_branding":
        raise ValueError(f"Unsupported message kind for video branding worker: {payload.get('kind')}")
    job_id = str(payload.get("jobId") or payload.get("job_id") or "").strip()
    if not job_id:
        raise ValueError("video_branding payload missing jobId")
    source = payload.get("source_video") if isinstance(payload.get("source_video"), dict) else {}
    output = payload.get("output") if isinstance(payload.get("output"), dict) else {}
    source_bucket = str(source.get("bucket") or "").strip()
    source_key = str(source.get("key") or "").strip()
    output_bucket = str(output.get("bucket") or "").strip()
    output_key = str(output.get("key") or "").strip()
    if not source_bucket or not source_key:
        raise ValueError("video_branding payload missing source_video.bucket/key")
    if not output_bucket or not output_key:
        raise ValueError("video_branding payload missing output.bucket/key")

    emit_callback(job_id, "processing", "starting", 1, message="Video branding job started")
    ensure_worker_storage()
    if receipt_handle:
        change_message_visibility(receipt_handle, max(VISIBILITY_TIMEOUT, FFMPEG_TIMEOUT_SECONDS + FFMPEG_VISIBILITY_BUFFER_SECONDS), job_id)

    with tempfile.TemporaryDirectory(prefix=f"video-branding-{job_id[:8]}-", dir=str(RENDER_WORK_DIR)) as tmp:
        work_dir = Path(tmp)
        source_path = work_dir / (Path(source_key).name or "source.mp4")
        emit_callback(job_id, "processing", "downloading", 5, message="Downloading source video")
        download_s3_object(source_bucket, source_key, source_path)
        probe = probe_video(source_path)
        log_event("source_video_probed", job_id=job_id, **probe)

        branding_enabled = payload.get("branded_enabled") is True
        branding = payload.get("branding") if isinstance(payload.get("branding"), dict) else {}
        if not branding_enabled:
            raise RuntimeError("video_branding job has branded_enabled=false; refusing to render branding")
        if not truthy_setting(branding.get("enabled")):
            raise RuntimeError("video_branding job branding.enabled is false or missing")

        emit_callback(job_id, "processing", "branding", 25, message="Applying video branding")
        mode = str(payload.get("branding_mode") or "prepend_append").strip()
        branded_path = work_dir / "video_branded.mp4"
        if mode == "overlay_existing":
            cmd = build_overlay_existing_command(source_path, branded_path, payload, branding, work_dir, probe)
            expected_duration = probe["duration"]
        elif mode == "prepend_append":
            cmd, expected_duration = build_prepend_append_command(source_path, branded_path, payload, branding, work_dir, probe)
        else:
            raise ValueError(f"Unsupported branding_mode: {mode}")
        run_ffmpeg(cmd, job_id, "video_branding", FFMPEG_TIMEOUT_SECONDS)
        output_path = branded_path

        settings = {
            "watermark": payload.get("watermark"),
            "output_width": probe["width"],
            "output_height": probe["height"],
            "fps": probe["fps"],
        }
        if isinstance(settings.get("watermark"), dict) and isinstance(settings["watermark"].get("spec"), dict):
            watermark = dict(settings["watermark"])
            spec = dict(watermark["spec"])
            spec.pop("caps", None)
            watermark["spec"] = spec
            settings["watermark"] = watermark
        if normalize_watermark_spec(settings):
            emit_callback(job_id, "processing", "watermarking", 80, message="Applying watermark")
            watermarked_path = work_dir / "video_branded_watermarked.mp4"
            watermark_cmd = build_watermark_post_assembly_command(output_path, watermarked_path, settings)
            if watermark_cmd:
                run_postprocess_command(watermark_cmd, job_id, FFMPEG_TIMEOUT_SECONDS, "video_branding_watermark")
                output_path = watermarked_path

        if not output_path.exists() or output_path.stat().st_size <= 0:
            raise RuntimeError("Video branding output file missing or empty")
        emit_callback(job_id, "processing", "uploading", 90, message="Uploading branded video")
        s3_uri = upload_file_to_s3(output_path, output_bucket, output_key)
        final_probe = probe_video(output_path)
        result = {
            "kind": "video_branding",
            "jobId": job_id,
            "status": "complete",
            "bucket": output_bucket,
            "key": output_key,
            "s3Uri": s3_uri,
            "output_url": s3_uri,
            "output_duration_seconds": round(final_probe.get("duration") or expected_duration, 3),
            "output_resolution": f"{probe['width']}x{probe['height']}",
            "size_bytes": output_path.stat().st_size,
        }
        emit_callback(job_id, "complete", "complete", 100, message="Video branding complete", **result)
        log_event("video_branding_completed", **result)
        return result


def process_sqs_message(message: dict[str, Any]) -> None:
    receipt_handle = message["ReceiptHandle"]
    message_id = message.get("MessageId", "")
    payload: dict[str, Any] = {}
    job_id = message_id
    try:
        payload = parse_message_body(message.get("Body", ""))
        job_id = str(payload.get("jobId") or payload.get("job_id") or message_id)
        if payload.get("kind") != "video_branding":
            raise ValueError(f"Video branding worker received non-video_branding message kind: {payload.get('kind')}")
        log_event("video_branding_message_received", message_id=message_id, job_id=job_id)
        process_video_branding_job(payload, receipt_handle=receipt_handle)
        sqs.delete_message(QueueUrl=SQS_QUEUE_URL, ReceiptHandle=receipt_handle)
        log_event("message_deleted", job_id=job_id, message_id=message_id)
    except Exception as exc:
        err = str(exc)
        log_event("video_branding_failed_do_not_delete_sqs_message", message_id=message_id, job_id=job_id, error=err, traceback=traceback.format_exc()[-8000:])
        try:
            emit_callback(job_id, "failed", "failed", 0, error=err, message="Video branding failed")
        except Exception:
            pass
        try:
            sqs.change_message_visibility(QueueUrl=SQS_QUEUE_URL, ReceiptHandle=receipt_handle, VisibilityTimeout=60)
        except Exception:
            pass


def poll_once(executor: concurrent.futures.Executor, futures: set[concurrent.futures.Future]) -> None:
    done = {f for f in futures if f.done()}
    for future in done:
        futures.remove(future)
        try:
            future.result()
        except Exception as exc:
            log_event("worker_future_error", error=str(exc), traceback=traceback.format_exc()[-4000:])
    available_slots = max(0, MAX_CONCURRENT - len(futures))
    if available_slots <= 0:
        return
    response = sqs.receive_message(
        QueueUrl=SQS_QUEUE_URL,
        MaxNumberOfMessages=min(10, available_slots),
        WaitTimeSeconds=20,
        VisibilityTimeout=VISIBILITY_TIMEOUT,
        MessageAttributeNames=["All"],
        AttributeNames=["All"],
    )
    messages = response.get("Messages", [])
    if not messages:
        log_event("poll_empty")
        return
    for message in messages:
        futures.add(executor.submit(process_sqs_message, message))


def validate_startup() -> None:
    ensure_worker_storage()
    if not SQS_QUEUE_URL:
        raise RuntimeError("Missing VIDEO_BRANDING_SQS_QUEUE_URL environment variable")
    result = subprocess.run([get_ffmpeg_bin(), "-version"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=15)
    if result.returncode != 0:
        raise RuntimeError(f"FFmpeg is not available: {result.stderr[:1000]}")
    try:
        sqs.get_queue_attributes(QueueUrl=SQS_QUEUE_URL, AttributeNames=["QueueArn", "ApproximateNumberOfMessages"])
        log_event("video_branding_sqs_access_ok")
    except ClientError as exc:
        raise RuntimeError(f"SQS access failed: {exc}") from exc


def main() -> None:
    validate_startup()
    futures: set[concurrent.futures.Future] = set()
    log_event("video_branding_worker_started", status="alive", max_concurrent=MAX_CONCURRENT)
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_CONCURRENT) as executor:
        last_heartbeat = 0.0
        while True:
            now = time.time()
            if now - last_heartbeat > 60:
                last_heartbeat = now
                log_event("video_branding_worker_heartbeat", status="alive", active_jobs=len(futures))
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
        log_event("video_branding_worker_fatal_error", error=str(exc), traceback=traceback.format_exc()[-8000:])
        raise
