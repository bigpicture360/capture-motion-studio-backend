# Render Worker

Document render job payloads, FFmpeg behavior, cache policy, progress callbacks,
and troubleshooting notes here.

## Queue Split

The image-to-video render worker and video-branding worker are separate
processes.

- `render_worker.py` polls `SQS_QUEUE_URL` and handles normal render jobs plus
  `kind: "frame_extraction"`.
- `video_branding_worker.py` polls `VIDEO_BRANDING_SQS_QUEUE_URL` and handles
  only `kind: "video_branding"`.

Use two SQS queues. A single shared queue is not safe here because standard SQS
does not route by message body, so either worker could receive a job it cannot
process.

## Frontend Contract

`enqueue-video-branding` should send messages to
`VIDEO_BRANDING_SQS_QUEUE_URL`. The normal render enqueue path should keep using
`SQS_QUEUE_URL`.

For video branding, every enabled slot must include a frontend pre-rendered PNG
`asset_key`. The worker downloads those PNGs from the output bucket and stops
with a failed callback if a required source video or slot asset is missing.

Video-branding final callbacks use:

```json
{
  "kind": "video_branding",
  "status": "complete",
  "stage": "complete",
  "bucket": "bucket",
  "key": "output/key.mp4",
  "output_url": "s3://bucket/output/key.mp4",
  "output_duration_seconds": 12.34
}
```
