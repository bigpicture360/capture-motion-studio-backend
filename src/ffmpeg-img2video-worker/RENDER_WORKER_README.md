# Render Worker — Deployment Guide (v2)

## What changed
This updated version fixes the main deployment and production issues:

- Standardizes on `AWS_DEFAULT_REGION`
- Removes the need to pass `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` into Docker
- Assumes **EC2 IAM Role / Instance Profile** for AWS access
- Adds `VISIBILITY_TIMEOUT` so long renders do not get double-processed
- Adds `MAX_CONCURRENT` worker concurrency
- Adds dynamic FFmpeg timeout sizing
- Adds low-disk protection
- Uses `requirements.txt` in the Docker build
- Installs FFmpeg in the container and setup script

## Recommended production access model

Use an **EC2 IAM Role**, not static AWS keys.

### Environment variables you still need
- `AWS_DEFAULT_REGION`
- `SQS_QUEUE_URL` for image-to-video render jobs
- `VIDEO_BRANDING_SQS_QUEUE_URL` for `kind: "video_branding"` jobs
- `S3_BUCKET`
- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY`
- `RENDER_WORKER_SECRET`

### Environment variables you should NOT store on the box
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`

Boto3 will automatically fetch credentials from the EC2 Instance Metadata Service when an IAM role is attached to the instance.

## Deployment files
- `render_worker.py`
- `video_branding_worker.py`
- `Dockerfile.render-worker`
- `docker-compose.render-worker.yml`
- `setup-render-worker.sh`

## Video-branding worker

Video branding is handled by a separate `video-branding-worker` compose
service. Use a dedicated SQS queue for this service. Do not send
`kind: "video_branding"` messages to the normal render queue, because standard
SQS queues cannot route messages by kind and either worker may receive the
other worker's job first.

Frontend edge-function requirement:
- `enqueue-render` continues to send render jobs to `SQS_QUEUE_URL`.
- `enqueue-video-branding` must send video-branding jobs to
  `VIDEO_BRANDING_SQS_QUEUE_URL`.
- Every enabled video-branding slot must include a pre-rendered PNG
  `asset_key`; the video-branding worker fails fast when an enabled slot is
  missing its source asset.

Callback status values for video branding are `processing`, `complete`, and
`failed`. Final completion includes `kind: "video_branding"`, `bucket`, `key`,
`output_url`, and `output_duration_seconds`.

## Quick deploy
```bash
chmod +x setup-render-worker.sh
sudo ./setup-render-worker.sh
cd /opt/render-worker
sudo nano .env
sudo docker compose -f docker-compose.render-worker.yml up -d --build
```

## Recommended SQS settings
- Main queue with a dead-letter queue
- `maxReceiveCount`: 3 to 5
- Worker `VISIBILITY_TIMEOUT`: large enough for the biggest render, e.g. 3600 seconds

## Recommended EC2 sizing
Start with:
- `t3.large` or `t3.xlarge` for light 1080p rendering
- `gp3` EBS 50–100 GB
- Move to larger CPU or GPU instances later if you add depth/parallax rendering

## Validation
```bash
aws sts get-caller-identity
aws sqs get-queue-attributes --queue-url "$SQS_QUEUE_URL" --attribute-names All
docker compose -f docker-compose.render-worker.yml logs -f
```
