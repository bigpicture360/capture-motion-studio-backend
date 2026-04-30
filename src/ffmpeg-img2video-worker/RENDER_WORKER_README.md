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
- `SQS_QUEUE_URL`
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
- `Dockerfile.render-worker`
- `docker-compose.render-worker.yml`
- `setup-render-worker.sh`

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
