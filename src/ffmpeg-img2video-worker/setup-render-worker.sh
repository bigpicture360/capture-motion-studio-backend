#!/usr/bin/env bash
# ============================================================
# BigPicture360 Render Worker — EC2 Setup Script
# Run on a fresh Ubuntu 22.04+ EC2 instance:
#   chmod +x setup-render-worker.sh && sudo ./setup-render-worker.sh
# ============================================================
set -euo pipefail

echo "=== BigPicture360 Render Worker Setup ==="

echo "[1/6] Updating system packages..."
apt-get update -y && apt-get upgrade -y

echo "[2/6] Installing Docker..."
if ! command -v docker &> /dev/null; then
  curl -fsSL https://get.docker.com | sh
  systemctl enable docker
  systemctl start docker
  usermod -aG docker ubuntu || true
fi

echo "[3/6] Installing Docker Compose plugin..."
if ! docker compose version &> /dev/null; then
  apt-get install -y docker-compose-plugin
fi



echo "[4/6] Installing helper packages..."
### WAS: apt-get install -y ffmpeg jq
### NOW:
apt-get install -y ffmpeg jq awscli

echo "[5/6] Setting up worker directory..."
WORKER_DIR="/opt/render-worker"
mkdir -p "$WORKER_DIR"

echo "[6/6] Creating environment config..."
ENV_FILE="$WORKER_DIR/.env"
if [ ! -f "$ENV_FILE" ]; then
  cat > "$ENV_FILE" << 'ENVEOF'
# === AWS ===
# Prefer EC2 Instance Profile / IAM Role. Do not store static AWS keys here.
AWS_DEFAULT_REGION=us-east-1
SQS_QUEUE_URL=https://sqs.us-east-1.amazonaws.com/123456789012/your-queue-name
VIDEO_BRANDING_SQS_QUEUE_URL=https://sqs.us-east-1.amazonaws.com/123456789012/your-video-branding-queue-name
S3_BUCKET=your-s3-bucket-name

# === Supabase / Lovable Cloud ===
SUPABASE_URL=https://vfvioexcoecdbjutacbi.supabase.co
SUPABASE_SERVICE_ROLE_KEY=replace-me
RENDER_WORKER_SECRET=replace-me

# === Worker Settings ===
POLL_INTERVAL=5
MAX_CONCURRENT=2
VIDEO_BRANDING_MAX_CONCURRENT=1
VISIBILITY_TIMEOUT=3600
VIDEO_BRANDING_VISIBILITY_TIMEOUT=3600
MIN_FREE_DISK_GB=2
LOG_LEVEL=INFO
ENVEOF
  echo "  → Created $ENV_FILE"
else
  echo "  → $ENV_FILE already exists, skipping."
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
for f in render_worker.py video_branding_worker.py Dockerfile.render-worker docker-compose.render-worker.yml requirements.txt; do
  if [ -f "$SCRIPT_DIR/$f" ]; then
    cp "$SCRIPT_DIR/$f" "$WORKER_DIR/"
    echo "  → Copied $f"
  else
    echo "  ⚠ $f not found in $SCRIPT_DIR — copy it manually to $WORKER_DIR/"
  fi
done

cat << 'EOF'

============================================
Setup complete!
============================================

Recommended next steps:
  1. Attach an IAM role to this EC2 instance with SQS + S3 permissions.
  2. Edit the environment file:
       sudo nano /opt/render-worker/.env
  3. Start the worker:
       cd /opt/render-worker
       sudo docker compose -f docker-compose.render-worker.yml up -d --build
  4. Monitor logs:
       sudo docker compose -f docker-compose.render-worker.yml logs -f

Validate AWS identity from the instance:
  aws sts get-caller-identity

EOF
