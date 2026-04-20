#!/bin/sh

set -eu

require_command() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "Required command not found: $1" >&2
    exit 1
  }
}

usage() {
  cat <<'EOF'
Usage:
  scripts/roundtrip-object.sh <bucket> <key> [content] [region] [profile] [create-bucket]

Arguments:
  bucket         S3 bucket name
  key            S3 object key
  content        Optional object content (default: Testing with the {sdk-java})
  region         Optional AWS region (default: us-east-2)
  profile        Optional AWS profile name
  create-bucket  Optional true/false flag for the upload step (default: false)
EOF
}

if [ "${1:-}" = "-h" ] || [ "${1:-}" = "--help" ]; then
  usage
  exit 0
fi

if [ $# -lt 2 ]; then
  usage >&2
  exit 1
fi

BUCKET=$1
KEY=$2
CONTENT=${3:-Testing with the {sdk-java}}
REGION=${4:-us-east-2}
PROFILE=${5:-}
CREATE_BUCKET=${6:-false}

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname "$0")" && pwd)

require_command mvn

printf '%s\n' "Uploading object to s3..."
"$SCRIPT_DIR/upload-text.sh" "$BUCKET" "$KEY" "$CONTENT" "$REGION" "$CREATE_BUCKET" false

printf '%s\n' ""
printf '%s\n' "Downloading the same object from s3..."
"$SCRIPT_DIR/download-object.sh" "$BUCKET" "$KEY" "$REGION" "$PROFILE"
