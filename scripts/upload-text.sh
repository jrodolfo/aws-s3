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
  scripts/upload-text.sh <bucket> <key> [content] [region] [create-bucket] [cleanup]

Arguments:
  bucket          S3 bucket name
  key             S3 object key
  content         Optional object content (default: Testing with the {sdk-java})
  region          Optional AWS region (default: us-east-2)
  create-bucket   Optional true/false flag (default: false)
  cleanup         Optional true/false flag (default: false)
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
CREATE_BUCKET=${5:-false}
CLEANUP=${6:-false}

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname "$0")" && pwd)
REPO_DIR=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)

require_command mvn

export AWS_S3_BUCKET=$BUCKET
export AWS_S3_KEY=$KEY
export AWS_S3_CONTENT=$CONTENT
export AWS_S3_REGION=$REGION
export AWS_S3_CREATE_BUCKET=$CREATE_BUCKET
export AWS_S3_CLEANUP=$CLEANUP

cd "$REPO_DIR"
exec mvn -q exec:java -Dexec.mainClass=net.jrodolfo.awss3.upload.S3TextUpload
