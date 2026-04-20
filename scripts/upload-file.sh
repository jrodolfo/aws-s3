#!/bin/sh

# Runs the file-upload Java sample through Maven with minimal shell validation.

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
  scripts/upload-file.sh <bucket> <file-path> [key] [region] [multipart-threshold-bytes]

Arguments:
  bucket                     S3 bucket name
  file-path                  Local file path
  key                        Optional S3 object key (default: file name)
  region                     Optional AWS region (default: us-east-2)
  multipart-threshold-bytes  Optional threshold in bytes (default: 5242880)
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
FILE_PATH=$2
KEY=${3:-}
REGION=${4:-us-east-2}
MULTIPART_THRESHOLD=${5:-5242880}

if [ ! -f "$FILE_PATH" ]; then
  echo "File not found: $FILE_PATH" >&2
  exit 1
fi

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname "$0")" && pwd)
REPO_DIR=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)

require_command mvn

export AWS_S3_BUCKET=$BUCKET
export AWS_S3_FILE=$FILE_PATH
export AWS_S3_REGION=$REGION
export AWS_S3_MULTIPART_THRESHOLD=$MULTIPART_THRESHOLD

if [ -n "$KEY" ]; then
  export AWS_S3_KEY=$KEY
fi

cd "$REPO_DIR"
exec mvn -q exec:java -Dexec.mainClass=net.jrodolfo.awss3.upload.S3FileUpload
