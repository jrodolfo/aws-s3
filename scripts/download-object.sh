#!/bin/sh

set -eu

usage() {
  cat <<'EOF'
Usage:
  scripts/download-object.sh <bucket> <key> [region] [profile]

Arguments:
  bucket   S3 bucket name
  key      S3 object key
  region   Optional AWS region (default: us-east-2)
  profile  Optional AWS profile name
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
REGION=${3:-us-east-2}
PROFILE=${4:-}

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname "$0")" && pwd)
REPO_DIR=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)

export AWS_S3_BUCKET=$BUCKET
export AWS_S3_KEY=$KEY
export AWS_S3_REGION=$REGION

if [ -n "$PROFILE" ]; then
  export AWS_PROFILE=$PROFILE
fi

cd "$REPO_DIR"
exec mvn -q exec:java -Dexec.mainClass=net.jrodolfo.awss3.download.S3ObjectDownload
