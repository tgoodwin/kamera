#!/usr/bin/env bash
set -euo pipefail

# Prepares go module dependencies for deterministic simulation.
#
# Required:
#   -c|--cache-root DIR       Directory under which to create gocache/ and gomodcache/
#   -t|--target DIR           Target source dir (contains go.mod) for go mod download and final go run
#   -m|--module PATH          Module path(s) to determinize (repeatable). Each is either
#                             an absolute path, or a path relative to the gomodcache (e.g., knative.dev)
#
# Optional:
#   --run-args "ARGS"         Args passed to final `go run .` in the target (default: "--depth 10")
#   -h|--help                 Show usage

print_usage() {
  cat <<USAGE
Usage: $(basename "$0") -c <cache-root-dir> -t <target-dir> -m <module> [-m <module> ...] [--run-args "--depth 10"]

Arguments:
  -c, --cache-root DIR   Directory in which to create gocache and gomodcache subdirectories.
  -t, --target DIR       Target source directory (with go.mod) used for go mod download and final go run.
  -m, --module PATH      Module path(s) to determinize (repeatable). If relative, resolved under gomodcache.
      --run-args ARGS    Extra arguments for the final 'go run .' in the target (default: --depth 10).
  -h, --help             Show this help and exit.

Examples:
  $(basename "$0") \
    -c ~/tmp \
    -t ./examples/knative-serving \
    -m knative.dev

USAGE
}

CACHE_ROOT=""
TARGET_DIR=""
RUN_ARGS="--depth 10"
MODULES=()

# Parse arguments (supports short and long options)
while [[ $# -gt 0 ]]; do
  case "$1" in
    -c|--cache-root)
      CACHE_ROOT=${2:-}
      shift 2
      ;;
    -t|--target)
      TARGET_DIR=${2:-}
      shift 2
      ;;
    -m|--module)
      MODULES+=("${2:-}")
      shift 2
      ;;
    --run-args)
      RUN_ARGS=${2:-}
      shift 2
      ;;
    -h|--help)
      print_usage
      exit 0
      ;;
    --)
      shift
      break
      ;;
    *)
      echo "Unknown argument: $1" >&2
      print_usage >&2
      exit 2
      ;;
  esac
done

if [[ -z "$CACHE_ROOT" || -z "$TARGET_DIR" || ${#MODULES[@]} -eq 0 ]]; then
  echo "Missing required arguments." >&2
  print_usage >&2
  exit 2
fi

# Canonicalize paths
CACHE_ROOT=$(cd "$CACHE_ROOT" && pwd)
TARGET_DIR=$(cd "$TARGET_DIR" && pwd)

# Resolve repo root relative to this script (../.. from cmd/determinize/)
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/../.." && pwd)

# create temp dirs (gocache and gomodcache under cache root)
GOCACHE_DIR="$CACHE_ROOT/gocache"
GOMODCACHE_DIR="$CACHE_ROOT/gomodcache"

echo "[1/5] Creating caches:"
echo "      GOCACHE=$GOCACHE_DIR"
echo "      GOMODCACHE=$GOMODCACHE_DIR"
mkdir -p "$GOCACHE_DIR" "$GOMODCACHE_DIR"

# install target deps into those directories
echo "[2/5] Downloading modules for target: $TARGET_DIR"
(
  cd "$TARGET_DIR"
  GOMODCACHE="$GOMODCACHE_DIR" go mod download
)

# ensure temp gomodcache contents are writable
echo "[3/5] Fixing permissions in $GOMODCACHE_DIR"
chmod -R u+rwX "$GOMODCACHE_DIR"

# run determinize against the dependencies requested
echo "[4/5] Determinizing modules..."

# Prefer the pre-built binary if it exists, otherwise use go run.
DETERMINIZE_BINARY="$REPO_ROOT/bin/determinize"
DETERMINIZE_CMD=("$DETERMINIZE_BINARY")
if [[ ! -x "$DETERMINIZE_BINARY" ]]; then
  DETERMINIZE_CMD=(go run "$REPO_ROOT/cmd/determinize/main.go")
fi

for mod in "${MODULES[@]}"; do
  # If the provided module is an absolute path or exists as-given, use it;
  # otherwise, assume it's relative to the gomodcache root.
  if [[ -d "$mod" ]]; then
    TARGET_PATH="$mod"
  elif [[ "$mod" = /* ]]; then
    TARGET_PATH="$mod"
  else
    TARGET_PATH="$GOMODCACHE_DIR/$mod"
  fi

  if [[ ! -d "$TARGET_PATH" ]]; then
    echo "  ! Skipping: $TARGET_PATH (not found)" >&2
    exit 1
  fi
  echo "  - $TARGET_PATH"
  "${DETERMINIZE_CMD[@]}" "$TARGET_PATH"
done

echo "Done."
