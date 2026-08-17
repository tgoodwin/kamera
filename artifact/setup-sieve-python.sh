#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<'EOF'
usage: ./artifact/setup-sieve-python.sh <sieve-checkout> [venv-directory]

Creates or updates the Python virtual environment used by the Table 6 Sieve
baseline runner. The default environment is <sieve-checkout>/.venv, which
run-sieve-baselines.sh discovers automatically.
EOF
  exit 2
}

[[ $# -ge 1 && $# -le 2 ]] || usage

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
sieve_root="$1"
[[ "$sieve_root" = /* ]] || sieve_root="$PWD/$sieve_root"
venv_dir="${2:-$sieve_root/.venv}"
[[ "$venv_dir" = /* ]] || venv_dir="$PWD/$venv_dir"
expected_sieve_commit="6c97abeb79e644fa5eda889a2c174b2436dbc264"

if [[ ! -f "$sieve_root/reproduce_bugs.py" ]]; then
  echo "Sieve checkout not found at $sieve_root" >&2
  exit 1
fi
actual_sieve_commit="$(git -C "$sieve_root" rev-parse HEAD)"
if [[ "$actual_sieve_commit" != "$expected_sieve_commit" &&
      "${KAMERA_AE_SIEVE_ALLOW_OTHER_COMMIT:-}" != "1" ]]; then
  echo "Sieve is at $actual_sieve_commit; expected $expected_sieve_commit" >&2
  echo "set KAMERA_AE_SIEVE_ALLOW_OTHER_COMMIT=1 to test another revision" >&2
  exit 1
fi

platform="$(uname -s)-$(uname -m)"
case "$platform" in
  Darwin-arm64)
    default_python="python3.11"
    requirements="$repo_root/artifact/sieve/requirements-reproduce.txt"
    version_rule="modern"
    ;;
  Linux-x86_64|Darwin-x86_64)
    default_python="python3.7"
    requirements="$sieve_root/requirements.txt"
    version_rule="original"
    ;;
  *)
    echo "unsupported Sieve host platform: $platform" >&2
    echo "supported paths are Apple Silicon and Linux/macOS x86-64" >&2
    exit 1
    ;;
esac
if [[ ! -f "$requirements" ]]; then
  echo "Sieve requirements file not found: $requirements" >&2
  exit 1
fi

bootstrap_python="${KAMERA_AE_SIEVE_BOOTSTRAP_PYTHON:-$default_python}"
if ! command -v "$bootstrap_python" >/dev/null 2>&1; then
  echo "required Python interpreter not found: $bootstrap_python" >&2
  if [[ "$version_rule" == "modern" ]]; then
    echo "install Python 3.11 or set KAMERA_AE_SIEVE_BOOTSTRAP_PYTHON" >&2
  else
    echo "install Python 3.7 or set KAMERA_AE_SIEVE_BOOTSTRAP_PYTHON" >&2
  fi
  exit 1
fi

"$bootstrap_python" - "$version_rule" <<'PY'
import sys

rule = sys.argv[1]
version = sys.version_info[:2]
if rule == "modern" and version < (3, 11):
    raise SystemExit("the native environment requires Python 3.11 or newer")
if rule == "original" and version != (3, 7):
    raise SystemExit("the original Sieve requirements require Python 3.7")
PY

if [[ -e "$venv_dir" && ! -x "$venv_dir/bin/python" ]]; then
  echo "refusing to replace non-venv path: $venv_dir" >&2
  exit 1
fi
if [[ ! -x "$venv_dir/bin/python" ]]; then
  echo "creating Sieve virtual environment at $venv_dir"
  "$bootstrap_python" -m venv "$venv_dir"
else
  echo "updating Sieve virtual environment at $venv_dir"
fi

"$venv_dir/bin/python" -m pip install --upgrade pip
"$venv_dir/bin/python" -m pip install -r "$requirements"
"$venv_dir/bin/python" -m pip check

cat <<EOF
Sieve Python environment is ready:
  $venv_dir/bin/python
EOF

if [[ "$venv_dir" == "$sieve_root/.venv" ]]; then
  cat <<EOF
The default Table 6 runner discovers this environment automatically:
  ./artifact/run-sieve-baselines.sh /absolute/path/to/sieve
EOF
else
  cat <<EOF
Select this non-default environment when running the Table 6 wrapper:
  KAMERA_AE_SIEVE_PYTHON="$venv_dir/bin/python" \
    ./artifact/run-sieve-baselines.sh /absolute/path/to/sieve
EOF
fi
