#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
deps_root="${1:-$repo_root/artifact-deps/figure8}"
[[ "$deps_root" = /* ]] || deps_root="$PWD/$deps_root"
kamera_repository="${KAMERA_AE_KAMERA_REPOSITORY:-https://github.com/tgoodwin/kamera.git}"
requirements="$repo_root/artifact/figure8/requirements.txt"
venv_dir="${KAMERA_AE_FIGURE8_VENV:-$deps_root/python-venv}"
[[ "$venv_dir" = /* ]] || venv_dir="$PWD/$venv_dir"

setup_python_environment() {
  if ! command -v python3 >/dev/null 2>&1; then
    echo "Figure 8 requires Python 3.11 or newer" >&2
    exit 1
  fi
  if ! python3 -c 'import sys; raise SystemExit(0 if sys.version_info >= (3, 11) else 1)'; then
    echo "Figure 8 requires Python 3.11 or newer; found $(python3 --version 2>&1)" >&2
    exit 1
  fi

  if [[ ! -x "$venv_dir/bin/python" ]]; then
    echo "creating pinned Figure 8 Python environment at $venv_dir"
    python3 -m venv "$venv_dir"
  fi

  local requirements_hash stamp installed_hash=""
  requirements_hash="$(python3 -c 'import hashlib, pathlib, sys; print(hashlib.sha256(pathlib.Path(sys.argv[1]).read_bytes()).hexdigest())' "$requirements")"
  stamp="$venv_dir/.kamera-figure8-requirements.sha256"
  if [[ -f "$stamp" ]]; then
    installed_hash="$(<"$stamp")"
  fi

  if [[ "$installed_hash" != "$requirements_hash" ]] || \
     ! "$venv_dir/bin/python" -c 'import matplotlib' >/dev/null 2>&1; then
    echo "installing pinned Figure 8 Python dependencies"
    "$venv_dir/bin/python" -m pip install \
      --disable-pip-version-check \
      -r "$requirements"
    printf '%s\n' "$requirements_hash" >"$stamp"
  else
    echo "pinned Figure 8 Python environment is ready"
  fi
}

clone_at() {
  local name="$1"
  local url="$2"
  local sha="$3"
  local destination="$4"

  if [[ ! -e "$destination" ]]; then
    echo "cloning $name at $sha"
    git clone --filter=blob:none --no-checkout "$url" "$destination"
    git -C "$destination" checkout --detach "$sha"
  elif ! git -C "$destination" rev-parse --git-dir >/dev/null 2>&1; then
    echo "$destination exists but is not a Git checkout" >&2
    exit 1
  fi

  local actual
  actual="$(git -C "$destination" rev-parse HEAD)"
  if [[ "$actual" != "$sha" ]]; then
    echo "$name checkout has commit $actual; expected $sha" >&2
    echo "remove or relocate $destination, then rerun setup" >&2
    exit 1
  fi
}

clone_kamera_at() {
  local name="$1"
  local sha="$2"
  local destination="$3"

  if [[ ! -e "$destination" ]]; then
    echo "cloning Kamera source for $name at $sha"
    git clone --filter=blob:none --no-checkout "$kamera_repository" "$destination"
    git -C "$destination" checkout --detach "$sha"
  elif ! git -C "$destination" rev-parse --git-dir >/dev/null 2>&1; then
    echo "$destination exists but is not a Git checkout" >&2
    exit 1
  fi

  local actual
  actual="$(git -C "$destination" rev-parse HEAD)"
  if [[ "$actual" != "$sha" ]]; then
    echo "$name Kamera checkout has commit $actual; expected $sha" >&2
    exit 1
  fi
  if [[ -n "$(git -C "$destination" status --short)" ]]; then
    echo "$name Kamera checkout is not clean: $destination" >&2
    exit 1
  fi
}

apply_exact_patch() {
  local name="$1"
  local destination="$2"
  local patch="$3"

  if git -C "$destination" apply --check --unidiff-zero "$patch" 2>/dev/null; then
    echo "applying pinned $name adapter"
    git -C "$destination" apply --unidiff-zero "$patch"
  elif git -C "$destination" apply --reverse --check --unidiff-zero "$patch" 2>/dev/null; then
    echo "pinned $name adapter is already applied"
  else
    echo "$name checkout has changes that do not match the pinned adapter" >&2
    exit 1
  fi

  if ! cmp -s <(git -C "$destination" diff --binary --unified=0 --abbrev=8) "$patch"; then
    echo "$name working-tree changes differ from the pinned adapter" >&2
    exit 1
  fi
}

mkdir -p "$deps_root/kcp" "$deps_root/kar"
setup_python_environment

kcp_kamera_sha="d629dded905603903a3440095f20baf460358205"
kcp_sha="301a8f749e7b99a0c81f43b37aa5b5e5ff0fc0b4"
clone_kamera_at "KCP-4" "$kcp_kamera_sha" "$deps_root/kcp/kamera"
clone_at "KCP" "https://github.com/kcp-dev/kcp.git" "$kcp_sha" "$deps_root/kcp/kcp"

kar_kamera_sha="06bbe01af6545280b282e2d2a3f5964685b2bae5"
karpenter_sha="8ae07cf8b4ecf8ae3f04bc306d97f1ee40d21849"
karpenter_patch="$repo_root/artifact/section61/patches/karpenter-simulation.patch"
clone_kamera_at "KAR-12" "$kar_kamera_sha" "$deps_root/kar/kamera"
clone_at "Karpenter" "https://github.com/kubernetes-sigs/karpenter.git" "$karpenter_sha" "$deps_root/kar/karpenter"
apply_exact_patch "Karpenter simulation" "$deps_root/kar/karpenter" "$karpenter_patch"

"$repo_root/artifact/setup-figure8-kro-deps.sh" "$deps_root/kro"

cat <<EOF
Figure 8 dependencies are ready under $deps_root:
  Python: $venv_dir
  KCP-4: Kamera $kcp_kamera_sha, KCP $kcp_sha, packaged harness
  KRO-2: see $deps_root/kro
  KAR-12: Kamera $kar_kamera_sha, Karpenter $karpenter_sha + pinned adapter
EOF
