#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
deps_root="${1:-$repo_root/artifact-deps/figure8-kro}"
[[ "$deps_root" = /* ]] || deps_root="$PWD/$deps_root"
kamera_repository="${KAMERA_AE_KAMERA_REPOSITORY:-https://github.com/tgoodwin/kamera.git}"

kro_url="https://github.com/kro-run/kro.git"
kro_sha="c9320ee963f745637bb622f6b68853a870187d20"
kro_patch="$repo_root/artifact/figure8/kro-historical/kro-simulation.patch"
kamera_sha="1c85e5b89fa46cc8470dbd63159d6640921fdeee"

mkdir -p "$deps_root"
kamera_dir="$deps_root/kamera-paper"
if [[ ! -e "$kamera_dir" ]]; then
  echo "cloning Kamera at $kamera_sha"
  git clone --filter=blob:none --no-checkout "$kamera_repository" "$kamera_dir"
  git -C "$kamera_dir" checkout --detach "$kamera_sha"
elif ! git -C "$kamera_dir" rev-parse --git-dir >/dev/null 2>&1; then
  echo "$kamera_dir exists but is not a Git checkout" >&2
  exit 1
fi
actual="$(git -C "$kamera_dir" rev-parse HEAD)"
if [[ "$actual" != "$kamera_sha" ]]; then
  echo "Kamera checkout has commit $actual; expected $kamera_sha" >&2
  echo "remove or relocate $kamera_dir, then rerun setup" >&2
  exit 1
fi
if [[ -n "$(git -C "$kamera_dir" status --short)" ]]; then
  echo "reconstructed Kamera checkout is not clean: $kamera_dir" >&2
  exit 1
fi

kro_dir="$deps_root/kro"

if [[ ! -e "$kro_dir" ]]; then
  echo "cloning KRO at $kro_sha"
  git clone --filter=blob:none --no-checkout "$kro_url" "$kro_dir"
  git -C "$kro_dir" checkout --detach "$kro_sha"
elif ! git -C "$kro_dir" rev-parse --git-dir >/dev/null 2>&1; then
  echo "$kro_dir exists but is not a Git checkout" >&2
  exit 1
fi

actual="$(git -C "$kro_dir" rev-parse HEAD)"
if [[ "$actual" != "$kro_sha" ]]; then
  echo "KRO checkout has commit $actual; expected $kro_sha" >&2
  echo "remove or relocate $kro_dir, then rerun setup" >&2
  exit 1
fi

if git -C "$kro_dir" apply --check --unidiff-zero "$kro_patch" 2>/dev/null; then
  echo "applying pinned KRO-2 simulation adapter"
  git -C "$kro_dir" apply --unidiff-zero "$kro_patch"
elif git -C "$kro_dir" apply --reverse --check --unidiff-zero "$kro_patch" 2>/dev/null; then
  echo "pinned KRO-2 simulation adapter is already applied"
else
  echo "KRO checkout has changes that do not match the pinned adapter" >&2
  exit 1
fi

if ! cmp -s <(git -C "$kro_dir" diff --binary --unified=0 --abbrev=8) "$kro_patch"; then
  echo "KRO working-tree changes differ from the pinned adapter" >&2
  exit 1
fi

cat <<EOF
Figure 8 KRO-2 dependency is ready:
  Kamera: $kamera_dir ($kamera_sha)
  KRO: $kro_dir ($kro_sha + pinned adapter)
EOF
