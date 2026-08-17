#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
deps_root="${1:-$repo_root/artifact-deps/section61}"
[[ "$deps_root" = /* ]] || deps_root="$PWD/$deps_root"

kcp_url="https://github.com/kcp-dev/kcp.git"
kcp_sha="301a8f749e7b99a0c81f43b37aa5b5e5ff0fc0b4"
karpenter_url="https://github.com/kubernetes-sigs/karpenter.git"
karpenter_sha="8ae07cf8b4ecf8ae3f04bc306d97f1ee40d21849"
karpenter_patch="$repo_root/artifact/section61/patches/karpenter-simulation.patch"

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

mkdir -p "$deps_root"

kcp_dir="$deps_root/kcp"
clone_at "KCP" "$kcp_url" "$kcp_sha" "$kcp_dir"
if [[ -n "$(git -C "$kcp_dir" status --short)" ]]; then
  echo "KCP checkout is not clean: $kcp_dir" >&2
  exit 1
fi

karpenter_dir="$deps_root/karpenter"
clone_at "Karpenter" "$karpenter_url" "$karpenter_sha" "$karpenter_dir"
if git -C "$karpenter_dir" apply --check --unidiff-zero "$karpenter_patch" 2>/dev/null; then
  echo "applying pinned Karpenter simulation adapter"
  git -C "$karpenter_dir" apply --unidiff-zero "$karpenter_patch"
elif git -C "$karpenter_dir" apply --reverse --check --unidiff-zero "$karpenter_patch" 2>/dev/null; then
  echo "Karpenter simulation adapter is already applied"
else
  echo "Karpenter checkout has changes that do not match the pinned adapter" >&2
  exit 1
fi

if ! cmp -s <(git -C "$karpenter_dir" diff --binary --unified=0 --abbrev=8) "$karpenter_patch"; then
  echo "Karpenter working-tree changes differ from the pinned adapter" >&2
  exit 1
fi

"$repo_root/artifact/setup-figure8-kro-deps.sh" "$deps_root/kro"

cat <<EOF
Section 6.1 source dependencies are ready:
  KCP:       $kcp_dir ($kcp_sha)
  KRO-2:     $deps_root/kro (pinned Kamera and KRO adapter)
  Karpenter: $karpenter_dir ($karpenter_sha + pinned adapter)

Run:
  ./artifact/reproduce-section61.sh
EOF
