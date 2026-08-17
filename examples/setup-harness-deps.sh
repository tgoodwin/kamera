#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
deps_root="$repo_root/artifact-deps/harnesses"

clone_and_patch() {
  local name="$1"
  local url="$2"
  local revision="$3"
  local patch="$4"
  local destination="$deps_root/$name"

  if [[ ! -e "$destination" ]]; then
    echo "cloning $name at $revision"
    git clone --filter=blob:none --no-checkout "$url" "$destination"
    git -C "$destination" -c advice.detachedHead=false checkout --detach "$revision"
  elif ! git -C "$destination" rev-parse --git-dir >/dev/null 2>&1; then
    echo "$destination exists but is not a Git checkout" >&2
    exit 1
  fi

  local actual
  actual="$(git -C "$destination" rev-parse HEAD)"
  if [[ "$actual" != "$revision" ]]; then
    echo "$name checkout has revision $actual; expected $revision" >&2
    echo "remove or relocate $destination, then rerun setup" >&2
    exit 1
  fi

  if git -C "$destination" apply --check --unidiff-zero "$patch" 2>/dev/null; then
    echo "applying $name harness adapter"
    git -C "$destination" apply --unidiff-zero "$patch"
  elif git -C "$destination" apply --reverse --check --unidiff-zero "$patch" 2>/dev/null; then
    echo "$name harness adapter is already applied"
  else
    echo "$name checkout has changes that do not match its pinned adapter" >&2
    exit 1
  fi

  if ! cmp -s <(git -C "$destination" diff --binary --unified=0 --abbrev=8) "$patch"; then
    echo "$name working-tree changes differ from its pinned adapter" >&2
    exit 1
  fi
}

mkdir -p "$deps_root"

clone_and_patch \
  karpenter \
  https://github.com/kubernetes-sigs/karpenter.git \
  8ae07cf8b4ecf8ae3f04bc306d97f1ee40d21849 \
  "$repo_root/artifact/section61/patches/karpenter-simulation.patch"

clone_and_patch \
  kratix \
  https://github.com/syntasso/kratix.git \
  4b813b5616d72dfbeb05633c3025d7e1dc85a3c7 \
  "$repo_root/examples/dependency-patches/kratix-simulation.patch"

clone_and_patch \
  kro \
  https://github.com/kro-run/kro.git \
  c9320ee963f745637bb622f6b68853a870187d20 \
  "$repo_root/artifact/figure8/kro-historical/kro-simulation.patch"

echo "harness dependencies are ready in $deps_root"
