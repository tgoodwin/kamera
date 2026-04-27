# Examples

Most harnesses live in-tree under their respective subdirectories. Two
harnesses live in forks of their upstream projects and are accessed via
local symlinks that are intentionally untracked:

| Path                  | Expected target                                  |
| --------------------- | ------------------------------------------------ |
| `examples/cluster-api`| local checkout of `cluster-api/test/kamera`      |
| `examples/kcp`        | local checkout of `kcp/kamera`                   |

To set them up, clone the relevant fork and symlink it in, e.g.:

```sh
ln -s /path/to/cluster-api/test/kamera examples/cluster-api
ln -s /path/to/kcp/kamera              examples/kcp
```

Both paths are listed in `.gitignore` so the symlinks stay local.
See `docs/plans/2026-03-17-kcp-in-repo-harness.md` for background.
