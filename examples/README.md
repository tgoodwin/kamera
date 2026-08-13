# Examples

Most harnesses live in-tree under their respective subdirectories. The KCP
harness is checked in at `examples/kcp`. The Cluster API harness lives in a
fork of the upstream project and is accessed through an intentionally
untracked local symlink:

| Path                   | Expected target                             |
| ---------------------- | ------------------------------------------- |
| `examples/cluster-api` | local checkout of `cluster-api/test/kamera` |

To set them up, clone the relevant fork and symlink it in, e.g.:

```sh
ln -s /path/to/cluster-api/test/kamera examples/cluster-api
```

The path is listed in `.gitignore` so the symlink stays local.
