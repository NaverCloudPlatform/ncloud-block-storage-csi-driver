# Contributing to Ncloud Block Storage CSI Driver

Thank you for your interest in contributing to the Ncloud Block Storage CSI Driver!

## How to Contribute

1. Fork this repository
2. Create a feature branch (`git checkout -b feature/my-feature`)
3. Commit your changes (`git commit -m 'Add my feature'`)
4. Push to the branch (`git push origin feature/my-feature`)
5. Open a Pull Request

## Development

### Prerequisites

- Go 1.26+
- Docker (for building container images)
- A Kubernetes cluster on Ncloud VPC (NKS or self-managed) for end-to-end
  testing

### Build

```bash
make compile       # Build binary (linux/amd64)
make test          # Run unit tests
make image REGISTRY=your.registry.com   # Build Docker image
```

### Testing

```bash
# Unit tests
go test -v ./pkg/...

# Build verification
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o /dev/null ./cmd
```

### Running the binary

The image runs `/bin/ncloud-block-storage-csi-driver` with the arguments the chart sets. Run elsewhere, under csi-sanity for example, it takes the same flags and environment:

| Flag | Meaning |
|------|---------|
| `--mode` | `controller`, `node` or `all` (default `all`) |
| `--endpoint` | CSI socket; default `unix:///var/lib/kubelet/plugins/blk.csi.ncloud.com/csi.sock` |
| `--regionCode` | Ncloud API region; must be the region of the node's zone (default `KR`) |
| `--apiUrl` | API gateway origin, no path (see the `apiUrl` chart value) |
| `--defaultFsType` | `ext4`, `xfs`, or empty to detect from the host OS |
| `--legacy-topology-region-no` | Numeric `regionNo` advertised as a legacy node topology key (the `legacyRegionNo` chart value) |
| `--device-discovery-timeout` | Wait for the attached device to appear (default `60s`, `0` disables) |
| `--version` | Print the version and exit |

| Environment variable | Meaning |
|----------------------|---------|
| `NCLOUD_ACCESS_KEY`, `NCLOUD_SECRET_KEY` | API credentials (required) |
| `MAX_VOLUMES_PER_NODE` | Per-node attachment limit override (the `maxVolumesPerNode` chart value) |
| `NCLOUD_API_GW` | Read by the Ncloud SDK when `--apiUrl` is empty: the gateway origin, with the same `/vserver/v2` appended. The chart sets neither, so an empty `apiUrl` there means the public gateway |
| `NCLOUD_METADATA_API_ENDPOINT` | Replaces the instance metadata endpoint (`http://169.254.169.254`); a test hook, not for production |

The klog flags (`--v`, `--logtostderr`) are accepted as well.

### Runtime image

`Dockerfile` builds a static `linux/amd64` binary and places it on Alpine 3.24 with util-linux `mount`/`umount` (busybox `mount` skips the read-only remount a `readOnly` publish depends on), `e2fsprogs` and `xfsprogs` for formatting and growing filesystems, and `findmnt` and `blkid`. The container runs as root: the node plugin formats devices and mounts them on the host, and the controller shares the image.

### Dependencies

Dependencies are managed with [Go modules](https://go.dev/ref/mod). A `vendor/` directory is honored automatically when present, and the build works the same without one. After changing `go.mod`:

```bash
go mod tidy
go mod vendor   # only in a tree that carries vendor/
```

### File headers

Start every new file written for this project with the project header:

```go
// Ncloud Block Storage CSI Driver
// Copyright (c) 2026-present NAVER Cloud Corp.
// Apache-2.0
```

It goes above everything else, with a blank line after it — before a
`//go:build` constraint if the file has one, and before `package`. A file
derived from another project keeps that project's copyright notice: the
files that descend from digitalocean/csi-digitalocean (listed under
[Origin](README.md#origin) in the README) carry the project header followed
by a `Derived from …` notice that names the original work and states that
the file was modified, and the chart manifests adapted from the
kubernetes-csi repositories start with a `Modified from …` comment. Every
other Go file outside `vendor/` is written for this project and carries the
project header alone.

## Code of Conduct

This project follows the [Contributor Covenant](CODE_OF_CONDUCT.md).
Please be respectful and constructive in all interactions.

## Reporting security issues

Please do not open public issues for security problems — see
[SECURITY.md](SECURITY.md).

## License

By contributing, you agree that your contributions will be licensed under the Apache License 2.0.
