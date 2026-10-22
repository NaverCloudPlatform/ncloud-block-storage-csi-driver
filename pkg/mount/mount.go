// Ncloud Block Storage CSI Driver
// Copyright (c) 2026-present NAVER Cloud Corp.
// Apache-2.0
//
// Derived from digitalocean/csi-digitalocean (Apache-2.0), forked at v0.4.1
// and merged up to v1.1.2 with later code ported from v1.2.0, Copyright
// 2018-2020 DigitalOcean, through the in-house nks-csi fork, and modified
// by NAVER Cloud Corp.

// Package mount provides filesystem operations required by the CSI node service,
// including formatting, mounting, unmounting, and resizing block devices.
package mount

// VolumeStatistics holds capacity and inode usage data for a mounted volume.
type VolumeStatistics struct {
	AvailableBytes, TotalBytes, UsedBytes    int64
	AvailableInodes, TotalInodes, UsedInodes int64
}

// Mounter abstracts filesystem operations so they can be mocked in tests.
type Mounter interface {
	Format(source, fsType string) error
	Mount(source, target, fsType string, options ...string) error
	Unmount(target string) error
	IsFormatted(source string) (bool, error)
	IsMounted(target string) (bool, error)
	GetDevicePathFromMount(mountPath string) (string, error)
	GetMountOptions(mountPath string) ([]string, error)
	GetFsType(devicePath string) (string, error)
	ResizeFs(volumePath string) error
	GetStatistics(volumePath string) (VolumeStatistics, error)
	IsBlockDevice(volumePath string) (bool, error)
	// SameDevice reports whether two paths refer to the same block device
	// (by device number), independent of path spelling or bind mounts.
	SameDevice(a, b string) (bool, error)
}
