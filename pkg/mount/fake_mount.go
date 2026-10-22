// Ncloud Block Storage CSI Driver
// Copyright (c) 2026-present NAVER Cloud Corp.
// Apache-2.0

package mount

import (
	"fmt"
	"strings"
	"sync"
)

// fakeDevicePrefix marks paths the fake treats as block device nodes,
// mirroring the real mounter's /dev/-based device detection.
const fakeDevicePrefix = "/dev/"

type fakeMountEntry struct {
	source  string
	fsType  string
	options []string
}

// FakeMounter is an in-memory, stateful Mounter implementation for tests such
// as the csi-sanity suite. Unlike MockMounter (per-call function stubs), it
// tracks format and mount state so multi-step node flows (stage → publish →
// stats → unpublish → unstage) behave like a real host: mounts are visible to
// later calls, bind mounts resolve to their backing device, and statistics
// come from fixed values instead of the host filesystem.
type FakeMounter struct {
	mu        sync.Mutex
	formatted map[string]string         // device path -> filesystem type
	mounts    map[string]fakeMountEntry // target path -> mount entry
}

var _ Mounter = &FakeMounter{}

func NewFakeMounter() *FakeMounter {
	return &FakeMounter{
		formatted: map[string]string{},
		mounts:    map[string]fakeMountEntry{},
	}
}

func (f *FakeMounter) Format(source, fsType string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if source == "" || fsType == "" {
		return fmt.Errorf("source and fs type must be specified for formatting")
	}
	f.formatted[source] = fsType
	return nil
}

func (f *FakeMounter) Mount(source, target, fsType string, options ...string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if source == "" || target == "" {
		return fmt.Errorf("source and target must be specified for mounting")
	}
	if _, ok := f.mounts[target]; ok {
		return fmt.Errorf("target %q is already mounted", target)
	}
	f.mounts[target] = fakeMountEntry{source: source, fsType: fsType, options: options}
	return nil
}

// Unmount tolerates an unmounted target like the real implementation, whose
// CleanupMountPoint treats a non-mountpoint as already clean.
func (f *FakeMounter) Unmount(target string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	delete(f.mounts, target)
	return nil
}

func (f *FakeMounter) IsFormatted(source string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	_, ok := f.formatted[source]
	return ok, nil
}

func (f *FakeMounter) IsMounted(target string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	_, ok := f.mounts[target]
	return ok, nil
}

// MountInfo reports whether target is mounted and with which filesystem type
// (empty for raw block bind mounts). It lets tests answer path questions from
// fake state, e.g. the csi-sanity CheckPath hook.
func (f *FakeMounter) MountInfo(target string) (fsType string, ok bool) {
	f.mu.Lock()
	defer f.mu.Unlock()

	entry, ok := f.mounts[target]
	return entry.fsType, ok
}

// resolveDevice follows the mount chain from a path down to the device
// backing it: a publish target bind-mounted from a staging path resolves
// through the staging mount to the staged device. The second return value
// reports whether the path led to a device at all.
func (f *FakeMounter) resolveDevice(path string) (string, bool) {
	if strings.HasPrefix(path, fakeDevicePrefix) {
		return path, true
	}
	seen := map[string]bool{}
	for {
		if seen[path] {
			return "", false
		}
		seen[path] = true
		entry, ok := f.mounts[path]
		if !ok {
			return "", false
		}
		if strings.HasPrefix(entry.source, fakeDevicePrefix) {
			return entry.source, true
		}
		path = entry.source
	}
}

func (f *FakeMounter) GetDevicePathFromMount(mountPath string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if _, ok := f.mounts[mountPath]; !ok {
		return "", fmt.Errorf("could not get valid device for mount path: %q", mountPath)
	}
	device, ok := f.resolveDevice(mountPath)
	if !ok {
		return "", fmt.Errorf("could not get valid device for mount path: %q", mountPath)
	}
	return device, nil
}

func (f *FakeMounter) GetMountOptions(mountPath string) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	entry, ok := f.mounts[mountPath]
	if !ok {
		return nil, fmt.Errorf("could not determine mount options of %q: not mounted", mountPath)
	}
	return append([]string(nil), entry.options...), nil
}

func (f *FakeMounter) GetFsType(devicePath string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.formatted[devicePath], nil
}

func (f *FakeMounter) ResizeFs(volumePath string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if _, ok := f.mounts[volumePath]; !ok {
		return fmt.Errorf("volume path %q is not mounted", volumePath)
	}
	return nil
}

// GetStatistics returns fixed values: a fake mount has no real filesystem to
// measure. TotalBytes is large enough that any expansion request appears
// already satisfied at the device level.
func (f *FakeMounter) GetStatistics(volumePath string) (VolumeStatistics, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if !strings.HasPrefix(volumePath, fakeDevicePrefix) {
		if _, ok := f.mounts[volumePath]; !ok {
			return VolumeStatistics{}, fmt.Errorf("path %q is neither a device nor mounted", volumePath)
		}
	}
	const total = int64(16) * 1024 * 1024 * 1024 * 1024 // 16 TiB
	return VolumeStatistics{
		AvailableBytes:  total / 2,
		TotalBytes:      total,
		UsedBytes:       total / 2,
		AvailableInodes: 1000000,
		TotalInodes:     2000000,
		UsedInodes:      1000000,
	}, nil
}

func (f *FakeMounter) IsBlockDevice(volumePath string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if strings.HasPrefix(volumePath, fakeDevicePrefix) {
		return true, nil
	}
	// A bind mount of a device node (raw block publish) makes the target
	// itself a block device node; a filesystem mount target is a directory.
	if entry, ok := f.mounts[volumePath]; ok && entry.fsType == "" {
		if _, isDev := f.resolveDevice(volumePath); isDev {
			return true, nil
		}
	}
	return false, nil
}

func (f *FakeMounter) SameDevice(a, b string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	devA, okA := f.resolveDevice(a)
	devB, okB := f.resolveDevice(b)
	if !okA || !okB {
		return false, fmt.Errorf("%q and %q must both be block device nodes", a, b)
	}
	return devA == devB, nil
}
