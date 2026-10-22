// Ncloud Block Storage CSI Driver
// Copyright (c) 2026-present NAVER Cloud Corp.
// Apache-2.0
//
// Derived from digitalocean/csi-digitalocean (Apache-2.0), forked at v0.4.1
// and merged up to v1.1.2 with later code ported from v1.2.0, Copyright
// 2018-2020 DigitalOcean, through the in-house nks-csi fork, and modified
// by NAVER Cloud Corp.

package mount

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sys/unix"
	"k8s.io/klog/v2"
	kmount "k8s.io/mount-utils"
	kexec "k8s.io/utils/exec"
)

const (
	// cmdTimeout bounds every external command the mounter runs. A device
	// with hung I/O can put mkfs/blkid/mount into an uninterruptible sleep;
	// without a deadline the node RPC never returns and kubelet retries
	// pile new goroutines onto the same stuck call.
	cmdTimeout = 2 * time.Minute

	// cmdWaitDelay closes the command's I/O pipes after the deadline kill
	// so output collection stops; it cannot unblock the final wait on a
	// process stuck in uninterruptible sleep — the kubelet-side RPC
	// timeout remains the last resort for that case.
	cmdWaitDelay = 10 * time.Second

	// umountForceTimeout is how long a graceful umount may take before
	// CleanupMountWithForce escalates to umount -f. Without an escalation
	// path a dead device leaves the pod stuck in Terminating forever.
	umountForceTimeout = 30 * time.Second
)

// runCmd executes an external command with a hard deadline. The returned
// error keeps *exec.ExitError semantics for exit-code inspection.
func runCmd(name string, args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), cmdTimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, name, args...)
	cmd.WaitDelay = cmdWaitDelay
	out, err := cmd.CombinedOutput()
	if ctx.Err() == context.DeadlineExceeded {
		return out, fmt.Errorf("command %q timed out after %s: %w", name+" "+strings.Join(args, " "), cmdTimeout, err)
	}
	return out, err
}

// verifyReadable distinguishes "no filesystem signature" from "device
// cannot be read": blkid reports both with the same exit code, and
// treating an unreadable device as unformatted would send mkfs at a
// volume that may hold data.
func verifyReadable(source string) error {
	// Dead hardware can put the read into an uninterruptible sleep; bound
	// it like every external command. On timeout the reader goroutine is
	// abandoned (it cannot be cancelled), which mirrors what happens to a
	// timed-out external process — one leaked goroutine beats a hung RPC.
	done := make(chan error, 1)
	go func() {
		f, err := os.Open(source)
		if err != nil {
			done <- err
			return
		}
		defer f.Close()
		buf := make([]byte, 512)
		if _, err := f.Read(buf); err != nil && err != io.EOF {
			done <- err
			return
		}
		done <- nil
	}()
	select {
	case err := <-done:
		return err
	case <-time.After(cmdTimeout):
		return fmt.Errorf("reading %s timed out after %s", source, cmdTimeout)
	}
}

type findmntResponse struct {
	FileSystems []fileSystem `json:"filesystems"`
}

type fileSystem struct {
	Target      string `json:"target"`
	Propagation string `json:"propagation"`
	FsType      string `json:"fstype"`
	Options     string `json:"options"`
}

// xfsLTSPreset pins mkfs.xfs to a feature set mountable by the oldest
// kernel family NKS nodes run (Ubuntu 22.04 5.15 / RHEL9 derivatives).
// Shipped by the xfsprogs package in the runtime image.
const xfsLTSPreset = "/usr/share/xfsprogs/mkfs/lts_5.15.conf"

type safeMounter struct {
	kMounter *kmount.SafeFormatAndMount
}

// mkfsArgs builds the mkfs argument list for a filesystem type. For xfs the
// on-disk features are pinned to an LTS-kernel preset: newer xfsprogs (7.x)
// otherwise enable superblock features that node kernels reject at mount
// time with "unknown incompatible features" (observed with xfsprogs 7.0.1
// against kernel 6.8). A missing preset is treated as a packaging error
// instead of silently formatting with the risky defaults.
func mkfsArgs(fsType, source, xfsPreset string) ([]string, error) {
	switch fsType {
	case "ext3", "ext4":
		return []string{"-F", source}, nil
	case "xfs":
		if _, err := os.Stat(xfsPreset); err != nil {
			return nil, fmt.Errorf("xfs feature preset %s is missing from the runtime image; refusing to format with mkfs.xfs defaults that may not mount on node kernels", xfsPreset)
		}
		// -f matches ext's -F. The driver only formats devices it found
		// unformatted, so neither flag is reached with an existing
		// filesystem; they are here so a stale signature on a recycled
		// device cannot stall the format.
		return []string{"-c", "options=" + xfsPreset, "-f", source}, nil
	default:
		return []string{source}, nil
	}
}

func NewSafeMounter() Mounter {
	kMounter := &kmount.SafeFormatAndMount{
		Interface: kmount.New(""),
		Exec:      kexec.New(),
	}
	return &safeMounter{kMounter: kMounter}
}

func (m *safeMounter) Format(source, fsType string) error {
	klog.V(4).Infof("Format: source=%s, fsType=%s", source, fsType)

	if fsType == "" {
		return errors.New("fs type is not specified for formatting the volume")
	}
	if source == "" {
		return errors.New("source is not specified for formatting the volume")
	}

	mkfsCmd := fmt.Sprintf("mkfs.%s", fsType)

	if _, err := exec.LookPath(mkfsCmd); err != nil {
		if errors.Is(err, exec.ErrNotFound) {
			return fmt.Errorf("%q executable not found in $PATH", mkfsCmd)
		}
		return err
	}

	mkfsArgs, err := mkfsArgs(fsType, source, xfsLTSPreset)
	if err != nil {
		return err
	}

	klog.V(2).Infof("executing format: cmd=%s, args=%v", mkfsCmd, mkfsArgs)
	out, err := runCmd(mkfsCmd, mkfsArgs...)
	if err != nil {
		return fmt.Errorf("formatting disk failed: %v cmd: '%s %s' output: %q",
			err, mkfsCmd, strings.Join(mkfsArgs, " "), string(out))
	}

	return nil
}

func (m *safeMounter) Mount(source, target, fsType string, opts ...string) error {
	klog.V(4).Infof("Mount: source=%s, target=%s, fsType=%s, opts=%v", source, target, fsType, opts)

	if source == "" {
		return errors.New("source is not specified for mounting the volume")
	}
	if target == "" {
		return errors.New("target is not specified for mounting the volume")
	}

	// Snapshot-restored volumes share the origin's filesystem UUID, and
	// the kernel refuses to mount an xfs with a UUID that is already
	// mounted ("Filesystem has duplicate UUID"; reproduced on NKS when a
	// restored volume lands on the origin's node). nouuid skips the check.
	if fsType == "xfs" && !slices.Contains(opts, "nouuid") {
		opts = append(opts, "nouuid")
	}

	mountCmd := "mount"
	mountArgs := []string{}

	if fsType == "" {
		if err := os.MkdirAll(filepath.Dir(target), 0750); err != nil {
			return fmt.Errorf("failed to create target directory for raw block bind mount: %v", err)
		}
		file, err := os.OpenFile(target, os.O_CREATE, 0660)
		if err != nil {
			return fmt.Errorf("failed to create target file for raw block bind mount: %v", err)
		}
		if err := file.Close(); err != nil {
			return fmt.Errorf("failed to close target file for raw block bind mount: %v", err)
		}
	} else {
		mountArgs = append(mountArgs, "-t", fsType)
		if err := os.MkdirAll(target, 0750); err != nil {
			return err
		}
	}

	if len(opts) > 0 {
		mountArgs = append(mountArgs, "-o", strings.Join(opts, ","))
	}

	mountArgs = append(mountArgs, source, target)

	klog.V(2).Infof("executing mount: cmd=%s, args=%v", mountCmd, mountArgs)
	out, err := runCmd(mountCmd, mountArgs...)
	if err != nil {
		// Clean up the target file created for raw block bind mount on failure.
		if fsType == "" {
			if removeErr := os.Remove(target); removeErr != nil && !os.IsNotExist(removeErr) {
				klog.Warningf("failed to clean up target file %s after mount failure: %v", target, removeErr)
			}
		}
		return fmt.Errorf("mounting failed: %v cmd: '%s %s' output: %q",
			err, mountCmd, strings.Join(mountArgs, " "), string(out))
	}

	return nil
}

func (m *safeMounter) Unmount(target string) error {
	klog.V(4).Infof("Unmount: target=%s", target)
	// Escalate to umount -f when a graceful unmount stalls (dead device):
	// without an escalation path the pod stays in Terminating forever and
	// the volume can never detach.
	if fu, ok := m.kMounter.Interface.(kmount.MounterForceUnmounter); ok {
		return kmount.CleanupMountWithForce(target, fu, true, umountForceTimeout)
	}
	return kmount.CleanupMountPoint(target, m.kMounter, true)
}

func (m *safeMounter) IsFormatted(source string) (bool, error) {
	klog.V(4).Infof("IsFormatted: source=%s", source)

	if source == "" {
		return false, errors.New("source is not specified")
	}

	blkidCmd := "blkid"
	if _, err := exec.LookPath(blkidCmd); err != nil {
		if errors.Is(err, exec.ErrNotFound) {
			return false, fmt.Errorf("%q executable not found in $PATH", blkidCmd)
		}
		return false, err
	}

	// -p probes the device directly instead of consulting the blkid cache.
	// Without it blkid can answer exit 0 with empty output for a device it
	// merely failed to read, which the old code took as "empty disk" — and
	// the caller would then mkfs a volume that may hold data.
	args := []string{"-p", "-o", "export", source}
	klog.V(2).Infof("checking if source is formatted: cmd=%s, args=%v", blkidCmd, args)
	out, err := runCmd(blkidCmd, args...)
	if err != nil {
		// util-linux blkid exits with code 2 both when the device carries
		// no recognizable signature and when it could not be read; only a
		// readable device may be treated as "not formatted".
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) && exitErr.ExitCode() == 2 {
			if readErr := verifyReadable(source); readErr != nil {
				return false, fmt.Errorf("blkid found no signature on %s but the device is not readable: %v", source, readErr)
			}
			return false, nil
		}
		return false, fmt.Errorf("checking formatting failed: %v cmd: %q output: %q",
			err, blkidCmd, string(out))
	}

	if strings.TrimSpace(string(out)) == "" {
		// Success with no probe data is an inconsistent answer; refusing
		// is safer than treating it as an empty disk.
		return false, fmt.Errorf("blkid returned success but no probe data for %s", source)
	}

	return true, nil
}

func (m *safeMounter) IsMounted(target string) (bool, error) {
	klog.V(4).Infof("IsMounted: target=%s", target)

	if target == "" {
		return false, errors.New("target is not specified for checking the mount")
	}

	findmntCmd := "findmnt"
	if _, err := exec.LookPath(findmntCmd); err != nil {
		if errors.Is(err, exec.ErrNotFound) {
			return false, fmt.Errorf("%q executable not found in $PATH", findmntCmd)
		}
		return false, err
	}

	findmntArgs := []string{"-o", "TARGET,PROPAGATION,FSTYPE,OPTIONS", "-M", target, "-J"}

	klog.V(2).Infof("checking if target is mounted: cmd=%s, args=%v", findmntCmd, findmntArgs)
	out, err := runCmd(findmntCmd, findmntArgs...)
	if err != nil {
		// findmnt exits 1 with no output when nothing is mounted at the
		// target. Any other failure (fork error, kill by the command
		// deadline, ...) must not read as "not mounted": callers skip
		// unmount on false, and kubelet would then detach a still-mounted
		// disk.
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) && exitErr.ExitCode() == 1 && strings.TrimSpace(string(out)) == "" {
			return false, nil
		}
		return false, fmt.Errorf("checking mounted failed: %v cmd: %q output: %q",
			err, findmntCmd, string(out))
	}

	if string(out) == "" {
		return false, nil
	}

	var resp *findmntResponse
	if err = json.Unmarshal(out, &resp); err != nil {
		return false, fmt.Errorf("couldn't unmarshal data: %q: %s", string(out), err)
	}

	// findmnt canonicalizes the -M argument when matching but prints the
	// canonical mountinfo path in TARGET; a request path with symlink
	// components or a trailing slash would compare unequal and read a
	// mounted path as "not mounted" — callers would skip the unmount and
	// kubelet would detach a still-mounted disk. Canonicalize the request
	// the same way before comparing.
	canonical := filepath.Clean(target)
	if resolved, err := filepath.EvalSymlinks(canonical); err == nil {
		canonical = resolved
	}

	targetFound := false
	for _, fs := range resp.FileSystems {
		if fs.Propagation != "shared" {
			return true, fmt.Errorf("mount propagation for target %q is not enabled", target)
		}
		if fs.Target == canonical {
			targetFound = true
		}
	}

	return targetFound, nil
}

func (m *safeMounter) GetDevicePathFromMount(mountPath string) (string, error) {
	args := []string{"-o", "source", "--noheadings", "--target", mountPath}
	output, err := runCmd("findmnt", args...)
	if err != nil {
		return "", fmt.Errorf("could not determine device path: %v", err)
	}

	devicePath := strings.TrimSpace(string(output))
	if len(devicePath) == 0 {
		return "", fmt.Errorf("could not get valid device for mount path: %q", mountPath)
	}

	return devicePath, nil
}

func (m *safeMounter) GetMountOptions(mountPath string) ([]string, error) {
	klog.V(4).Infof("GetMountOptions: mountPath=%s", mountPath)

	output, err := runCmd("findmnt", "-o", "OPTIONS", "--noheadings", "--target", mountPath)
	if err != nil {
		return nil, fmt.Errorf("could not determine mount options of %q: %v output: %q", mountPath, err, string(output))
	}

	opts := strings.TrimSpace(string(output))
	if opts == "" {
		return nil, nil
	}
	return strings.Split(opts, ","), nil
}

func (m *safeMounter) GetFsType(devicePath string) (string, error) {
	klog.V(4).Infof("GetFsType: devicePath=%s", devicePath)

	if devicePath == "" {
		return "", errors.New("device path is not specified for filesystem type detection")
	}

	// -p probes the device directly (no cache); a read failure then shows
	// up as a non-zero exit instead of a silent empty answer that would
	// skip the staged-device fsType compatibility check.
	out, err := runCmd("blkid", "-p", "-o", "value", "-s", "TYPE", devicePath)
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) && exitErr.ExitCode() == 2 {
			if readErr := verifyReadable(devicePath); readErr != nil {
				return "", fmt.Errorf("cannot detect filesystem type: device %s is not readable: %v", devicePath, readErr)
			}
			return "", fmt.Errorf("no filesystem signature on %s", devicePath)
		}
		return "", fmt.Errorf("failed to detect filesystem type for %s: %v, output: %q",
			devicePath, err, string(out))
	}

	return strings.TrimSpace(string(out)), nil
}

func (m *safeMounter) ResizeFs(volumePath string) error {
	klog.V(4).Infof("ResizeFs: volumePath=%s", volumePath)

	if volumePath == "" {
		return errors.New("volume path is not specified for resizing")
	}

	devicePath, err := m.GetDevicePathFromMount(volumePath)
	if err != nil {
		return fmt.Errorf("could not determine device path: %v", err)
	}

	fsType, err := m.GetFsType(devicePath)
	if err != nil {
		return fmt.Errorf("could not detect filesystem type: %v", err)
	}

	var cmd string
	var args []string
	switch fsType {
	case "ext3", "ext4":
		cmd = "resize2fs"
		args = []string{devicePath}
	case "xfs":
		// xfs_growfs operates on the mount path, not the device path.
		cmd = "xfs_growfs"
		args = []string{"-d", volumePath}
	default:
		return fmt.Errorf("unsupported filesystem type %q for resize", fsType)
	}

	klog.V(2).Infof("resizing filesystem: cmd=%s, args=%v", cmd, args)
	out, err := runCmd(cmd, args...)
	if err != nil {
		return fmt.Errorf("%s failed: %v cmd: '%s %s' output: %q",
			cmd, err, cmd, strings.Join(args, " "), string(out))
	}

	return nil
}

func (m *safeMounter) GetStatistics(volumePath string) (VolumeStatistics, error) {
	klog.V(4).Infof("GetStatistics: volumePath=%s", volumePath)

	isBlock, err := m.IsBlockDevice(volumePath)
	if err != nil {
		return VolumeStatistics{}, fmt.Errorf("failed to determine if volume %s is block device: %v", volumePath, err)
	}

	if isBlock {
		output, err := runCmd("blockdev", "--getsize64", volumePath)
		if err != nil {
			return VolumeStatistics{}, fmt.Errorf("error getting size of block volume at %s: output: %s, err: %v", volumePath, string(output), err)
		}
		strOut := strings.TrimSpace(string(output))
		gotSizeBytes, err := strconv.ParseInt(strOut, 10, 64)
		if err != nil {
			return VolumeStatistics{}, fmt.Errorf("failed to parse size %s into int", strOut)
		}
		return VolumeStatistics{TotalBytes: gotSizeBytes}, nil
	}

	var statfs unix.Statfs_t
	err = unix.Statfs(volumePath, &statfs)
	if err != nil {
		return VolumeStatistics{}, err
	}

	return VolumeStatistics{
		AvailableBytes:  int64(statfs.Bavail) * int64(statfs.Bsize),
		TotalBytes:      int64(statfs.Blocks) * int64(statfs.Bsize),
		UsedBytes:       (int64(statfs.Blocks) - int64(statfs.Bfree)) * int64(statfs.Bsize),
		AvailableInodes: int64(statfs.Ffree),
		TotalInodes:     int64(statfs.Files),
		UsedInodes:      int64(statfs.Files) - int64(statfs.Ffree),
	}, nil
}

func (m *safeMounter) IsBlockDevice(devicePath string) (bool, error) {
	var stat unix.Stat_t
	err := unix.Stat(devicePath, &stat)
	if err != nil {
		return false, err
	}
	return (stat.Mode & unix.S_IFMT) == unix.S_IFBLK, nil
}

// SameDevice reports whether two paths refer to the same block device by
// comparing device numbers. This works for bind-mounted device nodes whose
// findmnt source strings are not comparable (devtmpfs subpaths).
func (m *safeMounter) SameDevice(a, b string) (bool, error) {
	var sa, sb unix.Stat_t
	if err := unix.Stat(a, &sa); err != nil {
		return false, fmt.Errorf("failed to stat %q: %w", a, err)
	}
	if err := unix.Stat(b, &sb); err != nil {
		return false, fmt.Errorf("failed to stat %q: %w", b, err)
	}
	if (sa.Mode&unix.S_IFMT) != unix.S_IFBLK || (sb.Mode&unix.S_IFMT) != unix.S_IFBLK {
		return false, fmt.Errorf("%q and %q must both be block device nodes", a, b)
	}
	return sa.Rdev == sb.Rdev, nil
}
