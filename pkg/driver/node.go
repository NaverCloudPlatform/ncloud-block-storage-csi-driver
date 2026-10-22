// Ncloud Block Storage CSI Driver
// Copyright (c) 2026-present NAVER Cloud Corp.
// Apache-2.0
//
// Derived from digitalocean/csi-digitalocean (Apache-2.0), forked at v0.4.1
// and merged up to v1.1.2 with later code ported from v1.2.0, Copyright
// 2018-2020 DigitalOcean, through the in-house nks-csi fork, and modified
// by NAVER Cloud Corp.

package driver

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/kubernetes-csi/csi-lib-utils/protosanitizer"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"

	"github.com/NaverCloudPlatform/ncloud-block-storage-csi-driver/pkg/utils"
)

// ncloudVolumeIDPattern matches Ncloud block storage instance numbers.
// A volume ID flows into /dev path construction (and from there into
// mkfs/mount argv), so anything but a plain number must be rejected
// before it can traverse outside /dev/disk/by-id (e.g. "/../../../vda"
// resolving to the node's root disk).
var ncloudVolumeIDPattern = regexp.MustCompile(`^[0-9]+$`)

const publishContextKeyServerInstanceNo = "serverInstanceNo"

const (
	diskIDPath          = "/dev/disk/by-id"
	diskNcloudKVMPrefix = "virtio-"

	volumeModeBlock      = "block"
	volumeModeFilesystem = "filesystem"

	// Timeout and poll interval for waiting on unmount operations.
	unmountTimeout      = 5 * time.Minute
	unmountPollInterval = 5 * time.Second
)

var nodeCaps = []csi.NodeServiceCapability_RPC_Type{
	csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME,
	csi.NodeServiceCapability_RPC_GET_VOLUME_STATS,
	csi.NodeServiceCapability_RPC_EXPAND_VOLUME,
}

func (d *Driver) NodeStageVolume(ctx context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	klog.V(4).Infof("[NodeStageVolume] called with args %+v", protosanitizer.StripSecrets(req))

	if req.VolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "NodeStageVolume Volume ID must be provided")
	}
	if req.StagingTargetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "NodeStageVolume Staging Target Path must be provided")
	}
	if req.VolumeCapability == nil {
		return nil, status.Error(codes.InvalidArgument, "NodeStageVolume Volume Capability must be provided")
	}

	if violations := validateCapabilities([]*csi.VolumeCapability{req.VolumeCapability}); len(violations) > 0 {
		return nil, status.Errorf(codes.InvalidArgument, "unsupported volume capability: %s", strings.Join(violations, "; "))
	}

	// The kubelet retries after two minutes, the same deadline the mount
	// helpers run under, so a retry can arrive while the first call is
	// still inside mkfs or mount. CSI allows ABORTED for a concurrent
	// request on the same volume; the CO retries it. Taken after the
	// argument checks so a malformed request never claims a volume.
	if !d.inFlight.acquire(req.VolumeId) {
		return nil, status.Errorf(codes.Aborted, "another operation on volume %q is already in progress", req.VolumeId)
	}
	defer d.inFlight.release(req.VolumeId)

	klog.V(2).Infof("[NodeStageVolume] volume=%s, stagingTargetPath=%s", req.VolumeId, req.StagingTargetPath)

	switch req.VolumeCapability.GetAccessType().(type) {
	case *csi.VolumeCapability_Block:
		return &csi.NodeStageVolumeResponse{}, nil
	}

	source, err := d.getVolumeSource(ctx, req.VolumeId, req.PublishContext, true)
	if err != nil {
		return nil, deviceResolutionStatus(req.VolumeId, err)
	}

	target := req.StagingTargetPath
	mnt := req.VolumeCapability.GetMount()
	if mnt == nil {
		mnt = &csi.VolumeCapability_MountVolume{
			FsType:     d.defaultFsType,
			MountFlags: []string{},
		}
	} else {
		klog.V(2).Infof("VolumeCapability: %v", mnt)
	}
	options := mnt.MountFlags

	fsType := d.defaultFsType
	if mnt.FsType != "" {
		fsType = mnt.FsType
	}

	// The mounted check comes before any format decision: a retry of an
	// already-staged volume must return idempotent success (or a
	// compatibility error) without ever reaching the mkfs branch below.
	mounted, err := d.mounter.IsMounted(target)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to check staging target %q: %v", target, err)
	}

	if mounted {
		// Idempotent success is only allowed when the existing mount is
		// compatible with this request: same backing device and, when a
		// filesystem type is requested, the same on-disk filesystem.
		if err := d.verifyMountedDevice(target, source); err != nil {
			return nil, err
		}
		if fsType != "" {
			actualFsType, err := d.mounter.GetFsType(source)
			if err != nil {
				return nil, status.Errorf(codes.Internal, "failed to detect filesystem type of %q: %v", source, err)
			}
			if actualFsType != "" && actualFsType != fsType {
				return nil, status.Errorf(codes.AlreadyExists,
					"staging target %q is already mounted with filesystem %q, which does not match the requested %q",
					target, actualFsType, fsType)
			}
		}
		klog.V(2).Infof("[NodeStageVolume] source device(%s) is already mounted to target(%s)", source, target)
		klog.V(2).Infof("[NodeStageVolume] succeeded: volume=%s, stage=%s", req.VolumeId, target)
		return &csi.NodeStageVolumeResponse{}, nil
	}

	formatted, err := d.mounter.IsFormatted(source)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to check whether device %q is formatted: %v", source, err)
	}

	// Unformatted devices are formatted; already-formatted devices are
	// never re-formatted. The noformat volume attribute (which could force
	// a destructive re-format on every stage) is ignored.
	needsFormat := !formatted

	if needsFormat {
		klog.V(2).Infof("[NodeStageVolume] formatting volume(%s) for source(%s)", req.VolumeId, source)
		if err := d.mounter.Format(source, fsType); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to format volume %q: %v", req.VolumeId, err)
		}
	} else {
		klog.V(2).Infof("[NodeStageVolume] source device(%s) does not need formatting, skipping", source)
	}

	klog.V(2).Infof("[NodeStageVolume] mounting volume(%s) for staging(%s)", req.VolumeId, target)
	if err := d.mounter.Mount(source, target, fsType, options...); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to mount volume %q to %q: %v", req.VolumeId, target, err)
	}

	klog.V(2).Infof("[NodeStageVolume] succeeded: volume=%s, stage=%s", req.VolumeId, target)
	return &csi.NodeStageVolumeResponse{}, nil
}

func (d *Driver) NodeUnstageVolume(ctx context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	klog.V(4).Infof("[NodeUnstageVolume] called with args %+v", protosanitizer.StripSecrets(req))

	if req.VolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "NodeUnstageVolume Volume ID must be provided")
	}
	if req.StagingTargetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "NodeUnstageVolume Staging Target Path must be provided")
	}

	if !d.inFlight.acquire(req.VolumeId) {
		return nil, status.Errorf(codes.Aborted, "another operation on volume %q is already in progress", req.VolumeId)
	}
	defer d.inFlight.release(req.VolumeId)

	klog.V(2).Infof("[NodeUnstageVolume] volume=%s, stagingTargetPath=%s", req.VolumeId, req.StagingTargetPath)

	mounted, err := d.mounter.IsMounted(req.StagingTargetPath)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to check staging target %q: %v", req.StagingTargetPath, err)
	}

	if mounted {
		klog.V(2).Infof("[NodeUnstageVolume] unmounting volume(%s) from staging target(%s)", req.VolumeId, req.StagingTargetPath)
		if err := d.mounter.Unmount(req.StagingTargetPath); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to unmount staging target %q: %v", req.StagingTargetPath, err)
		}
	} else {
		klog.V(2).Infof("[NodeUnstageVolume] staging target(%s) of volume(%s) is already unmounted", req.StagingTargetPath, req.VolumeId)
	}

	ctx, cancel := context.WithTimeout(ctx, unmountTimeout)
	defer cancel()

	ticker := time.NewTicker(unmountPollInterval)
	defer ticker.Stop()
	// Check-then-wait: the unmount above is synchronous, so the first
	// check usually succeeds without waiting a poll interval.
	for {
		mounted, err = d.mounter.IsMounted(req.StagingTargetPath)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to check staging target %q: %v", req.StagingTargetPath, err)
		}
		if !mounted {
			klog.V(2).Infof("[NodeUnstageVolume] succeeded: volume=%s", req.VolumeId)
			return &csi.NodeUnstageVolumeResponse{}, nil
		}
		klog.Warningf("[NodeUnstageVolume] volume(%s) still mounted at staging target(%s)", req.VolumeId, req.StagingTargetPath)

		select {
		case <-ticker.C:
		case <-ctx.Done():
			return nil, status.Errorf(codes.Internal, "timeout waiting for unmount of volume %s from staging target %s", req.VolumeId, req.StagingTargetPath)
		}
	}
}

func (d *Driver) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	klog.V(4).Infof("[NodePublishVolume] called with args %+v", protosanitizer.StripSecrets(req))

	if req.VolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "NodePublishVolume Volume ID must be provided")
	}
	if req.StagingTargetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "NodePublishVolume Staging Target Path must be provided")
	}
	if req.TargetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "NodePublishVolume Target Path must be provided")
	}
	if req.VolumeCapability == nil {
		return nil, status.Error(codes.InvalidArgument, "NodePublishVolume Volume Capability must be provided")
	}

	if violations := validateCapabilities([]*csi.VolumeCapability{req.VolumeCapability}); len(violations) > 0 {
		return nil, status.Errorf(codes.InvalidArgument, "unsupported volume capability: %s", strings.Join(violations, "; "))
	}

	// After argument validation, like the other node RPCs: a malformed
	// request is answered as such without touching the volume's guard.
	if !d.inFlight.acquire(req.VolumeId) {
		return nil, status.Errorf(codes.Aborted, "another operation on volume %q is already in progress", req.VolumeId)
	}
	defer d.inFlight.release(req.VolumeId)

	klog.V(2).Infof("[NodePublishVolume] volume=%s, stagingPath=%s, targetPath=%s", req.VolumeId, req.StagingTargetPath, req.TargetPath)

	options := []string{"bind"}
	if req.Readonly {
		options = append(options, "ro")
	}

	var err error
	switch req.GetVolumeCapability().GetAccessType().(type) {
	case *csi.VolumeCapability_Block:
		err = d.nodePublishVolumeForBlock(ctx, req, options)
	case *csi.VolumeCapability_Mount:
		err = d.nodePublishVolumeForFileSystem(req, options)
	default:
		return nil, status.Error(codes.InvalidArgument, "Unknown access type")
	}

	if err != nil {
		return nil, err
	}

	klog.V(2).Infof("[NodePublishVolume] succeeded: volume=%s, target=%s", req.VolumeId, req.TargetPath)
	return &csi.NodePublishVolumeResponse{}, nil
}

func (d *Driver) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	klog.V(4).Infof("[NodeUnpublishVolume] called with args %+v", protosanitizer.StripSecrets(req))

	if req.VolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "NodeUnpublishVolume Volume ID must be provided")
	}
	if req.TargetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "NodeUnpublishVolume Target Path must be provided")
	}

	if !d.inFlight.acquire(req.VolumeId) {
		return nil, status.Errorf(codes.Aborted, "another operation on volume %q is already in progress", req.VolumeId)
	}
	defer d.inFlight.release(req.VolumeId)

	klog.V(2).Infof("[NodeUnpublishVolume] volume=%s, targetPath=%s", req.VolumeId, req.TargetPath)

	mounted, err := d.mounter.IsMounted(req.TargetPath)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to check target %q: %v", req.TargetPath, err)
	}

	if mounted {
		klog.V(2).Infof("[NodeUnpublishVolume] unmounting volume(%s) from target(%s)", req.VolumeId, req.TargetPath)
		if err := d.mounter.Unmount(req.TargetPath); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to unmount target %q: %v", req.TargetPath, err)
		}
	} else {
		klog.V(2).Infof("[NodeUnpublishVolume] volume(%s) is already unmounted from target(%s)", req.VolumeId, req.TargetPath)
	}

	ctx, cancel := context.WithTimeout(ctx, unmountTimeout)
	defer cancel()

	ticker := time.NewTicker(unmountPollInterval)
	defer ticker.Stop()
	// Check-then-wait: the unmount above is synchronous, so the first
	// check usually succeeds without waiting a poll interval.
	for {
		mounted, err = d.mounter.IsMounted(req.TargetPath)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to check target %q: %v", req.TargetPath, err)
		}
		if !mounted {
			klog.V(2).Infof("[NodeUnpublishVolume] succeeded: volume=%s", req.VolumeId)
			return &csi.NodeUnpublishVolumeResponse{}, nil
		}
		klog.V(2).Infof("[NodeUnpublishVolume] volume(%s) still mounted at target(%s)", req.VolumeId, req.TargetPath)

		select {
		case <-ticker.C:
		case <-ctx.Done():
			return nil, status.Errorf(codes.Internal, "timeout waiting for unmount of volume %s from target %s", req.VolumeId, req.TargetPath)
		}
	}
}

func (d *Driver) NodeGetCapabilities(_ context.Context, req *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	klog.V(4).Infof("NodeGetCapabilities: called with args %+v", protosanitizer.StripSecrets(req))

	var caps []*csi.NodeServiceCapability
	for _, cap := range nodeCaps {
		c := &csi.NodeServiceCapability{
			Type: &csi.NodeServiceCapability_Rpc{
				Rpc: &csi.NodeServiceCapability_RPC{
					Type: cap,
				},
			},
		}
		caps = append(caps, c)
	}
	return &csi.NodeGetCapabilitiesResponse{Capabilities: caps}, nil
}

func (d *Driver) NodeGetInfo(_ context.Context, req *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	klog.V(4).Infof("[NodeGetInfo] called with args %+v", protosanitizer.StripSecrets(req))

	// Startup fails fast when no limit could be determined, so in node
	// mode this is always set (override or API-reported).
	limit := d.maxVolumesPerNode

	// Startup resolves the zone from the metadata service and refuses to
	// run when it is empty or absent from the zone table, so in node mode
	// this is always set; the check is here so a future caller that skips
	// that path cannot register an empty topology label.
	segments := make(map[string]string)
	if d.zoneCode != "" {
		segments[corev1.LabelTopologyZone] = d.topologyZoneValue
		// Advertise the legacy "zoneNo" key alongside the standard one so
		// existing PVs and StorageClasses with that topology key keep
		// scheduling. Deprecated; removal requires a major version.
		// Same value as the standard key.
		segments[legacyTopologyZoneKey] = d.topologyZoneValue
	}
	// PVs with the legacy affinity require the regionNo label in addition
	// to zoneNo (ANDed nodeAffinity); see legacyTopologyRegionKey.
	if d.legacyRegionNo != "" {
		segments[legacyTopologyRegionKey] = d.legacyRegionNo
	}

	klog.V(2).Infof("[NodeGetInfo] nodeId=%s, maxVolumesPerNode=%d, topologySegments=%v", d.serverInstanceNo, limit, segments)
	return &csi.NodeGetInfoResponse{
		NodeId:            d.serverInstanceNo,
		MaxVolumesPerNode: limit,
		AccessibleTopology: &csi.Topology{
			Segments: segments,
		},
	}, nil
}

func (d *Driver) NodeGetVolumeStats(ctx context.Context, req *csi.NodeGetVolumeStatsRequest) (*csi.NodeGetVolumeStatsResponse, error) {
	klog.V(4).Infof("NodeGetVolumeStats: called with args %+v", protosanitizer.StripSecrets(req))

	if req.VolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "NodeGetVolumeStats Volume ID must be provided")
	}

	volumePath := req.VolumePath
	if volumePath == "" {
		return nil, status.Error(codes.InvalidArgument, "NodeGetVolumeStats Volume Path must be provided")
	}

	mounted, err := d.mounter.IsMounted(volumePath)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to check if volume %q path %q is mounted: %v", req.VolumeId, volumePath, err)
	}
	// CSI requires NOT_FOUND when the volume does not exist on the given
	// path. Corrupted-but-present mounts still pass here (mountinfo lists
	// them) and surface below as Internal when the statistics call hits
	// the broken filesystem.
	if !mounted {
		return nil, status.Errorf(codes.NotFound, "volume %q does not exist on path %q: path is not mounted", req.VolumeId, volumePath)
	}

	// The volume must actually be the one backing volumePath. The expected
	// device comes from the same by-id resolution staging uses; failure
	// there means the volume is not attached to this node at all.
	expectedDevice, err := d.getVolumeSource(ctx, req.VolumeId, nil, false)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "volume %q not found on this node: %v", req.VolumeId, err)
	}
	mountedDevice, err := d.mounter.GetDevicePathFromMount(volumePath)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to inspect the mount at %q: %v", volumePath, err)
	}
	if stripBindSubpath(mountedDevice) != expectedDevice {
		// Bind-mounted device nodes (raw block publish) resolve to devtmpfs
		// subpaths, so identity only holds at the device-number level.
		same, sameErr := d.mounter.SameDevice(expectedDevice, volumePath)
		if sameErr != nil {
			// A stat failure is not "wrong volume" — surface it instead
			// of folding it into NotFound and losing the cause.
			return nil, status.Errorf(codes.Internal, "failed to compare device identity of %q and %q: %v", expectedDevice, volumePath, sameErr)
		}
		if !same {
			return nil, status.Errorf(codes.NotFound, "volume %q does not exist on path %q", req.VolumeId, volumePath)
		}
	}

	isBlock, err := d.mounter.IsBlockDevice(volumePath)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to determine if %q is block device: %s", volumePath, err)
	}

	stats, err := d.mounter.GetStatistics(volumePath)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to retrieve capacity statistics for volume path %q: %s", volumePath, err)
	}

	if isBlock {
		klog.V(2).Infof("node capacity statistics: volume_mode=%s, bytes_total=%d", volumeModeBlock, stats.TotalBytes)
		return &csi.NodeGetVolumeStatsResponse{
			Usage: []*csi.VolumeUsage{
				{
					Unit:  csi.VolumeUsage_BYTES,
					Total: stats.TotalBytes,
				},
			},
		}, nil
	}

	return &csi.NodeGetVolumeStatsResponse{
		Usage: []*csi.VolumeUsage{
			{
				Available: stats.AvailableBytes,
				Total:     stats.TotalBytes,
				Used:      stats.UsedBytes,
				Unit:      csi.VolumeUsage_BYTES,
			},
			{
				Available: stats.AvailableInodes,
				Total:     stats.TotalInodes,
				Used:      stats.UsedInodes,
				Unit:      csi.VolumeUsage_INODES,
			},
		},
	}, nil
}

func (d *Driver) NodeExpandVolume(_ context.Context, req *csi.NodeExpandVolumeRequest) (*csi.NodeExpandVolumeResponse, error) {
	klog.V(4).Infof("[NodeExpandVolume] called with args %+v", protosanitizer.StripSecrets(req))

	volumeID := req.GetVolumeId()
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID not provided")
	}

	volumePath := req.GetVolumePath()
	if volumePath == "" {
		return nil, status.Error(codes.InvalidArgument, "Volume path not provided")
	}

	// capacity_range is optional for NodeExpandVolume (unlike the controller
	// RPC): without it the filesystem is grown to whatever the underlying
	// device provides, which is what resize2fs/xfs_growfs do anyway.
	var requestedSize int64
	if capRange := req.GetCapacityRange(); capRange != nil {
		var err error
		requestedSize, err = utils.ExtractStorage(capRange)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid capacity range: %v", err)
		}
	}

	klog.V(2).Infof("[NodeExpandVolume] volume=%s, volumePath=%s, requestedSize=%d bytes (%d GiB)", volumeID, volumePath, requestedSize, requestedSize/1024/1024/1024)

	if cap := req.GetVolumeCapability(); cap != nil {
		if violations := validateCapabilities([]*csi.VolumeCapability{cap}); len(violations) > 0 {
			return nil, status.Errorf(codes.InvalidArgument, "unsupported volume capability: %s", strings.Join(violations, "; "))
		}
		switch cap.GetAccessType().(type) {
		case *csi.VolumeCapability_Block:
			klog.V(2).Info("filesystem expansion is skipped for block volumes")
			return &csi.NodeExpandVolumeResponse{}, nil
		}
	}

	// resize2fs and xfs_growfs run under the same two-minute kubelet
	// deadline as the mount helpers, so a retry could reach the filesystem
	// while the first call is still growing it. Same guard, same place
	// (after argument validation) as the other node RPCs.
	if !d.inFlight.acquire(volumeID) {
		return nil, status.Errorf(codes.Aborted, "another operation on volume %q is already in progress", volumeID)
	}
	defer d.inFlight.release(volumeID)

	mounted, err := d.mounter.IsMounted(volumePath)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to check if volume path %q is mounted: %s", volumePath, err)
	}
	if !mounted {
		return nil, status.Errorf(codes.NotFound, "volume path %q is not mounted", volumePath)
	}

	devicePath, err := d.mounter.GetDevicePathFromMount(volumePath)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get device path for volumePath(%s): %v", volumePath, err)
	}
	// findmnt reports bind mounts as "source[/subpath]"; the statistics
	// call needs the plain device node (same handling as NodeGetVolumeStats).
	devicePath = stripBindSubpath(devicePath)

	stats, err := d.mounter.GetStatistics(devicePath)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get device size for %s: %v", devicePath, err)
	}

	if stats.TotalBytes < requestedSize {
		return nil, status.Errorf(codes.Aborted,
			"device(%s) size(%d bytes) is less than requested size(%d bytes), waiting for device expansion",
			devicePath, stats.TotalBytes, requestedSize)
	}

	if err := d.mounter.ResizeFs(volumePath); err != nil {
		return nil, status.Errorf(codes.Internal, "Could not resize volume %q (%q): %v", volumeID, volumePath, err)
	}

	klog.V(2).Infof("[NodeExpandVolume] succeeded: volume=%s, volumePath=%s", volumeID, volumePath)
	return &csi.NodeExpandVolumeResponse{}, nil
}

// getVolumeSource resolves the volume's device node. waitForDevice enables
// the by-id symlink wait for the attach paths (stage, block publish); the
// verification paths (volume stats) check exactly once — an absent device
// there is an answer, not a race to wait out.
func (d *Driver) getVolumeSource(ctx context.Context, volumeId string, publishContext map[string]string, waitForDevice bool) (string, error) {
	klog.V(2).Infof("getVolumeSource: volumeId=%s", volumeId)

	if !ncloudVolumeIDPattern.MatchString(volumeId) {
		return "", fmt.Errorf("volume ID %q is not a valid Ncloud block storage instance number", volumeId)
	}

	if targetNode, ok := publishContext[publishContextKeyServerInstanceNo]; ok {
		if targetNode != d.serverInstanceNo {
			return "", fmt.Errorf("volume %s is not attached to this node (expected=%s, actual=%s)",
				volumeId, d.serverInstanceNo, targetNode)
		}
	}

	opts := deviceDiscoveryOptions{byIDDir: diskIDPath, interval: deviceDiscoveryInterval}
	if waitForDevice {
		opts.timeout = d.deviceDiscoveryTimeout
	}
	return resolveDevicePath(ctx, opts, diskNcloudKVMPrefix, volumeId)
}

// deviceResolutionStatus wraps a device-resolution failure on an attach
// path. A wait ended by cancellation or deadline is not an Internal driver
// fault, so it carries the context's code — decided from the returned
// error, not from ctx.Err(): the same path also rejects malformed volume
// IDs, and those must not be reported as Canceled just because the
// request happened to be cancelled by then.
func deviceResolutionStatus(volumeID string, err error) error {
	switch {
	case errors.Is(err, context.Canceled):
		return status.Errorf(codes.Canceled, "failed to get volume source for %q: %v", volumeID, err)
	case errors.Is(err, context.DeadlineExceeded):
		return status.Errorf(codes.DeadlineExceeded, "failed to get volume source for %q: %v", volumeID, err)
	}
	return status.Errorf(codes.Internal, "failed to get volume source for %q: %v", volumeID, err)
}

// resolveDevicePath is indirected so unit tests can stub device resolution,
// which otherwise requires a real /dev/disk/by-id entry.
var resolveDevicePath = findAbsoluteDeviceByIDPath

const (
	// After the attach API returns, the hypervisor still has to hot-plug
	// the virtio disk into the VM and udev has to create the by-id
	// symlink, which can take tens of seconds. The attach paths poll for
	// the symlink instead of failing on the first miss.
	deviceDiscoveryInterval = 1 * time.Second
	// deviceDiscoveryLogInterval throttles the "still waiting" log line.
	deviceDiscoveryLogInterval = 10 * time.Second
)

// deviceDiscoveryOptions configures findAbsoluteDeviceByIDPath. Passing it
// in keeps the lookup directory and the timers injectable from tests
// without mutable package state.
type deviceDiscoveryOptions struct {
	byIDDir  string        // directory holding the stable by-id symlinks
	interval time.Duration // how often the symlink is re-checked
	timeout  time.Duration // total wait; 0 = check exactly once
}

func (d *Driver) nodePublishVolumeForFileSystem(req *csi.NodePublishVolumeRequest, mountOptions []string) error {
	source := req.StagingTargetPath
	target := req.TargetPath

	mnt := req.VolumeCapability.GetMount()
	mountOptions = append(mountOptions, mnt.MountFlags...)

	fsType := d.defaultFsType
	if mnt.FsType != "" {
		fsType = mnt.FsType
	}

	// Bind-mounting an unmounted directory "succeeds" and would hand the
	// pod the node's root filesystem under the globalmount path; require
	// the staged mount to actually be there first.
	stagingMounted, err := d.mounter.IsMounted(source)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to check staging path %q: %v", source, err)
	}
	if !stagingMounted {
		return status.Errorf(codes.FailedPrecondition,
			"staging target path %q is not mounted; NodeStageVolume must complete before NodePublishVolume", source)
	}

	mounted, err := d.mounter.IsMounted(target)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to check target %q: %v", target, err)
	}

	if !mounted {
		klog.V(2).Infof("mounting volume: source=%s, mode=%s, fsType=%s, mountOptions=%v",
			source, volumeModeFilesystem, fsType, mountOptions)
		if err := d.mounter.Mount(source, target, fsType, mountOptions...); err != nil {
			return status.Errorf(codes.Internal, "failed to mount %q to %q: %v", source, target, err)
		}
	} else {
		// The existing mount must be served by the same device that backs
		// the staging path and match the requested read-only mode. The
		// effective mode also counts an explicit "ro" mount flag (e.g.
		// from StorageClass mountOptions), not just req.Readonly.
		stagedDevice, err := d.mounter.GetDevicePathFromMount(source)
		if err != nil {
			return status.Errorf(codes.Internal, "failed to get the device backing staging path %q: %v", source, err)
		}
		if err := d.verifyMountedDevice(target, stagedDevice); err != nil {
			return err
		}
		if err := d.verifyMountReadOnly(target, req.Readonly || hasReadOnlyOption(mountOptions)); err != nil {
			return err
		}
		klog.V(2).Infof("volume is already mounted: source=%s, mode=%s", source, volumeModeFilesystem)
	}

	return nil
}

func (d *Driver) nodePublishVolumeForBlock(ctx context.Context, req *csi.NodePublishVolumeRequest, mountOptions []string) error {
	volumeID := req.GetVolumeId()
	if len(volumeID) == 0 {
		return status.Error(codes.InvalidArgument, "Volume ID not provided")
	}

	source, err := d.getVolumeSource(ctx, req.VolumeId, req.PublishContext, true)
	if err != nil {
		return deviceResolutionStatus(volumeID, err)
	}

	target := req.TargetPath

	mounted, err := d.mounter.IsMounted(target)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to check target %q: %v", target, err)
	}

	if !mounted {
		klog.V(2).Infof("mounting volume: source=%s, mode=%s, mountOptions=%v",
			source, volumeModeBlock, mountOptions)
		if err := d.mounter.Mount(source, target, "", mountOptions...); err != nil {
			return status.Errorf(codes.Internal, "%s", err.Error())
		}
	} else {
		// findmnt source strings of bind-mounted device nodes resolve to
		// devtmpfs subpaths, so device identity is compared by device
		// number (major/minor) instead of by path.
		same, err := d.mounter.SameDevice(source, target)
		if err != nil {
			return status.Errorf(codes.Internal, "failed to compare block devices %q and %q: %v", source, target, err)
		}
		if !same {
			return status.Errorf(codes.AlreadyExists,
				"target %q is already bound to a different block device than %q", target, source)
		}
		if err := d.verifyMountReadOnly(target, req.Readonly || hasReadOnlyOption(mountOptions)); err != nil {
			return err
		}
		klog.V(2).Infof("volume is already mounted: source=%s, mode=%s", source, volumeModeBlock)
	}

	return nil
}

// stripBindSubpath removes the "[subpath]" suffix findmnt appends to the
// source of bind mounts (e.g. "/dev/vdb[/]" -> "/dev/vdb").
func stripBindSubpath(source string) string {
	if i := strings.Index(source, "["); i >= 0 {
		return source[:i]
	}
	return source
}

// verifyMountedDevice checks that the existing mount at target is served by
// the expected device. CSI only allows idempotent success for a compatible
// existing mount; a foreign mount occupying the target must fail.
func (d *Driver) verifyMountedDevice(target, expectedDevice string) error {
	mountedDevice, err := d.mounter.GetDevicePathFromMount(target)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to inspect the existing mount at %q: %v", target, err)
	}
	if stripBindSubpath(mountedDevice) != stripBindSubpath(expectedDevice) {
		return status.Errorf(codes.AlreadyExists,
			"target %q is already mounted from %q, which does not match the requested device %q",
			target, mountedDevice, expectedDevice)
	}
	return nil
}

// hasReadOnlyOption reports whether the option list contains "ro".
func hasReadOnlyOption(options []string) bool {
	for _, opt := range options {
		if opt == "ro" {
			return true
		}
	}
	return false
}

// verifyMountReadOnly checks that the existing mount at target matches the
// requested read-only mode.
func (d *Driver) verifyMountReadOnly(target string, wantReadOnly bool) error {
	options, err := d.mounter.GetMountOptions(target)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to inspect mount options of %q: %v", target, err)
	}
	if isReadOnly := hasReadOnlyOption(options); isReadOnly != wantReadOnly {
		return status.Errorf(codes.AlreadyExists,
			"target %q is already mounted with read-only=%v, which does not match the requested read-only=%v",
			target, isReadOnly, wantReadOnly)
	}
	return nil
}

// findAbsoluteDeviceByIDPath follows the by-id symlink to the absolute
// device path, polling until the symlink appears when opts.timeout is set
// (the symlink is created by udev only after the hypervisor finishes the
// hot-plug, well after the attach API returns).
func findAbsoluteDeviceByIDPath(ctx context.Context, opts deviceDiscoveryOptions, prefix, volumeName string) (string, error) {
	path := filepath.Join(opts.byIDDir, fmt.Sprintf("%s%s", prefix, volumeName))

	// filepath.Join cleans "..", so a crafted volumeName could escape the
	// by-id directory and point the driver at an arbitrary device node;
	// refuse any candidate that is not directly inside it.
	if filepath.Dir(path) != opts.byIDDir {
		return "", fmt.Errorf("device path %q for volume %q escapes %s", path, volumeName, opts.byIDDir)
	}

	start := time.Now()
	lastLogged := start
	for {
		resolved, err := filepath.EvalSymlinks(path)
		if err == nil {
			if !strings.HasPrefix(resolved, "/dev/") {
				return "", fmt.Errorf("resolved symlink %q for %q was unexpected", resolved, path)
			}
			if waited := time.Since(start); waited >= opts.interval {
				klog.V(2).Infof("device %s resolved to %s after %v", path, resolved, waited.Truncate(time.Millisecond))
			}
			return resolved, nil
		}

		// Only "not there (yet)" is fixed by waiting; EvalSymlinks returns
		// lstat's *fs.PathError, which errors.Is unwraps to the errno.
		if !errors.Is(err, os.ErrNotExist) {
			return "", fmt.Errorf("could not resolve symlink %q: %v", path, err)
		}
		if opts.timeout <= 0 || time.Since(start) >= opts.timeout {
			return "", fmt.Errorf("could not resolve symlink %q: %v", path, err)
		}
		if time.Since(lastLogged) >= deviceDiscoveryLogInterval {
			klog.V(2).Infof("still waiting for device %s (%v elapsed)", path, time.Since(start).Truncate(time.Second))
			lastLogged = time.Now()
		}

		select {
		case <-time.After(opts.interval):
		case <-ctx.Done():
			return "", fmt.Errorf("could not resolve symlink %q before the request ended: %w", path, ctx.Err())
		}
	}
}
