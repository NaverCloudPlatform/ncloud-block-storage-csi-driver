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
	"strconv"
	"strings"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/kubernetes-csi/csi-lib-utils/protosanitizer"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/klog/v2"

	"github.com/NaverCloudPlatform/ncloud-block-storage-csi-driver/pkg/cloud"
	"github.com/NaverCloudPlatform/ncloud-block-storage-csi-driver/pkg/utils"
)

const (
	// Stamped on the volumes and snapshots the driver creates so an
	// operator looking at the console can tell them apart from resources
	// a person made by hand.
	createdByCSI = "Created by Ncloud Block Storage CSI driver"
)

// createRPCError maps a failed create call to its CSI code. The
// provisioner and the snapshotter (external-provisioner v6.1.0 checkError,
// external-snapshotter v8.4.0 isCSIFinalError) treat every code except
// Canceled, DeadlineExceeded, Unavailable, ResourceExhausted and Aborted
// as final: the request leaves their in-progress set, and if the PVC or
// VolumeSnapshot is deleted before the next retry, nothing ever comes back
// for the resource. So only an explicit rejection by the API -- HTTP 4xx,
// nothing was created -- may be final. A transport error, a timeout, a 5xx
// or an answer the SDK could not parse leaves the outcome unknown: the
// backend may well have created the resource, and Unavailable keeps the CO
// tracking the request until a retry finds it by name.
func createRPCError(err error, format string, args ...any) error {
	if cloud.IsRejectedByAPI(err) {
		return status.Errorf(codes.Internal, format, args...)
	}
	return status.Errorf(codes.Unavailable, format, args...)
}

// contextCode is the CSI code for a request that ended with its context:
// DeadlineExceeded for a deadline, Canceled otherwise.
func contextCode(err error) codes.Code {
	if errors.Is(err, context.DeadlineExceeded) {
		return codes.DeadlineExceeded
	}
	return codes.Canceled
}

func (d *Driver) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	klog.V(4).Infof("[CreateVolume] called with args %+v", protosanitizer.StripSecrets(req))

	cloudClient := d.cloud

	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "CreateVolume Name must be provided")
	}

	if len(req.VolumeCapabilities) == 0 {
		return nil, status.Error(codes.InvalidArgument, "CreateVolume Volume capabilities must be provided")
	}

	if violations := validateCapabilities(req.VolumeCapabilities); len(violations) > 0 {
		return nil, status.Error(codes.InvalidArgument, "invalid volume capabilities requested. Only SINGLE_NODE_WRITER is supported ('accessModes.ReadWriteOnce' on Kubernetes)")
	}

	volumeName := ncloudResourceName(req.Name)
	if volumeName != req.Name {
		klog.V(2).Infof("[CreateVolume] name %q exceeds the Ncloud %d-character limit, using %q",
			req.Name, maxVolumeNameLength, volumeName)
	}

	// Only snapshot content sources are supported. A clone request
	// (VolumeContentSource_Volume) must be rejected instead of being
	// silently fulfilled with an empty volume.
	var snapshotInstanceNo string
	if src := req.GetVolumeContentSource(); src != nil {
		switch source := src.GetType().(type) {
		case *csi.VolumeContentSource_Snapshot:
			snapshotInstanceNo = source.Snapshot.GetSnapshotId()
			if snapshotInstanceNo == "" {
				return nil, status.Error(codes.InvalidArgument, "snapshot content source must carry a snapshot ID")
			}
		case *csi.VolumeContentSource_Volume:
			return nil, status.Error(codes.InvalidArgument, "volume cloning is not supported; only snapshot content sources are supported")
		default:
			return nil, status.Errorf(codes.InvalidArgument, "unsupported volume content source type %T", source)
		}
	}

	if err := validateVolumeParameters(req.Parameters); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}

	volumeType, err := volumeTypeParameter(req.Parameters)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}

	if err := validateVolumeType(volumeType); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}

	if err := validateVolumeName(volumeName); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}

	size, err := utils.ExtractStorage(req.CapacityRange)
	if err != nil {
		return nil, status.Errorf(codes.OutOfRange, "invalid capacity range: %v", err)
	}

	// A limit-only range means "any size up to the limit", so the limit is
	// rounded DOWN to the allocation unit; rounding up would overshoot every
	// non-aligned limit and reject a satisfiable request. Ranges that set
	// required_bytes keep rounding up (the minimum must be met).
	if capRange := req.GetCapacityRange(); capRange != nil && capRange.GetRequiredBytes() == 0 {
		size = utils.RoundDownBytes(size)
		if size == 0 {
			return nil, status.Errorf(codes.OutOfRange, "limit %v is below the %v allocation unit",
				utils.FormatBytes(capRange.GetLimitBytes()), utils.FormatBytes(utils.NcloudVolumeSizeUnitInBytes))
		}
	} else {
		size = utils.RoundUpBytes(size)
	}
	if req.CapacityRange != nil && req.CapacityRange.GetLimitBytes() > 0 && size > req.CapacityRange.GetLimitBytes() {
		return nil, status.Errorf(codes.OutOfRange, "requested size rounded up to %v exceeds limit %v",
			utils.FormatBytes(size), utils.FormatBytes(req.CapacityRange.GetLimitBytes()))
	}

	// The type was validated above, so the only failure left here is a
	// size outside the type's range -- OUT_OF_RANGE in the spec's
	// CreateVolume table, and what ControllerExpandVolume already returns
	// for the same check.
	if err := validateVolumeSizeForType(volumeType, size); err != nil {
		return nil, status.Errorf(codes.OutOfRange, "%v", err)
	}

	klog.V(2).Infof("[CreateVolume] volumeName=%s, size=%d GiB, capabilities=%v", volumeName, size/utils.GiB, req.VolumeCapabilities)

	selectedZoneCode, err := selectPlacementZone(req.GetAccessibilityRequirements())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid accessibility requirements: %v", err)
	}

	// The lookup-then-create pair must not interleave with another request
	// for the same name (see controllerInFlight); the key is the name the
	// API enforces uniqueness on.
	if !d.controllerInFlight.acquire(volumeName) {
		return nil, status.Errorf(codes.Aborted, "another CreateVolume for %q is already in progress", volumeName)
	}
	defer d.controllerInFlight.release(volumeName)

	existingVol, err := cloudClient.GetVolumeByName(ctx, volumeName)
	if err != nil && !errors.Is(err, cloud.ErrVolumeNotFound) {
		// A failed read says nothing about whether the volume exists -- an
		// earlier attempt may have created it -- so it must not be a final
		// error: see createRPCError.
		return nil, status.Errorf(codes.Unavailable, "failed to get volume by name %q: %v", volumeName, err)
	}

	if existingVol != nil {
		return d.existingVolumeResponse(ctx, req, existingVol, volumeName, volumeType, snapshotInstanceNo)
	}

	volOpts := &cloud.VolumeOptions{
		Name:        volumeName,
		SizeGiB:     int32(size / utils.GiB),
		VolumeType:  volumeType,
		ZoneCode:    selectedZoneCode,
		Description: createdByCSI,
	}

	if snapshotInstanceNo != "" {
		snap, err := cloudClient.GetSnapshotByID(ctx, snapshotInstanceNo)
		if err != nil {
			if errors.Is(err, cloud.ErrSnapshotNotFound) {
				return nil, status.Errorf(codes.NotFound, "source snapshot %q not found", snapshotInstanceNo)
			}
			// A failed read is not a final error (see createRPCError): an
			// earlier attempt may have created the volume already.
			return nil, status.Errorf(codes.Unavailable, "failed to get source snapshot %q: %v", snapshotInstanceNo, err)
		}
		// A restored volume cannot be smaller than its source snapshot.
		if snap.SizeBytes > size {
			return nil, status.Errorf(codes.OutOfRange,
				"requested size %v is smaller than the source snapshot size %v",
				utils.FormatBytes(size), utils.FormatBytes(snap.SizeBytes))
		}
		klog.V(2).Infof("[CreateVolume] using snapshot(%s) as volume(%s) source", snapshotInstanceNo, volumeName)
		volOpts.SnapshotID = snapshotInstanceNo
	}

	klog.V(2).Infof("[CreateVolume] creating volume: name=%s, sizeGiB=%d", volOpts.Name, volOpts.SizeGiB)

	newVol, err := cloudClient.CreateVolume(ctx, volOpts)
	if err != nil {
		klog.Errorf("[CreateVolume] failed to create volume(%s): %v", volumeName, err)
		if errors.Is(err, cloud.ErrNameInUse) {
			// Created by a request this one did not see -- a retry whose
			// first attempt is still running, or one whose answer was
			// lost. The API refuses the duplicate (measured: 1103047), so
			// the volume behind the name is the one to return.
			existingVol, lookupErr := cloudClient.GetVolumeByName(ctx, volumeName)
			if lookupErr == nil {
				return d.existingVolumeResponse(ctx, req, existingVol, volumeName, volumeType, snapshotInstanceNo)
			}
			return nil, status.Errorf(codes.Aborted, "volume name %q is in use by a concurrent request and the volume is not readable yet: %v", volumeName, lookupErr)
		}
		return nil, createRPCError(err, "failed to create volume %q: %v", volumeName, err)
	}

	// The create response already describes the volume -- ID, zone, type,
	// size (measured on the live gateway) -- and the re-read only refreshes
	// it. The volume exists, so a failed re-read must not fail the request
	// (see createRPCError): the create response stands in.
	createdVol := newVol
	if refreshedVol, err := cloudClient.GetVolumeByID(ctx, newVol.ID); err == nil {
		createdVol = refreshedVol
	} else {
		klog.Warningf("[CreateVolume] volume %s was created but could not be re-read, answering from the create response: %v", newVol.ID, err)
	}

	newTopology, err := d.topologyForZone(createdVol.ZoneCode)
	if err != nil {
		// Final on purpose, although the volume exists: the zone was taken
		// from the driver's own table, so a volume reported outside it is
		// a driver inconsistency that no retry resolves. A non-final code
		// would have the provisioner retry forever -- after the PVC is
		// deleted too, since it keeps a claim in progress until a create
		// answers -- while this answer names the volume for the operator.
		return nil, status.Errorf(codes.Internal, "volume %q was created in a zone this driver cannot advertise: %v", createdVol.ID, err)
	}
	resp := &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:      createdVol.ID,
			CapacityBytes: size,
			VolumeContext: map[string]string{
				"volumeName":             volumeName,
				"storageType":            "blockStorage",
				"blockStorageVolumeType": createdVol.VolumeTypeCode,
			},
			AccessibleTopology: newTopology,
		},
	}

	if snapshotInstanceNo != "" {
		resp.Volume.ContentSource = &csi.VolumeContentSource{
			Type: &csi.VolumeContentSource_Snapshot{
				Snapshot: &csi.VolumeContentSource_SnapshotSource{
					SnapshotId: snapshotInstanceNo,
				},
			},
		}
	}

	klog.V(2).Infof("[CreateVolume] succeeded: volume=%s", volumeName)
	return resp, nil
}

// existingVolumeResponse answers a CreateVolume whose name already names a
// volume -- found by the lookup, or reported by the API when the create
// collided with it. A retry of the same request must return the same
// volume; a different request that merely reuses the name must fail.
func (d *Driver) existingVolumeResponse(ctx context.Context, req *csi.CreateVolumeRequest, existingVol *cloud.Volume, volumeName, volumeType, snapshotInstanceNo string) (*csi.CreateVolumeResponse, error) {
	existingTopology, err := d.topologyForZone(existingVol.ZoneCode)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "volume %q lives in a zone this driver cannot advertise: %v", existingVol.ID, err)
	}
	klog.V(2).Infof("[CreateVolume] already created: volume=%s(%s)", existingVol.Name, existingVol.ID)

	// A retry of the same request must return the same volume; a
	// different request that merely reuses the name must fail.
	// Capacity compatibility is judged against the raw requested range
	// (not the derived creation size, which equals LimitBytes for
	// limit-only requests and would wrongly reject valid volumes).
	if capRange := req.GetCapacityRange(); capRange != nil {
		if existingVol.SizeBytes < capRange.GetRequiredBytes() {
			return nil, status.Errorf(codes.AlreadyExists,
				"volume %q already exists with size %d bytes, which is less than required %d bytes",
				volumeName, existingVol.SizeBytes, capRange.GetRequiredBytes())
		}
		if limit := capRange.GetLimitBytes(); limit > 0 && existingVol.SizeBytes > limit {
			return nil, status.Errorf(codes.AlreadyExists,
				"volume %q already exists with size %d bytes, which exceeds the requested limit of %d bytes",
				volumeName, existingVol.SizeBytes, limit)
		}
	}
	if existingVol.VolumeTypeCode != volumeType {
		return nil, status.Errorf(codes.AlreadyExists,
			"volume %q already exists with type %s, which does not match requested type %s",
			volumeName, existingVol.VolumeTypeCode, volumeType)
	}
	// An existing volume is compatible when it lies inside the hard
	// Requisite set; a Preferred-only mismatch must not fail the retry.
	if !isZoneCompatible(req.GetAccessibilityRequirements(), existingVol.ZoneCode) {
		return nil, status.Errorf(codes.AlreadyExists,
			"volume %q already exists in zone %s, which is outside the requisite topology",
			volumeName, existingVol.ZoneCode)
	}

	resp := &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:      existingVol.ID,
			CapacityBytes: existingVol.SizeBytes,
			VolumeContext: map[string]string{
				"volumeName":             volumeName,
				"storageType":            "blockStorage",
				"blockStorageVolumeType": existingVol.VolumeTypeCode,
			},
			AccessibleTopology: existingTopology,
		},
	}

	if snapshotInstanceNo != "" {
		// The retry claims a snapshot lineage; echoing it without a
		// lookup would fabricate a content source that the fresh
		// create path validates. Hold both paths to the same
		// contract.
		if _, err := d.cloud.GetSnapshotByID(ctx, snapshotInstanceNo); err != nil {
			if errors.Is(err, cloud.ErrSnapshotNotFound) {
				return nil, status.Errorf(codes.NotFound, "source snapshot %q not found", snapshotInstanceNo)
			}
			// A failed read is not a final error: see createRPCError.
			return nil, status.Errorf(codes.Unavailable, "failed to get source snapshot %q: %v", snapshotInstanceNo, err)
		}
		resp.Volume.ContentSource = &csi.VolumeContentSource{
			Type: &csi.VolumeContentSource_Snapshot{
				Snapshot: &csi.VolumeContentSource_SnapshotSource{
					SnapshotId: snapshotInstanceNo,
				},
			},
		}
	}

	return resp, nil
}

func (d *Driver) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	klog.V(4).Infof("[DeleteVolume] called with args %+v", protosanitizer.StripSecrets(req))

	cloudClient := d.cloud

	if req.VolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "DeleteVolume Volume ID must be provided")
	}
	// An ID that cannot be an Ncloud instance number cannot name an
	// existing volume; short-circuit instead of letting the list API
	// reject the filter as Internal on every retry.
	if !ncloudVolumeIDPattern.MatchString(req.VolumeId) {
		klog.V(2).Infof("[DeleteVolume] volume ID %q is not an instance number, treating as already deleted", req.VolumeId)
		return &csi.DeleteVolumeResponse{}, nil
	}

	if !d.controllerInFlight.acquire(req.VolumeId) {
		return nil, status.Errorf(codes.Aborted, "another operation on volume %q is already in progress", req.VolumeId)
	}
	defer d.controllerInFlight.release(req.VolumeId)

	klog.V(2).Infof("[DeleteVolume] volumeId=%s", req.VolumeId)

	if _, err := cloudClient.GetVolumeByID(ctx, req.VolumeId); err != nil {
		if errors.Is(err, cloud.ErrVolumeNotFound) {
			return &csi.DeleteVolumeResponse{}, nil
		}
		return nil, status.Errorf(codes.Internal, "failed to get volume %q: %v", req.VolumeId, err)
	}

	if _, err := cloudClient.EnsureDetachVolume(ctx, req.VolumeId); err != nil {
		if errors.Is(err, cloud.ErrVolumeNotFound) {
			return &csi.DeleteVolumeResponse{}, nil
		}
		return nil, status.Errorf(codes.Internal, "failed to detach volume %q: %v", req.VolumeId, err)
	}
	klog.V(2).Infof("[DeleteVolume] volume(%s) is detached", req.VolumeId)

	if err := cloudClient.WaitForVolumeStatus(ctx, req.VolumeId, []string{cloud.StatusCreated}); err != nil {
		if errors.Is(err, cloud.ErrVolumeNotFound) {
			return &csi.DeleteVolumeResponse{}, nil
		}
		return nil, status.Errorf(codes.Internal, "failed waiting for volume %q status: %v", req.VolumeId, err)
	}

	if err := cloudClient.DeleteVolume(ctx, req.VolumeId); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to delete volume %q: %v", req.VolumeId, err)
	}

	klog.V(2).Infof("[DeleteVolume] succeeded: volumeId=%s", req.VolumeId)
	return &csi.DeleteVolumeResponse{}, nil
}

func (d *Driver) ControllerPublishVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	klog.V(4).Infof("[ControllerPublishVolume] called with args %+v", protosanitizer.StripSecrets(req))

	cloudClient := d.cloud

	if req.VolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "ControllerPublishVolume Volume ID must be provided")
	}
	if req.NodeId == "" {
		return nil, status.Error(codes.InvalidArgument, "ControllerPublishVolume Node ID must be provided")
	}
	if req.VolumeCapability == nil {
		return nil, status.Error(codes.InvalidArgument, "ControllerPublishVolume Volume capability must be provided")
	}
	if violations := validateCapabilities([]*csi.VolumeCapability{req.VolumeCapability}); len(violations) > 0 {
		return nil, status.Errorf(codes.InvalidArgument, "unsupported volume capability: %s", strings.Join(violations, "; "))
	}
	if req.Readonly {
		return nil, status.Error(codes.InvalidArgument, "read-only volumes are not supported")
	}
	if !ncloudVolumeIDPattern.MatchString(req.VolumeId) {
		return nil, status.Errorf(codes.NotFound, "volume %q not found: not a valid Ncloud block storage instance number", req.VolumeId)
	}

	if !d.controllerInFlight.acquire(req.VolumeId) {
		return nil, status.Errorf(codes.Aborted, "another operation on volume %q is already in progress", req.VolumeId)
	}
	defer d.controllerInFlight.release(req.VolumeId)

	klog.V(2).Infof("[ControllerPublishVolume] volume=%s, node=%s", req.VolumeId, req.NodeId)

	vol, err := cloudClient.GetVolumeByID(ctx, req.VolumeId)
	if err != nil {
		if errors.Is(err, cloud.ErrVolumeNotFound) {
			return nil, status.Errorf(codes.NotFound, "volume %q not found", req.VolumeId)
		}
		return nil, status.Errorf(codes.Internal, "failed to get volume %q: %v", req.VolumeId, err)
	}

	if vol.ServerInstanceNo == req.NodeId {
		// The request is already satisfied as long as the device is there
		// and is staying there. Following the same principle as the
		// unpublish guard below, the operation code only matters when it
		// changes the attachment: a detach in flight keeps the volume in
		// StatusAttached while the device is on its way out, so that one
		// is not "already attached". Operations that leave the attachment
		// alone (a snapshot, a resize) must not turn a satisfied request
		// into a retry the CO can only spin on.
		if strings.ToUpper(vol.StatusCode) == cloud.StatusAttached &&
			strings.ToUpper(vol.OperationCode) != cloud.OpDetach {
			klog.V(2).Infof("[ControllerPublishVolume] volume(%s) is already attached to node(%s)", vol.ID, req.NodeId)
			return &csi.ControllerPublishVolumeResponse{
				PublishContext: map[string]string{
					d.publishInfoVolumeName: vol.Name,
					"serverInstanceNo":      vol.ServerInstanceNo,
				},
			}, nil
		}
		return nil, status.Errorf(codes.Aborted,
			"volume %q is on node %q but its attachment is not usable yet (status=%s, operation=%s)",
			req.VolumeId, req.NodeId, vol.StatusCode, vol.OperationCode)
	}

	klog.V(2).Infof("[ControllerPublishVolume] volume(%s) status: StatusCode=%s, ServerInstanceNo=%s",
		vol.ID, vol.StatusCode, vol.ServerInstanceNo)

	// A volume held by another server is not ours to take. The Ncloud API
	// refuses to move an attached volume, and the CSI answer to that is to
	// report the precondition rather than detach behind the other node's
	// back: the CO releases an attachment with ControllerUnpublishVolume,
	// and pulling the device out from under a node that still has it
	// mounted corrupts that filesystem.
	//
	// The field is what "held" means: measured on the live API, a detached
	// volume reports it empty (status CREAT, no operation), and it is only
	// set while the volume is attached to — or being attached to — a
	// server. The status is in the message so a backend that ever left a
	// stale value behind would be diagnosable from the error alone.
	if vol.ServerInstanceNo != "" {
		return nil, status.Errorf(codes.FailedPrecondition,
			"volume %q is held by node %q (status=%s, operation=%s); it must be released from that node first (the CO does this with ControllerUnpublishVolume)",
			req.VolumeId, vol.ServerInstanceNo, vol.StatusCode, vol.OperationCode)
	}

	// CreateVolume returns as soon as the create API answers, so a volume
	// can still be building when the first publish arrives -- a restore
	// from a snapshot takes the longest. The API refuses an attach then
	// (measured: HTTP 400, returnCode 1103026), and the driver would spend
	// that refusal on an Internal error plus the node lookup its error
	// classification needs, leaving the retry to the attacher's backoff.
	// Waiting for the volume to finish is what the original driver did
	// here, and it is the cheaper of the two.
	if err = cloudClient.WaitForVolumeStatus(ctx, req.VolumeId, []string{cloud.StatusCreated}); err != nil {
		if errors.Is(err, cloud.ErrVolumeNotFound) {
			return nil, status.Errorf(codes.NotFound, "volume %q disappeared before attaching: %v", req.VolumeId, err)
		}
		return nil, status.Errorf(codes.Aborted, "volume %q is not ready to attach yet: %v", req.VolumeId, err)
	}

	klog.V(2).Infof("[ControllerPublishVolume] attaching volume(%s) to node(%s)", vol.ID, req.NodeId)
	if err = cloudClient.AttachVolume(ctx, req.VolumeId, req.NodeId); err != nil {
		// The call waited its turn behind other attaches and the RPC's
		// deadline passed before it was issued: AttachVolume then returns
		// the context's own error, unwrapped, without calling the API.
		// There is nothing to classify, and the node lookup below would
		// spend two more API calls after the deadline -- on the slow-API
		// day that made the queue long in the first place. Only that exact
		// error qualifies: an attach that did go out and failed after the
		// deadline passed (the API's own rejection, or the HTTP client's
		// timeout, which also unwraps to DeadlineExceeded) is classified
		// by its own error below.
		if ctxErr := ctx.Err(); ctxErr != nil && err == ctxErr {
			return nil, status.Errorf(contextCode(ctxErr), "attach of volume %q to node %q was not issued before the deadline: %v", req.VolumeId, req.NodeId, ctxErr)
		}
		if errors.Is(err, cloud.ErrAttachmentLimitReached) {
			return nil, status.Errorf(codes.ResourceExhausted, "node %q cannot take another volume attachment: %v", req.NodeId, err)
		}
		// CSI requires NOT_FOUND when the target node does not exist. The
		// attach error alone cannot be classified, so only a failed attach
		// pays for the extra lookup.
		if _, siErr := cloudClient.GetServerInstance(ctx, req.NodeId); errors.Is(siErr, cloud.ErrServerInstanceNotFound) {
			return nil, status.Errorf(codes.NotFound, "node %q not found", req.NodeId)
		}
		return nil, status.Errorf(codes.Internal, "failed to attach volume %q to node %q: %v", req.VolumeId, req.NodeId, err)
	}

	if err = cloudClient.WaitForVolumeStatus(ctx, req.VolumeId, []string{cloud.StatusAttached}); err != nil {
		if errors.Is(err, cloud.ErrVolumeNotFound) {
			return nil, status.Errorf(codes.NotFound, "volume %q disappeared while attaching: %v", req.VolumeId, err)
		}
		return nil, status.Errorf(codes.Internal, "volume %s did not reach attached status: %v", req.VolumeId, err)
	}

	attachedVol, err := cloudClient.GetVolumeByID(ctx, req.VolumeId)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get volume %q after attach: %v", req.VolumeId, err)
	}

	if attachedVol.ServerInstanceNo != req.NodeId {
		klog.Warningf("[ControllerPublishVolume] failed to attach volume to appropriate instance: ServerInstanceNo=%s", attachedVol.ServerInstanceNo)
		return nil, status.Error(codes.Internal, "failed to mount storage to the appropriate instance")
	}

	klog.V(2).Infof("[ControllerPublishVolume] succeeded: volume=%s, node=%s", vol.ID, req.NodeId)

	return &csi.ControllerPublishVolumeResponse{
		PublishContext: map[string]string{
			d.publishInfoVolumeName: vol.Name,
			"serverInstanceNo":      attachedVol.ServerInstanceNo,
		},
	}, nil
}

func (d *Driver) ControllerUnpublishVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	klog.V(4).Infof("[ControllerUnpublishVolume] called with args %+v", protosanitizer.StripSecrets(req))

	cloudClient := d.cloud

	if req.VolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "ControllerUnpublishVolume Volume ID must be provided")
	}
	if !ncloudVolumeIDPattern.MatchString(req.VolumeId) {
		klog.V(2).Infof("[ControllerUnpublishVolume] volume ID %q is not an instance number, nothing to unpublish", req.VolumeId)
		return &csi.ControllerUnpublishVolumeResponse{}, nil
	}

	if !d.controllerInFlight.acquire(req.VolumeId) {
		return nil, status.Errorf(codes.Aborted, "another operation on volume %q is already in progress", req.VolumeId)
	}
	defer d.controllerInFlight.release(req.VolumeId)

	klog.V(2).Infof("[ControllerUnpublishVolume] volume=%s", req.VolumeId)

	vol, err := cloudClient.GetVolumeByID(ctx, req.VolumeId)
	if err != nil {
		if errors.Is(err, cloud.ErrVolumeNotFound) {
			klog.V(2).Infof("[ControllerUnpublishVolume] volume(%s) is already deleted", req.VolumeId)
			return &csi.ControllerUnpublishVolumeResponse{}, nil
		}
		return nil, status.Errorf(codes.Internal, "failed to get volume %q: %v", req.VolumeId, err)
	}

	// CSI allows node_id to be empty: it then means "unpublish from
	// whatever node the volume is currently published to". Only an
	// explicit, non-matching node ID is an idempotent no-op — and that
	// no-op must not depend on the volume's operation state, so it is
	// checked before the in-progress guard.
	if req.NodeId != "" && req.NodeId != vol.ServerInstanceNo {
		klog.V(2).Infof("[ControllerUnpublishVolume] volume(%s) is already detached from node(%s), attached to node(%s)", req.VolumeId, req.NodeId, vol.ServerInstanceNo)
		return &csi.ControllerUnpublishVolumeResponse{}, nil
	}

	// An attach in flight has not finished claiming the volume, and the
	// status does not say so: measured on the live API, the volume reads
	// StatusCreated with operation ATTAC and serverInstanceNo already set
	// for about 27 seconds. The check below would call that "not attached"
	// and report success, which is the one answer that cannot be taken
	// back -- external-attacher drops the VolumeAttachment finalizer, the
	// attach then completes, and nothing is left that would ever detach
	// it. Aborted instead, so the attacher retries until the attach
	// settles and a real detach can run.
	if strings.ToUpper(vol.OperationCode) == cloud.OpAttach {
		klog.V(2).Infof("[ControllerUnpublishVolume] volume(%s) is still being attached to node(%s), cannot detach yet", req.VolumeId, vol.ServerInstanceNo)
		return nil, status.Errorf(codes.Aborted,
			"volume %q is still being attached to node %q (status=%s, operation=%s); retry once the attach settles",
			req.VolumeId, vol.ServerInstanceNo, vol.StatusCode, vol.OperationCode)
	}

	// An already-detached volume satisfies unpublish as-is, regardless of
	// any other in-flight operation (e.g. a snapshot): the operation guard
	// only matters when a detach will actually be issued. Checking it
	// first would turn an already-satisfied request into a spurious
	// Aborted.
	if strings.ToUpper(vol.StatusCode) != cloud.StatusAttached {
		klog.V(2).Infof("[ControllerUnpublishVolume] volume(%s) is not attached (status=%s), nothing to do", req.VolumeId, vol.StatusCode)
		return &csi.ControllerUnpublishVolumeResponse{}, nil
	}

	if strings.ToUpper(vol.OperationCode) != cloud.OpNormal {
		klog.Errorf("[ControllerUnpublishVolume] volume(%s) has operation in progress, can't detach", req.VolumeId)
		return nil, status.Errorf(codes.Aborted, "can't detach volume(%s)", req.VolumeId)
	}

	if _, err = cloudClient.EnsureDetachVolume(ctx, req.VolumeId); err != nil && !errors.Is(err, cloud.ErrVolumeNotFound) {
		return nil, status.Errorf(codes.Internal, "failed to detach volume %q: %v", req.VolumeId, err)
	}

	klog.V(2).Infof("[ControllerUnpublishVolume] succeeded: volume=%s", req.VolumeId)
	return &csi.ControllerUnpublishVolumeResponse{}, nil
}

func (d *Driver) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	klog.V(4).Infof("[ValidateVolumeCapabilities] called with args %+v", protosanitizer.StripSecrets(req))

	cloudClient := d.cloud

	if req.VolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "ValidateVolumeCapabilities Volume ID must be provided")
	}
	if len(req.VolumeCapabilities) == 0 {
		return nil, status.Error(codes.InvalidArgument, "ValidateVolumeCapabilities Volume Capabilities must be provided")
	}
	if !ncloudVolumeIDPattern.MatchString(req.VolumeId) {
		return nil, status.Errorf(codes.NotFound, "volume %q not found: not a valid Ncloud block storage instance number", req.VolumeId)
	}

	if _, err := cloudClient.GetVolumeByID(ctx, req.VolumeId); err != nil {
		if errors.Is(err, cloud.ErrVolumeNotFound) {
			return nil, status.Errorf(codes.NotFound, "volume %q not found", req.VolumeId)
		}
		return nil, status.Errorf(codes.Internal, "failed to get volume %q: %v", req.VolumeId, err)
	}

	// Confirm only capabilities the driver actually supports; echo the
	// requested capabilities back instead of a fixed answer.
	if violations := validateCapabilities(req.VolumeCapabilities); len(violations) > 0 {
		msg := fmt.Sprintf("unsupported volume capabilities: %s", strings.Join(violations, "; "))
		klog.V(2).Infof("[ValidateVolumeCapabilities] %s", msg)
		return &csi.ValidateVolumeCapabilitiesResponse{Message: msg}, nil
	}

	resp := &csi.ValidateVolumeCapabilitiesResponse{
		Confirmed: &csi.ValidateVolumeCapabilitiesResponse_Confirmed{
			VolumeCapabilities: req.VolumeCapabilities,
		},
	}

	klog.V(2).Infof("[ValidateVolumeCapabilities] supported: %v", resp.Confirmed)
	return resp, nil
}

func (d *Driver) ControllerExpandVolume(ctx context.Context, req *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error) {
	klog.V(4).Infof("[ControllerExpandVolume] called with args %+v", protosanitizer.StripSecrets(req))

	cloudClient := d.cloud

	volumeID := req.GetVolumeId()
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID not provided")
	}
	if !ncloudVolumeIDPattern.MatchString(volumeID) {
		return nil, status.Errorf(codes.NotFound, "volume %q not found: not a valid Ncloud block storage instance number", volumeID)
	}

	capRange := req.GetCapacityRange()
	if capRange == nil {
		return nil, status.Error(codes.InvalidArgument, "Capacity range not provided")
	}

	newSize, err := utils.ExtractStorage(capRange)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid capacity range: %v", err)
	}

	// Same rounding rule as CreateVolume: a limit-only range is a ceiling
	// (round down), a range with required_bytes is a minimum (round up).
	if capRange.GetRequiredBytes() == 0 {
		newSize = utils.RoundDownBytes(newSize)
		if newSize == 0 {
			return nil, status.Errorf(codes.OutOfRange, "limit %v is below the %v allocation unit",
				utils.FormatBytes(capRange.GetLimitBytes()), utils.FormatBytes(utils.NcloudVolumeSizeUnitInBytes))
		}
	} else {
		newSize = utils.RoundUpBytes(newSize)
	}
	if capRange.GetLimitBytes() > 0 && newSize > capRange.GetLimitBytes() {
		return nil, status.Errorf(codes.OutOfRange, "requested size rounded up to %v exceeds limit %v",
			utils.FormatBytes(newSize), utils.FormatBytes(capRange.GetLimitBytes()))
	}

	nodeExpansionRequired := true
	if cap := req.GetVolumeCapability(); cap != nil {
		if violations := validateCapabilities([]*csi.VolumeCapability{cap}); len(violations) > 0 {
			return nil, status.Errorf(codes.InvalidArgument, "unsupported volume capability: %s", strings.Join(violations, "; "))
		}
		if _, ok := cap.GetAccessType().(*csi.VolumeCapability_Block); ok {
			klog.V(2).Info("node expansion is not required for block volumes")
			nodeExpansionRequired = false
		}
	}

	if !d.controllerInFlight.acquire(volumeID) {
		return nil, status.Errorf(codes.Aborted, "another operation on volume %q is already in progress", volumeID)
	}
	defer d.controllerInFlight.release(volumeID)

	vol, err := cloudClient.GetVolumeByID(ctx, volumeID)
	if err != nil {
		if errors.Is(err, cloud.ErrVolumeNotFound) {
			return nil, status.Errorf(codes.NotFound, "volume %q not found", volumeID)
		}
		return nil, status.Errorf(codes.Internal, "failed to get volume %q: %v", volumeID, err)
	}

	// A type outside the supported set means the volume is not one this
	// driver can manage — in practice a first-generation XEN volume, which
	// the API reports as type SSD/HDD.
	if err := validateVolumeType(vol.VolumeTypeCode); err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "volume %q cannot be resized by this driver: %v", volumeID, err)
	}

	// A volume that already exceeds the requested limit cannot satisfy
	// the request at all.
	if limit := capRange.GetLimitBytes(); limit > 0 && vol.SizeBytes > limit {
		return nil, status.Errorf(codes.OutOfRange,
			"volume %q already has size %v, which exceeds the requested limit %v",
			volumeID, utils.FormatBytes(vol.SizeBytes), utils.FormatBytes(limit))
	}

	// required_bytes is a minimum: a volume already at or above the
	// requested size satisfies the request without a backend resize.
	// This also keeps shrink attempts away from the backend.
	if vol.SizeBytes >= newSize {
		// The reported size can reflect an in-flight resize before the
		// backend operation completes (a retry of an interrupted expand
		// lands here); success must wait for the volume to settle, under
		// the same contract as the freshly issued resize below.
		if strings.ToUpper(vol.OperationCode) != cloud.OpNormal {
			if err := cloudClient.WaitForVolumeResize(ctx, volumeID, newSize); err != nil {
				if errors.Is(err, cloud.ErrVolumeNotFound) {
					return nil, status.Errorf(codes.NotFound, "volume %q not found while waiting for resize", volumeID)
				}
				return nil, status.Errorf(codes.Aborted, "resize of volume %q is still in progress: %v", volumeID, err)
			}
		}
		klog.V(2).Infof("[ControllerExpandVolume] volume=%s already has size %d >= requested %d, nothing to do",
			volumeID, vol.SizeBytes, newSize)
		return &csi.ControllerExpandVolumeResponse{
			CapacityBytes:         vol.SizeBytes,
			NodeExpansionRequired: nodeExpansionRequired,
		}, nil
	}

	if err := validateVolumeSizeForType(vol.VolumeTypeCode, newSize); err != nil {
		return nil, status.Errorf(codes.OutOfRange, "%v", err)
	}

	if strings.ToUpper(vol.OperationCode) != cloud.OpNormal {
		return nil, status.Errorf(codes.Aborted, "volume %q has operation %q in progress, can't resize", volumeID, vol.OperationCode)
	}

	klog.V(2).Infof("[ControllerExpandVolume] volume=%s, newSize=%d", volumeID, newSize)

	newSizeGiB := int32(newSize / utils.GiB)
	if err := cloudClient.ResizeVolume(ctx, volumeID, newSizeGiB); err != nil {
		return nil, status.Errorf(codes.Internal, "Could not resize volume(%s): %v", volumeID, err)
	}

	// ChangeBlockStorageInstance is asynchronous: it only acknowledges the
	// request. Returning success here would let the CO record the new size
	// (and set FileSystemResizePending) while the backend resize may still
	// fail, leaving the PVC stuck with no self-recovery.
	if err := cloudClient.WaitForVolumeResize(ctx, volumeID, newSize); err != nil {
		if errors.Is(err, cloud.ErrVolumeNotFound) {
			return nil, status.Errorf(codes.NotFound, "volume %q not found while waiting for resize", volumeID)
		}
		return nil, status.Errorf(codes.Aborted, "resize of volume %q was accepted but has not completed: %v", volumeID, err)
	}

	klog.V(2).Infof("[ControllerExpandVolume] succeeded: volume=%s, size=%d", volumeID, newSize)
	return &csi.ControllerExpandVolumeResponse{
		CapacityBytes:         newSize,
		NodeExpansionRequired: nodeExpansionRequired,
	}, nil
}

func (d *Driver) ListVolumes(_ context.Context, req *csi.ListVolumesRequest) (*csi.ListVolumesResponse, error) {
	klog.V(4).Infof("[ListVolumes] called with args %+v", protosanitizer.StripSecrets(req))
	// This optional RPC is not implemented yet. The LIST_VOLUMES capability
	// is not advertised, so CO components are not expected to call it.
	return nil, status.Error(codes.Unimplemented, "ListVolumes is not supported")
}

func (d *Driver) GetCapacity(_ context.Context, req *csi.GetCapacityRequest) (*csi.GetCapacityResponse, error) {
	klog.V(4).Infof("[GetCapacity] called with args %+v", protosanitizer.StripSecrets(req))
	return nil, status.Error(codes.Unimplemented, "")
}

func (d *Driver) ControllerGetCapabilities(_ context.Context, req *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	klog.V(4).Infof("[ControllerGetCapabilities] called with args %+v", protosanitizer.StripSecrets(req))

	newCap := func(cap csi.ControllerServiceCapability_RPC_Type) *csi.ControllerServiceCapability {
		return &csi.ControllerServiceCapability{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: cap,
				},
			},
		}
	}

	var caps []*csi.ControllerServiceCapability
	for _, cap := range []csi.ControllerServiceCapability_RPC_Type{
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
		csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME,
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT,
		csi.ControllerServiceCapability_RPC_LIST_SNAPSHOTS,
		csi.ControllerServiceCapability_RPC_EXPAND_VOLUME,
	} {
		caps = append(caps, newCap(cap))
	}

	return &csi.ControllerGetCapabilitiesResponse{Capabilities: caps}, nil
}

func (d *Driver) CreateSnapshot(ctx context.Context, req *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error) {
	klog.V(4).Infof("[CreateSnapshot] called with args %+v", protosanitizer.StripSecrets(req))

	cloudClient := d.cloud

	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "CreateSnapshot Name must be provided")
	}
	if req.GetSourceVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "CreateSnapshot Source Volume ID must be provided")
	}
	if !ncloudVolumeIDPattern.MatchString(req.GetSourceVolumeId()) {
		return nil, status.Errorf(codes.NotFound, "source volume %q not found: not a valid Ncloud block storage instance number", req.GetSourceVolumeId())
	}

	snapshotName := ncloudResourceName(req.GetName())
	if snapshotName != req.GetName() {
		klog.V(2).Infof("[CreateSnapshot] name %q exceeds the Ncloud %d-character limit, using %q",
			req.GetName(), maxVolumeNameLength, snapshotName)
	}
	sourceVolumeId := req.GetSourceVolumeId()
	klog.V(2).Infof("[CreateSnapshot] snapshotName=%s, sourceVolume=%s", snapshotName, sourceVolumeId)

	// Same lookup-then-create pair as CreateVolume, keyed by the name the
	// API enforces uniqueness on.
	if !d.controllerInFlight.acquire(snapshotName) {
		return nil, status.Errorf(codes.Aborted, "another CreateSnapshot for %q is already in progress", snapshotName)
	}
	defer d.controllerInFlight.release(snapshotName)

	// The idempotent-retry lookup must come first: a snapshot that was
	// already cut successfully must be returned even when its source
	// volume has been deleted since. Only a genuinely new snapshot needs
	// the source volume to exist.
	existingSnapshot, err := cloudClient.GetSnapshotByName(ctx, snapshotName)
	if err != nil && !errors.Is(err, cloud.ErrSnapshotNotFound) {
		// A failed read says nothing about whether the snapshot exists; see
		// createRPCError for why that must not be a final error.
		return nil, status.Errorf(codes.Unavailable, "failed to get snapshot by name %q: %v", snapshotName, err)
	}

	if existingSnapshot != nil {
		return existingSnapshotResponse(existingSnapshot, snapshotName, sourceVolumeId)
	}

	if _, err := cloudClient.GetVolumeByID(ctx, sourceVolumeId); err != nil {
		if errors.Is(err, cloud.ErrVolumeNotFound) {
			return nil, status.Errorf(codes.NotFound, "source volume %q not found", sourceVolumeId)
		}
		return nil, status.Errorf(codes.Unavailable, "failed to get source volume %q: %v", sourceVolumeId, err)
	}

	newSnapshot, err := cloudClient.CreateSnapshot(ctx, &cloud.SnapshotOptions{
		Name:           snapshotName,
		SourceVolumeID: sourceVolumeId,
		Description:    createdByCSI,
	})
	if err != nil {
		klog.Errorf("[CreateSnapshot] failed to create snapshot(%s): %v", snapshotName, err)
		if errors.Is(err, cloud.ErrNameInUse) {
			// Cut by a request this one did not see; the API refuses the
			// duplicate (measured: 1100015), so the snapshot behind the
			// name is the one to return.
			existingSnapshot, lookupErr := cloudClient.GetSnapshotByName(ctx, snapshotName)
			if lookupErr == nil {
				return existingSnapshotResponse(existingSnapshot, snapshotName, sourceVolumeId)
			}
			return nil, status.Errorf(codes.Aborted, "snapshot name %q is in use by a concurrent request and the snapshot is not readable yet: %v", snapshotName, lookupErr)
		}
		return nil, createRPCError(err, "failed to create snapshot %q: %v", snapshotName, err)
	}

	// The create response already describes the snapshot; the re-read only
	// refreshes it. The snapshot exists, so a failed re-read must not fail
	// the request (see createRPCError): the create response stands in, and
	// the snapshotter keeps polling until the snapshot reports ready.
	createdSnapshot := newSnapshot
	if refreshedSnapshot, err := cloudClient.GetSnapshotByID(ctx, newSnapshot.ID); err == nil {
		createdSnapshot = refreshedSnapshot
	} else {
		klog.Warningf("[CreateSnapshot] snapshot %s was created but could not be re-read, answering from the create response: %v", newSnapshot.ID, err)
	}

	klog.V(2).Infof("[CreateSnapshot] succeeded: snapshot=%s(%s)", snapshotName, createdSnapshot.ID)
	return &csi.CreateSnapshotResponse{
		Snapshot: toCSISnapshot(createdSnapshot),
	}, nil
}

// existingSnapshotResponse answers a CreateSnapshot whose name already
// names a snapshot -- found by the lookup, or reported by the API when the
// create collided with it. A retry of the same request must return the
// same snapshot; the same name for a different source volume must fail.
func existingSnapshotResponse(existingSnapshot *cloud.Snapshot, snapshotName, sourceVolumeId string) (*csi.CreateSnapshotResponse, error) {
	if existingSnapshot.SourceVolumeID != sourceVolumeId {
		return nil, status.Errorf(codes.AlreadyExists,
			"snapshot %q already exists for source volume %s, which does not match requested source volume %s",
			snapshotName, existingSnapshot.SourceVolumeID, sourceVolumeId)
	}
	klog.V(2).Infof("[CreateSnapshot] snapshot already exists: %s(%s)", snapshotName, existingSnapshot.ID)
	return &csi.CreateSnapshotResponse{
		Snapshot: toCSISnapshot(existingSnapshot),
	}, nil
}

func (d *Driver) DeleteSnapshot(ctx context.Context, req *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {
	klog.V(4).Infof("[DeleteSnapshot] called with args %+v", protosanitizer.StripSecrets(req))

	cloudClient := d.cloud

	if req.GetSnapshotId() == "" {
		return nil, status.Error(codes.InvalidArgument, "DeleteSnapshot Snapshot ID must be provided")
	}
	// Same contract as DeleteVolume: an ID that cannot be an instance
	// number cannot name an existing snapshot.
	if !ncloudVolumeIDPattern.MatchString(req.GetSnapshotId()) {
		klog.V(2).Infof("[DeleteSnapshot] snapshot ID %q is not an instance number, treating as already deleted", req.GetSnapshotId())
		return &csi.DeleteSnapshotResponse{}, nil
	}

	if !d.controllerInFlight.acquire(req.GetSnapshotId()) {
		return nil, status.Errorf(codes.Aborted, "another operation on snapshot %q is already in progress", req.GetSnapshotId())
	}
	defer d.controllerInFlight.release(req.GetSnapshotId())

	klog.V(2).Infof("[DeleteSnapshot] snapshotId=%s", req.GetSnapshotId())

	if _, err := cloudClient.GetSnapshotByID(ctx, req.GetSnapshotId()); err != nil {
		if errors.Is(err, cloud.ErrSnapshotNotFound) {
			klog.Warningf("[DeleteSnapshot] snapshot(%s) is already deleted", req.GetSnapshotId())
			return &csi.DeleteSnapshotResponse{}, nil
		}
		return nil, status.Errorf(codes.Internal, "failed to get snapshot %q: %v", req.GetSnapshotId(), err)
	}

	if err := cloudClient.DeleteSnapshot(ctx, req.GetSnapshotId()); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to delete snapshot %q: %v", req.GetSnapshotId(), err)
	}

	klog.V(2).Infof("[DeleteSnapshot] succeeded: snapshotId=%s", req.GetSnapshotId())
	return &csi.DeleteSnapshotResponse{}, nil
}

func (d *Driver) ListSnapshots(ctx context.Context, req *csi.ListSnapshotsRequest) (*csi.ListSnapshotsResponse, error) {
	klog.V(4).Infof("[ListSnapshots] called with args %+v", protosanitizer.StripSecrets(req))

	cloudClient := d.cloud

	if req.MaxEntries < 0 {
		return nil, status.Errorf(codes.InvalidArgument, "max entries must not be negative: %d", req.MaxEntries)
	}

	// Case 1: specific snapshot ID requested — return single entry.
	if req.GetSnapshotId() != "" {
		if !ncloudVolumeIDPattern.MatchString(req.GetSnapshotId()) {
			return &csi.ListSnapshotsResponse{}, nil
		}
		snap, err := cloudClient.GetSnapshotByID(ctx, req.GetSnapshotId())
		if err != nil {
			if errors.Is(err, cloud.ErrSnapshotNotFound) {
				return &csi.ListSnapshotsResponse{}, nil
			}
			return nil, status.Errorf(codes.Internal, "failed to get snapshot %s: %v", req.GetSnapshotId(), err)
		}
		// Both filters set: the entry must satisfy both, not just the ID.
		if src := req.GetSourceVolumeId(); src != "" && snap.SourceVolumeID != src {
			return &csi.ListSnapshotsResponse{}, nil
		}
		return &csi.ListSnapshotsResponse{
			Entries: []*csi.ListSnapshotsResponse_Entry{
				{Snapshot: toCSISnapshot(snap)},
			},
		}, nil
	}

	// Case 2: filter by source volume ID.
	// Case 3: no filter — list all snapshots.
	var (
		snapshots []*cloud.Snapshot
		listErr   error
	)
	if req.GetSourceVolumeId() != "" {
		if !ncloudVolumeIDPattern.MatchString(req.GetSourceVolumeId()) {
			return &csi.ListSnapshotsResponse{}, nil
		}
		snapshots, listErr = cloudClient.GetSnapshotsByVolumeID(ctx, req.GetSourceVolumeId())
	} else {
		snapshots, listErr = cloudClient.ListSnapshots(ctx)
	}
	if listErr != nil {
		return nil, status.Errorf(codes.Internal, "failed to list snapshots: %v", listErr)
	}

	// Pagination: StartingToken is an index into the snapshot list. Parse
	// into a width that cannot wrap and only narrow after bounds-checking
	// against the actual list length.
	startingToken := 0
	if v := req.GetStartingToken(); v != "" {
		parsed, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return nil, status.Errorf(codes.Aborted, "invalid starting token %q: %v", v, err)
		}
		if parsed > uint64(len(snapshots)) {
			return nil, status.Errorf(codes.Aborted,
				"starting token %d is beyond the end of the snapshot list (%d entries)",
				parsed, len(snapshots))
		}
		startingToken = int(parsed)
	}

	rem := len(snapshots) - startingToken
	maxEntries := int(req.MaxEntries)
	if maxEntries == 0 || maxEntries > rem {
		maxEntries = rem
	}

	entries := make([]*csi.ListSnapshotsResponse_Entry, maxEntries)
	for i := 0; i < maxEntries; i++ {
		entries[i] = &csi.ListSnapshotsResponse_Entry{
			Snapshot: toCSISnapshot(snapshots[startingToken+i]),
		}
	}

	var nextToken string
	if startingToken+maxEntries < len(snapshots) {
		nextToken = strconv.Itoa(startingToken + maxEntries)
	}

	klog.V(2).Infof("[ListSnapshots] returning %d entries, nextToken=%q", len(entries), nextToken)
	return &csi.ListSnapshotsResponse{
		Entries:   entries,
		NextToken: nextToken,
	}, nil
}

func (d *Driver) ControllerGetVolume(_ context.Context, _ *csi.ControllerGetVolumeRequest) (*csi.ControllerGetVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

func (d *Driver) ControllerModifyVolume(_ context.Context, _ *csi.ControllerModifyVolumeRequest) (*csi.ControllerModifyVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

func toCSISnapshot(snap *cloud.Snapshot) *csi.Snapshot {
	return &csi.Snapshot{
		SnapshotId:     snap.ID,
		SourceVolumeId: snap.SourceVolumeID,
		SizeBytes:      snap.SizeBytes,
		CreationTime:   timestamppb.New(snap.CreatedAt),
		ReadyToUse:     snap.ReadyToUse,
	}
}
