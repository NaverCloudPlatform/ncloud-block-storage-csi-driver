// Ncloud Block Storage CSI Driver
// Copyright (c) 2026-present NAVER Cloud Corp.
// Apache-2.0

// Package cloud provides an abstraction layer for Ncloud block storage operations.
// It defines the Cloud interface that decouples CSI driver logic from the
// underlying Ncloud VPC Server API.
package cloud

import "context"

// Cloud defines the contract for block storage and snapshot operations
// against the Naver Cloud Platform (Ncloud) API.
type Cloud interface {
	// Volume lifecycle
	CreateVolume(ctx context.Context, opts *VolumeOptions) (*Volume, error)
	DeleteVolume(ctx context.Context, volumeID string) error
	GetVolumeByID(ctx context.Context, volumeID string) (*Volume, error)
	GetVolumeByName(ctx context.Context, name string) (*Volume, error)
	ListVolumes(ctx context.Context) ([]*Volume, error)

	// Volume attach/detach
	AttachVolume(ctx context.Context, volumeID, serverInstanceNo string) error
	DetachVolume(ctx context.Context, volumeID string) error
	// EnsureDetachVolume polls until the volume is detached or the context expires.
	EnsureDetachVolume(ctx context.Context, volumeID string) (*Volume, error)

	// Volume resize
	ResizeVolume(ctx context.Context, volumeID string, newSizeGiB int32) error
	// WaitForVolumeResize polls until the volume reports at least minSizeBytes
	// with no operation in progress.
	WaitForVolumeResize(ctx context.Context, volumeID string, minSizeBytes int64) error
	// WaitForVolumeStatus polls until the volume reaches one of the desired statuses.
	WaitForVolumeStatus(ctx context.Context, volumeID string, desiredStatuses []string) error

	// Snapshot lifecycle
	CreateSnapshot(ctx context.Context, opts *SnapshotOptions) (*Snapshot, error)
	DeleteSnapshot(ctx context.Context, snapshotID string) error
	GetSnapshotByID(ctx context.Context, snapshotID string) (*Snapshot, error)
	GetSnapshotByName(ctx context.Context, name string) (*Snapshot, error)
	GetSnapshotsByVolumeID(ctx context.Context, volumeID string) ([]*Snapshot, error)
	ListSnapshots(ctx context.Context) ([]*Snapshot, error)

	// Server metadata
	GetServerInstance(ctx context.Context, serverInstanceNo string) (*ServerInstance, error)
}
