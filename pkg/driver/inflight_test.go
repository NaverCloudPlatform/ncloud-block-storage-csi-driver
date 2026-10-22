// Ncloud Block Storage CSI Driver
// Copyright (c) 2026-present NAVER Cloud Corp.
// Apache-2.0

package driver

import (
	"context"
	"sync"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/NaverCloudPlatform/ncloud-block-storage-csi-driver/pkg/mount"
)

func TestInFlightAcquireAndRelease(t *testing.T) {
	var f inFlight

	if !f.acquire("9000001") {
		t.Fatal("first acquire should succeed")
	}
	if f.acquire("9000001") {
		t.Error("second acquire of the same volume should fail")
	}
	if !f.acquire("9000002") {
		t.Error("a different volume should not be blocked")
	}

	f.release("9000001")
	if !f.acquire("9000001") {
		t.Error("acquire should succeed again after release")
	}

	// Releasing something not held must not panic or free anything else.
	f.release("9000003")
	if f.acquire("9000002") {
		t.Error("releasing an unrelated key freed a held volume")
	}
}

// Exactly one of many concurrent claims may win.
func TestInFlightAllowsOneWinner(t *testing.T) {
	var f inFlight

	const racers = 64
	var wg sync.WaitGroup
	won := make(chan struct{}, racers)
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if f.acquire("9000001") {
				won <- struct{}{}
			}
		}()
	}
	wg.Wait()
	close(won)

	if n := len(won); n != 1 {
		t.Errorf("expected exactly one winner, got %d", n)
	}
}

// The guard has to be reachable through the RPCs, not just correct on
// its own. NodeUnpublishVolume is the probe because it reaches the
// mounter immediately after taking the guard, so the first call can be
// parked inside the RPC while the others are tried.
func TestNodeRPCsRefuseAConcurrentRequestForTheSameVolume(t *testing.T) {
	const volumeID = "9000001"

	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	d := &Driver{
		defaultFsType: "ext4",
		mounter: &mount.MockMounter{
			// Only the parked volume's target blocks; anything else must
			// return, or the "a different volume is unaffected" probe
			// would wait on this mock instead of on the guard.
			IsMountedFn: func(path string) (bool, error) {
				if path != "/target-held" {
					return false, nil
				}
				once.Do(func() { close(started) })
				<-release
				return false, nil
			},
		},
	}

	first := make(chan error, 1)
	go func() {
		_, err := d.NodeUnpublishVolume(context.Background(), &csi.NodeUnpublishVolumeRequest{
			VolumeId: volumeID, TargetPath: "/target-held",
		})
		first <- err
	}()
	<-started

	// Same volume, any node RPC: refused while the first call holds it.
	if _, err := d.NodeUnpublishVolume(context.Background(), &csi.NodeUnpublishVolumeRequest{
		VolumeId: volumeID, TargetPath: "/target-2",
	}); status.Code(err) != codes.Aborted {
		t.Errorf("concurrent unpublish of the same volume: want Aborted, got %v", err)
	}
	if _, err := d.NodeUnstageVolume(context.Background(), &csi.NodeUnstageVolumeRequest{
		VolumeId: volumeID, StagingTargetPath: "/staging",
	}); status.Code(err) != codes.Aborted {
		t.Errorf("concurrent unstage of the same volume: want Aborted, got %v", err)
	}
	if _, err := d.NodeStageVolume(context.Background(), &csi.NodeStageVolumeRequest{
		VolumeId: volumeID, StagingTargetPath: "/staging", VolumeCapability: rwoMountCapability(),
	}); status.Code(err) != codes.Aborted {
		t.Errorf("concurrent stage of the same volume: want Aborted, got %v", err)
	}
	if _, err := d.NodePublishVolume(context.Background(), &csi.NodePublishVolumeRequest{
		VolumeId: volumeID, StagingTargetPath: "/staging", TargetPath: "/target-2", VolumeCapability: rwoMountCapability(),
	}); status.Code(err) != codes.Aborted {
		t.Errorf("concurrent publish of the same volume: want Aborted, got %v", err)
	}
	if _, err := d.NodeExpandVolume(context.Background(), &csi.NodeExpandVolumeRequest{
		VolumeId: volumeID, VolumePath: "/target-2",
	}); status.Code(err) != codes.Aborted {
		t.Errorf("concurrent expand of the same volume: want Aborted, got %v", err)
	}

	// Validation comes before the guard on every node RPC: a malformed
	// request for the held volume is answered as such, not as a collision.
	if _, err := d.NodePublishVolume(context.Background(), &csi.NodePublishVolumeRequest{
		VolumeId: volumeID, StagingTargetPath: "/staging", TargetPath: "/target-2",
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER},
		},
	}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("unsupported capability on a held volume: want InvalidArgument, got %v", err)
	}

	// A different volume is unaffected.
	if _, err := d.NodeUnpublishVolume(context.Background(), &csi.NodeUnpublishVolumeRequest{
		VolumeId: "9000002", TargetPath: "/target-other",
	}); status.Code(err) == codes.Aborted {
		t.Error("a different volume must not be blocked")
	}

	close(release)
	if err := <-first; err != nil {
		t.Fatalf("the parked call failed: %v", err)
	}

	// Released on return: the next request for it goes through.
	if _, err := d.NodeUnpublishVolume(context.Background(), &csi.NodeUnpublishVolumeRequest{
		VolumeId: volumeID, TargetPath: "/target-2",
	}); status.Code(err) == codes.Aborted {
		t.Error("the volume was not released when the first call returned")
	}
}
