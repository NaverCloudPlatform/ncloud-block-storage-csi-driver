// Ncloud Block Storage CSI Driver
// Copyright (c) 2026-present NAVER Cloud Corp.
// Apache-2.0

package driver

import (
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/NaverCloudPlatform/ncloud-block-storage-csi-driver/pkg/cloud"
)

// shortSocket returns a socket path well under the ~104-byte sun_path
// limit. t.TempDir() embeds the test name, which is long enough here to
// make bind() fail with EINVAL and look like a driver bug.
func shortSocket(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "csi")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return filepath.Join(dir, "s.sock")
}

// SIGTERM can land between signal.Notify and the moment Run() builds the
// gRPC server. The handler calls Stop(), which has no server to stop, and
// if that were the end of it the signal would be swallowed: Run() would
// go on to serve and the process would keep working until the kubelet
// escalates to SIGKILL. Stop() records the request so Run() declines.
func TestRunDeclinesAfterStopWasRequested(t *testing.T) {
	d := &Driver{endpoint: "unix://" + shortSocket(t)}

	d.Stop()

	done := make(chan error, 1)
	go func() { done <- d.Run() }()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run after Stop should return cleanly, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run is serving even though shutdown was requested before it started")
	}

	if d.ready {
		t.Error("driver reports ready after declining to start")
	}
}

// The ordinary path must be unaffected: a driver that was never asked to
// stop serves, and Stop() then shuts it down.
func TestRunServesAndStops(t *testing.T) {
	d := &Driver{endpoint: "unix://" + shortSocket(t)}

	done := make(chan error, 1)
	go func() { done <- d.Run() }()

	deadline := time.After(5 * time.Second)
	for {
		d.readyMu.Lock()
		ready := d.ready
		d.readyMu.Unlock()
		if ready {
			break
		}
		select {
		case <-deadline:
			t.Fatal("server did not become ready")
		case err := <-done:
			t.Fatalf("Run returned early: %v", err)
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	d.Stop()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run returned %v after Stop", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not bring the server down")
	}
}

// Stop() can land between the moment Run() publishes the server and its
// Serve call. GracefulStop then runs first, and gRPC answers the Serve
// that follows with ErrServerStopped -- the shutdown that was asked for,
// which main must not log as "driver failed" and exit non-zero on.
func TestServeTreatsAStoppedServerAsACleanShutdown(t *testing.T) {
	listener, err := net.Listen("unix", shortSocket(t))
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	srv.GracefulStop()

	if err := serve(srv, listener); err != nil {
		t.Errorf("serve after GracefulStop should return cleanly, got %v", err)
	}
}

// A defaultFsType the driver cannot format is refused at startup; left
// alone it would surface as "mkfs.btrfs: not found" on the first stage of
// a volume that carries no fsType of its own.
func TestNewDriverRejectsAnUnknownDefaultFsType(t *testing.T) {
	for _, fsType := range []string{"btrfs", "EXT4", "ext4 "} {
		_, err := NewDriver(&cloud.MockCloud{}, nil, &Options{
			Endpoint: "unix:///tmp/csi-test.sock", Mode: ControllerMode, RegionCode: "KR", DefaultFsType: fsType,
		})
		if err == nil {
			t.Errorf("defaultFsType %q: expected an error, got a driver", fsType)
		}
	}
}
