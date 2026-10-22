// Ncloud Block Storage CSI Driver
// Copyright (c) 2026-present NAVER Cloud Corp.
// Apache-2.0

package driver

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"

	sanity "github.com/kubernetes-csi/csi-test/v5/pkg/sanity"
	ginkgo "github.com/onsi/ginkgo/v2"
	gomega "github.com/onsi/gomega"

	"github.com/NaverCloudPlatform/ncloud-block-storage-csi-driver/pkg/cloud"
	"github.com/NaverCloudPlatform/ncloud-block-storage-csi-driver/pkg/mount"
	"github.com/NaverCloudPlatform/ncloud-block-storage-csi-driver/pkg/utils"
)

const (
	sanityNodeID   = "1234567"
	sanityZoneCode = "KR-1"
)

// newFakeIMDS serves the two IMDSv2 endpoints NewDriver needs in node mode.
func newFakeIMDS(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/latest/api/token", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		fmt.Fprint(w, "sanity-token")
	})
	// The real IMDS terminates every value with a newline; answer the same
	// way so the suite exercises the client's trimming.
	mux.HandleFunc("/latest/meta-data/serverInstanceNo", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintln(w, sanityNodeID)
	})
	mux.HandleFunc("/latest/meta-data/zoneCode", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintln(w, sanityZoneCode)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// sanityIDGenerator produces IDs in the numeric instance-number form the
// driver expects; the sanity default generates UUID-style strings that the
// node service would reject as malformed instead of unknown.
type sanityIDGenerator struct{ counter atomic.Uint64 }

func (g *sanityIDGenerator) GenerateUniqueValidVolumeID() string {
	return strconv.FormatUint(90000000+g.counter.Add(1), 10)
}
func (g *sanityIDGenerator) GenerateInvalidVolumeID() string { return "not-a-number" }
func (g *sanityIDGenerator) GenerateUniqueValidNodeID() string {
	return strconv.FormatUint(80000000+g.counter.Add(1), 10)
}
func (g *sanityIDGenerator) GenerateInvalidNodeID() string { return "not-a-number" }

// TestSanity runs the csi-sanity suite in-process against fakes: FakeCloud
// for the backend, FakeMounter for the host filesystem, a fake IMDS for node
// metadata, and a stubbed device resolver standing in for the
// /dev/disk/by-id entries a real attach would create.
//
// The spec tree is registered twice — once with mount test volumes and once
// with raw block test volumes (covering the block staging/publish paths the
// mount run does not reach) — under a single Ginkgo run, because Ginkgo
// does not support calling RunSpecs more than once per process.
func TestSanity(t *testing.T) {
	imds := newFakeIMDS(t)
	t.Setenv("NCLOUD_METADATA_API_ENDPOINT", imds.URL)

	fc := cloud.NewFakeCloud(sanityZoneCode, &cloud.ServerInstance{
		ServerInstanceNo:     sanityNodeID,
		ZoneCode:             sanityZoneCode,
		RegionCode:           "KR",
		MaxBlockStorageCount: 20,
	})

	// A real attach creates the by-id symlink; the stub answers the same
	// question from FakeCloud attachment state.
	orig := resolveDevicePath
	resolveDevicePath = func(_ context.Context, _ deviceDiscoveryOptions, _, volumeID string) (string, error) {
		vol, err := fc.GetVolumeByID(context.Background(), volumeID)
		if err != nil {
			return "", fmt.Errorf("could not resolve device for volume %s: %w", volumeID, err)
		}
		if vol.ServerInstanceNo != sanityNodeID {
			return "", fmt.Errorf("volume %s is not attached to node %s", volumeID, sanityNodeID)
		}
		return "/dev/sanity-" + volumeID, nil
	}
	t.Cleanup(func() { resolveDevicePath = orig })

	// Unix socket paths are limited to ~104 bytes on macOS, so the socket
	// lives in a short MkdirTemp directory instead of t.TempDir().
	dir, err := os.MkdirTemp("", "csi-sanity-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })

	endpoint := "unix://" + filepath.Join(dir, "csi.sock")
	fm := mount.NewFakeMounter()
	drv, err := NewDriver(fc, fm, &Options{
		Endpoint:      endpoint,
		Mode:          AllMode,
		RegionCode:    "KR",
		DefaultFsType: "ext4",
	})
	if err != nil {
		t.Fatalf("failed to create driver: %v", err)
	}
	go func() {
		if err := drv.Run(); err != nil {
			panic(fmt.Sprintf("driver server failed: %v", err))
		}
	}()
	t.Cleanup(drv.Stop)

	newCfg := func(accessType string) sanity.TestConfig {
		cfg := sanity.NewTestConfig()
		cfg.Address = endpoint
		cfg.TargetPath = filepath.Join(dir, "csi-mount-"+accessType)
		cfg.StagingPath = filepath.Join(dir, "csi-staging-"+accessType)
		cfg.TestVolumeSize = 10 * utils.GiB
		cfg.TestVolumeExpandSize = 20 * utils.GiB
		cfg.TestVolumeAccessType = accessType
		// FakeCloud enforces the per-server attachment limit the driver
		// reads at startup (MaxBlockStorageCount above), so the suite can
		// verify the publish-beyond-limit failure.
		cfg.TestNodeVolumeAttachLimit = true
		cfg.IDGen = &sanityIDGenerator{}
		// FakeMounter performs no real mounts, so path checks are answered
		// from its mount table first and fall back to the real filesystem
		// for the directories the sanity suite itself creates.
		cfg.CheckPath = func(path string) (sanity.PathKind, error) {
			if fsType, ok := fm.MountInfo(path); ok {
				if fsType == "" {
					return sanity.PathIsFile, nil // raw block bind target
				}
				return sanity.PathIsDir, nil
			}
			fi, err := os.Stat(path)
			switch {
			case err == nil && fi.IsDir():
				return sanity.PathIsDir, nil
			case err == nil:
				return sanity.PathIsFile, nil
			case os.IsNotExist(err):
				return sanity.PathIsNotFound, nil
			default:
				return sanity.PathIsOther, err
			}
		}
		return cfg
	}

	cfgMount := newCfg("mount")
	cfgBlock := newCfg("block")
	scMount := sanity.GinkgoTest(&cfgMount)
	scBlock := sanity.GinkgoTest(&cfgBlock)
	gomega.RegisterFailHandler(ginkgo.Fail)
	ginkgo.RunSpecs(t, "CSI Driver Test Suite")
	scMount.Finalize()
	scBlock.Finalize()
}
