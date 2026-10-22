// Ncloud Block Storage CSI Driver
// Copyright (c) 2026-present NAVER Cloud Corp.
// Apache-2.0
//
// Derived from digitalocean/csi-digitalocean (Apache-2.0), forked at v0.4.1
// and merged up to v1.1.2 with later code ported from v1.2.0, Copyright
// 2018-2020 DigitalOcean, through the in-house nks-csi fork, and modified
// by NAVER Cloud Corp.

// Entry point for the Ncloud Block Storage CSI Driver.
// It initializes the cloud client and the mounter, then starts the gRPC
// server in the configured mode (controller/node/all).
package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"k8s.io/klog/v2"

	"github.com/NaverCloudPlatform/ncloud-block-storage-csi-driver/pkg/cloud"
	"github.com/NaverCloudPlatform/ncloud-block-storage-csi-driver/pkg/driver"
	"github.com/NaverCloudPlatform/ncloud-block-storage-csi-driver/pkg/mount"
)

var (
	mode = flag.String("mode", "all", "Operating mode: controller, node or all.")

	endpoint = flag.String("endpoint", "unix:///var/lib/kubelet/plugins/"+driver.DriverName+"/csi.sock", "CSI endpoint")

	regionCode = flag.String("regionCode", "KR",
		"Ncloud API region: KR, SGN, JPN, USWN, DEN, FKR or KRS. Topology does not come from it (the node plugin reads its zone "+
			"from the instance metadata), but it must be the region of that zone -- the prefix of the zone code -- or the node plugin refuses to start.")

	apiUrl = flag.String("apiUrl", "",
		"Ncloud API Gateway origin: scheme and host only, no path (the driver appends /vserver/v2). Empty leaves the SDK default -- the public "+
			"gateway, or NCLOUD_API_GW when that variable is set; the public gateway serves the public platform only, so the financial and "+
			"government platforms need their own gateway (see the README).")

	defaultFsType = flag.String("defaultFsType", "",
		"Default filesystem type (ext4/xfs) used when a volume does not specify one. Empty means auto-detect from the host OS.")

	legacyRegionNo = flag.String("legacy-topology-region-no", "",
		"Numeric region number advertised as the legacy \"regionNo\" node topology key, for clusters whose PVs carry "+
			"regionNo/zoneNo node affinity from an earlier driver. "+
			"Read the value from such a node: kubectl get nodes -o jsonpath='{.items[0].metadata.labels.regionNo}'. "+
			"Leave empty otherwise.")

	deviceDiscoveryTimeout = flag.Duration("device-discovery-timeout", 60*time.Second,
		"how long NodeStage (and NodePublish for raw block volumes) waits for the attached device to appear under /dev/disk/by-id before failing; 0 disables waiting. Keep below kubelet's 2-minute CSI call budget.")

	version = flag.Bool("version", false, "Print the version and exit.")
)

func main() {
	klog.InitFlags(nil)
	flag.Parse()

	if *version {
		fmt.Printf("%s - %s (%s)\n", driver.GetVersion(), driver.GetCommit(), driver.GetTreeState())
		os.Exit(0)
	}

	// Region codes are upper case (KR, SGN, ...). The node-side check
	// compares case-insensitively, but the API receives the value as given.
	*regionCode = strings.ToUpper(*regionCode)

	accessKey := os.Getenv("NCLOUD_ACCESS_KEY")
	secretKey := os.Getenv("NCLOUD_SECRET_KEY")

	if accessKey == "" {
		klog.Fatalf("NCLOUD_ACCESS_KEY environment variable must be set")
	}
	if secretKey == "" {
		klog.Fatalf("NCLOUD_SECRET_KEY environment variable must be set")
	}

	cloudClient, err := cloud.NewNcloudCloud(accessKey, secretKey, *regionCode, *apiUrl)
	if err != nil {
		klog.Fatalf("failed to create cloud client: %v", err)
	}

	var mounter mount.Mounter
	if *mode == driver.NodeMode || *mode == driver.AllMode {
		klog.V(2).Infoln("initializing node mounter")
		mounter = mount.NewSafeMounter()
	}

	drv, err := driver.NewDriver(cloudClient, mounter, &driver.Options{
		Endpoint:       *endpoint,
		Mode:           *mode,
		RegionCode:     *regionCode,
		DefaultFsType:  *defaultFsType,
		LegacyRegionNo: *legacyRegionNo,

		DeviceDiscoveryTimeout: *deviceDiscoveryTimeout,
	})
	if err != nil {
		klog.Fatalf("failed to create driver: %v", err)
	}

	stopCh := make(chan os.Signal, 1)
	signal.Notify(stopCh, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-stopCh
		klog.Infof("received signal %v, shutting down", sig)
		drv.Stop()
	}()

	if err := drv.Run(); err != nil {
		klog.Fatalf("driver failed: %v", err)
	}
}
