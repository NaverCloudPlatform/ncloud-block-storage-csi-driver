// Ncloud Block Storage CSI Driver
// Copyright (c) 2026-present NAVER Cloud Corp.
// Apache-2.0
//
// Derived from digitalocean/csi-digitalocean (Apache-2.0), forked at v0.4.1
// and merged up to v1.1.2 with later code ported from v1.2.0, Copyright
// 2018-2020 DigitalOcean, through the in-house nks-csi fork, and modified
// by NAVER Cloud Corp.

// Package driver implements the CSI specification for Ncloud Block Storage.
// It supports controller, node, and identity services and can run in
// controller-only, node-only, or combined (all) mode.
package driver

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"sync"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"

	"github.com/NaverCloudPlatform/ncloud-block-storage-csi-driver/pkg/cloud"
	"github.com/NaverCloudPlatform/ncloud-block-storage-csi-driver/pkg/mount"
)

const (
	ControllerMode = "controller"
	NodeMode       = "node"
	AllMode        = "all"

	// DriverName is the CSI driver name registered with Kubernetes.
	DriverName = "blk.csi.ncloud.com"
	// DefaultVolumeType is the default Ncloud block storage volume type.
	DefaultVolumeType = VolumeTypeCB1
)

var (
	gitTreeState = "not a git tree"
	commit       string
	version      string
)

type Options struct {
	Endpoint      string
	Mode          string
	RegionCode    string
	DefaultFsType string // "ext4" or "xfs"; empty means auto-detect from the host OS
	// LegacyRegionNo, when non-empty, is advertised as the legacy
	// "regionNo" node topology segment for compatibility with PVs
	// provisioned by the original nks-csi driver. Numeric string.
	LegacyRegionNo string
	// DeviceDiscoveryTimeout bounds how long the attach paths (NodeStage,
	// block NodePublish) wait for the attached device's by-id symlink to
	// appear. Zero or negative means a single check.
	DeviceDiscoveryTimeout time.Duration
}

type Driver struct {
	csi.UnimplementedIdentityServer
	csi.UnimplementedControllerServer
	csi.UnimplementedNodeServer

	cloud   cloud.Cloud
	mounter mount.Mounter

	name                  string
	publishInfoVolumeName string

	endpoint         string
	serverInstanceNo string

	regionCode        string
	zoneCode          string
	topologyZoneValue string // value for topology.kubernetes.io/zone; always the zone number

	mode                   string
	maxVolumesPerNode      int64
	deviceDiscoveryTimeout time.Duration
	defaultFsType          string
	legacyRegionNo         string

	// inFlight serializes the node RPCs per volume. The kubelet retries
	// after two minutes, which is also the mount helpers' deadline, so a
	// retry can otherwise reach mkfs or mount while the first call is
	// still inside it.
	inFlight inFlight

	// controllerInFlight serializes the controller RPCs per resource: the
	// volume or snapshot ID, or the Ncloud name for a create.
	// ControllerPublishVolume waits for a volume to finish creating and
	// then for the attach to settle, which can outlast the attacher's
	// 60-second deadline; the attacher moves on, and an unpublish that
	// then runs beside the attach -- before the API shows it -- reads
	// "not attached" and succeeds, leaving an attachment nothing would
	// detach. Kept apart from the node guard so that, in all mode, a node
	// RPC and a controller RPC on the same volume do not refuse each
	// other. A guard lives in one process: during a rolling update the
	// old pod's handlers finish under GracefulStop beside the new pod's
	// calls, out of its sight.
	controllerInFlight inFlight

	srv     *grpc.Server
	readyMu sync.Mutex
	ready   bool
	// stopping records a Stop() that arrived before Run() built the
	// server. Without it the signal is consumed and nothing acts on it:
	// Run() goes on to serve, and the process ignores SIGTERM until the
	// kubelet escalates to SIGKILL.
	stopping bool
}

func NewDriver(c cloud.Cloud, m mount.Mounter, opts *Options) (*Driver, error) {
	if opts.Endpoint == "" {
		return nil, fmt.Errorf("endpoint must be provided")
	}
	switch opts.Mode {
	case ControllerMode, NodeMode, AllMode:
	default:
		return nil, fmt.Errorf("invalid mode %q: must be one of %s, %s, %s", opts.Mode, ControllerMode, NodeMode, AllMode)
	}
	switch opts.DefaultFsType {
	case "", fsTypeExt4, fsTypeXfs:
	default:
		// Anything else would reach mkfs.<value> on the first stage of a
		// volume without an fsType; fail at startup instead.
		return nil, fmt.Errorf("invalid defaultFsType %q: must be %s, %s or empty (auto-detect from the host OS)", opts.DefaultFsType, fsTypeExt4, fsTypeXfs)
	}
	if opts.LegacyRegionNo != "" {
		if _, err := strconv.ParseUint(opts.LegacyRegionNo, 10, 32); err != nil {
			return nil, fmt.Errorf("invalid legacy-topology-region-no %q: must be the numeric regionNo found in the node labels (e.g. \"1\")", opts.LegacyRegionNo)
		}
	}

	klog.V(4).Infof("NewDriver: endpoint=%s, regionCode=%s, mode=%s",
		opts.Endpoint, opts.RegionCode, opts.Mode)

	d := &Driver{
		cloud:                 c,
		mounter:               m,
		name:                  DriverName,
		publishInfoVolumeName: DriverName + "/volume-name",
		endpoint:              opts.Endpoint,
		regionCode:            opts.RegionCode,
		mode:                  opts.Mode,
		legacyRegionNo:        opts.LegacyRegionNo,
	}
	if opts.DeviceDiscoveryTimeout > 0 {
		d.deviceDiscoveryTimeout = opts.DeviceDiscoveryTimeout
	}

	if opts.Mode == NodeMode || opts.Mode == AllMode {
		d.defaultFsType = opts.DefaultFsType
		if d.defaultFsType == "" {
			d.defaultFsType = detectDefaultFsType(defaultOsReleasePath)
		}
		klog.V(2).Infof("defaultFsType: %s", d.defaultFsType)

		var serverInstanceNo, zoneCode string
		if err := withStartupRetries("node metadata (IMDS)", func() (err error) {
			serverInstanceNo, zoneCode, err = getNodeInfoFromMetadata()
			return err
		}); err != nil {
			return nil, fmt.Errorf("failed to get node info from metadata: %w", err)
		}
		d.serverInstanceNo = serverInstanceNo
		d.zoneCode = zoneCode

		// topology.kubernetes.io/zone must carry the zone number the Ncloud
		// cloud controller manager labels nodes with. A zone missing from
		// the driver's table cannot be advertised, so refuse to start
		// instead of registering a node plugin the kubelet would reject
		// with a topology value collision.
		zoneNo, err := cloud.ZoneNoFromCode(zoneCode)
		if err != nil {
			return nil, fmt.Errorf("cannot advertise the zone of this node: %w; supporting this zone needs a driver release", err)
		}
		d.topologyZoneValue = zoneNo

		// The API region must be this node's region. A block volume attaches
		// only to a server in its own zone, so calls sent elsewhere operate on
		// a region holding none of this node's volumes -- and the symptom
		// surfaces far from the cause, as a volume that was just created not
		// being found, or an attach failing on a volume the console shows.
		// Every supported zone code carries its API region as the prefix, so
		// this runs after the zone table check: an unsupported zone fails on
		// the table, which names the actual remedy, rather than on a region
		// mismatch that is only its consequence.
		if err := cloud.ValidateRegionForZone(d.regionCode, zoneCode); err != nil {
			return nil, fmt.Errorf("--regionCode does not match this node: %w", err)
		}

		// The attachment limit lands in CSINode.allocatable and sticks
		// until the plugin re-registers; a guessed value that turns out
		// wrong strands pods in ContainerCreating with nothing pointing
		// at the cause. So: an explicit override skips the lookup
		// entirely (and is the escape hatch during an API outage), an
		// API-reported limit is used as is, and anything else fails the
		// startup loudly instead of advertising a guess.
		override, err := maxVolumesOverrideFromEnv()
		if err != nil {
			return nil, err
		}
		if override > 0 {
			klog.V(2).Infof("%s overrides the attachment limit: %d (server spec lookup skipped)", maxVolumesPerNodeEnv, override)
			d.maxVolumesPerNode = override
		} else {
			var inst *cloud.ServerInstance
			if err := withStartupRetries("server spec lookup", func() (err error) {
				inst, err = c.GetServerInstance(context.Background(), serverInstanceNo)
				return err
			}); err != nil {
				return nil, fmt.Errorf("failed to get server instance %s (set %s / chart value maxVolumesPerNode to start without the lookup): %w",
					serverInstanceNo, maxVolumesPerNodeEnv, err)
			}
			if inst.MaxBlockStorageCount <= 0 {
				return nil, fmt.Errorf("server instance %s did not report a block storage attachment limit; set %s (chart value maxVolumesPerNode) explicitly",
					serverInstanceNo, maxVolumesPerNodeEnv)
			}
			d.maxVolumesPerNode = int64(inst.MaxBlockStorageCount)
		}
		klog.V(2).Infof("resolved serverInstanceNo=%s zoneCode=%q topologyZoneValue=%q maxVolumesPerNode=%d",
			serverInstanceNo, zoneCode, d.topologyZoneValue, d.maxVolumesPerNode)
	}

	return d, nil
}

// unaryServerInterceptor logs RPC failures and converts handler panics into
// an Internal error instead of letting them kill the whole driver process.
func unaryServerInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
	defer func() {
		if r := recover(); r != nil {
			klog.Errorf("panic in %s: %v\n%s", info.FullMethod, r, debug.Stack())
			err = status.Errorf(codes.Internal, "internal error handling %s", info.FullMethod)
		}
	}()

	resp, err = handler(ctx, req)
	if err != nil {
		klog.Errorf("method failed: method=%s, err=%v", info.FullMethod, err)
	}
	return resp, err
}

// maxVolumesPerNodeEnv overrides the attachment limit reported by the
// server spec API. Same variable name as the original nks-csi driver.
const maxVolumesPerNodeEnv = "MAX_VOLUMES_PER_NODE"

// maxVolumesOverrideFromEnv returns the MAX_VOLUMES_PER_NODE override,
// or (0, nil) when the variable is unset.
func maxVolumesOverrideFromEnv() (int64, error) {
	v := os.Getenv(maxVolumesPerNodeEnv)
	if v == "" {
		return 0, nil
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n < 1 {
		return 0, fmt.Errorf("invalid %s %q: must be a positive integer", maxVolumesPerNodeEnv, v)
	}
	return n, nil
}

// Startup lookups tolerate brief blips (IMDS or API gateway hiccups) with
// a couple of quick retries, but a persistent failure must stop the node
// plugin: guessing an attachment limit or topology would be recorded in
// CSINode/node labels and mislead the scheduler long after the blip.
var (
	startupRetryAttempts = 3
	startupRetryDelay    = 2 * time.Second
)

// withStartupRetries runs fn until it succeeds or the attempts run out,
// backing off linearly between tries, and returns the last error. It is
// for startup lookups only: the delays are blocking, and every caller is
// on the path that either resolves the node's identity or refuses to
// serve.
func withStartupRetries(what string, fn func() error) error {
	var err error
	for attempt := 1; ; attempt++ {
		// Logged before the call: with the API slow each attempt can run
		// for a minute, and this line is what tells an operator reading
		// the pod log what the plugin is waiting for.
		klog.Infof("%s (attempt %d/%d)", what, attempt, startupRetryAttempts)
		if err = fn(); err == nil {
			return nil
		}
		if attempt >= startupRetryAttempts {
			return err
		}
		delay := time.Duration(attempt) * startupRetryDelay
		klog.Warningf("%s failed (attempt %d/%d), retrying in %v: %v", what, attempt, startupRetryAttempts, delay, err)
		time.Sleep(delay)
	}
}

// getNodeInfoFromMetadata retrieves serverInstanceNo and zoneCode from the
// Ncloud Instance Metadata Service v2 (IMDSv2). This works regardless of
// whether CCM is deployed and does not rely on node.Spec.ProviderID.
func getNodeInfoFromMetadata() (serverInstanceNo, zoneCode string, err error) {
	mc := newMetadataClient()

	serverInstanceNo, err = mc.get("serverInstanceNo")
	if err != nil {
		return "", "", fmt.Errorf("failed to get serverInstanceNo from metadata: %w", err)
	}
	if serverInstanceNo == "" {
		return "", "", fmt.Errorf("metadata returned empty serverInstanceNo")
	}

	zoneCode, err = mc.get("zoneCode")
	if err != nil {
		return "", "", fmt.Errorf("failed to get zoneCode from metadata: %w", err)
	}
	if zoneCode == "" {
		return "", "", fmt.Errorf("metadata returned empty zoneCode")
	}

	return serverInstanceNo, zoneCode, nil
}

func (d *Driver) Run() error {
	klog.V(2).Infoln("Run")

	// Checked before the socket is touched: a shutdown that arrived while
	// the driver was still starting should not leave a bound socket
	// behind, and removing the existing one would disturb whatever holds
	// it. The same flag is read again once the server exists, for a
	// signal that lands during the setup below.
	if d.shutdownRequested() {
		klog.V(2).Infoln("shutdown requested before startup finished; not serving")
		return nil
	}

	u, err := url.Parse(d.endpoint)
	if err != nil {
		return fmt.Errorf("unable to parse address: %w", err)
	}

	addr := filepath.Join(u.Host, filepath.FromSlash(u.Path))
	if u.Host == "" {
		addr = filepath.FromSlash(u.Path)
	}

	if u.Scheme != "unix" {
		return fmt.Errorf("currently only unix domain sockets are supported, have: %s", u.Scheme)
	}

	klog.V(2).Infof("removing socket: %s", addr)
	if err := os.Remove(addr); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove unix domain socket file %s: %s", addr, err)
	}

	listener, err := net.Listen(u.Scheme, addr)
	if err != nil {
		return fmt.Errorf("failed to listen: %v", err)
	}

	srv := grpc.NewServer(grpc.UnaryInterceptor(unaryServerInterceptor))
	csi.RegisterIdentityServer(srv, d)

	if d.mode == ControllerMode || d.mode == AllMode {
		csi.RegisterControllerServer(srv, d)
	}

	if d.mode == NodeMode || d.mode == AllMode {
		csi.RegisterNodeServer(srv, d)
	}

	d.readyMu.Lock()
	if d.stopping {
		d.readyMu.Unlock()
		_ = listener.Close()
		klog.V(2).Infoln("shutdown requested while the server was being set up; not serving")
		return nil
	}
	d.srv = srv
	d.ready = true
	d.readyMu.Unlock()
	klog.V(2).Infof("server started: addr=%s", addr)

	return serve(srv, listener)
}

// serve runs the gRPC server until it is stopped. Stop() can land between
// the moment Run publishes the server above and this call: GracefulStop
// then runs first and gRPC answers the Serve that follows with
// ErrServerStopped. That is the shutdown that was asked for, not a
// failure for main to report (and exit non-zero) as one.
func serve(srv *grpc.Server, listener net.Listener) error {
	if err := srv.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
		return err
	}
	return nil
}

// topologyForZone returns the accessible topology for a volume's zone as
// the zone number the node label carries. A volume whose zone the API did
// not report gets no segment — an empty value would give the PV a
// nodeAffinity no node can ever satisfy (permanent Pending) — and a zone
// outside the driver's table is an error: the driver cannot say where the
// volume lives, and no node it serves runs in such a zone anyway.
func (d *Driver) topologyForZone(zoneCode string) ([]*csi.Topology, error) {
	if zoneCode == "" {
		klog.Warningf("volume reported no zone code; omitting the zone topology segment")
		return nil, nil
	}
	zoneNo, err := cloud.ZoneNoFromCode(zoneCode)
	if err != nil {
		return nil, err
	}
	return []*csi.Topology{{Segments: map[string]string{
		corev1.LabelTopologyZone: zoneNo,
	}}}, nil
}

// shutdownRequested reports whether Stop() has already been called.
func (d *Driver) shutdownRequested() bool {
	d.readyMu.Lock()
	defer d.readyMu.Unlock()
	return d.stopping
}

// Stop performs a graceful shutdown of the gRPC server, waiting for
// in-flight RPCs to complete before stopping.
func (d *Driver) Stop() {
	d.readyMu.Lock()
	d.ready = false
	d.stopping = true
	srv := d.srv
	d.readyMu.Unlock()

	// A signal can arrive before Run() has created the server. Recording
	// it above is the point: Run() checks the flag and declines to serve,
	// so the signal is not simply swallowed.
	if srv == nil {
		klog.V(2).Infoln("server not started yet; shutdown recorded so it will not start")
		return
	}

	klog.V(2).Infoln("gracefully stopping server")
	srv.GracefulStop()
	klog.V(2).Infoln("server stopped")
}

func GetVersion() string   { return version }
func GetCommit() string    { return commit }
func GetTreeState() string { return gitTreeState }
