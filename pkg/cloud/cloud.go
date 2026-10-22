// Ncloud Block Storage CSI Driver
// Copyright (c) 2026-present NAVER Cloud Corp.
// Apache-2.0

package cloud

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"

	"k8s.io/klog/v2"

	"github.com/NaverCloudPlatform/ncloud-sdk-go-v2/ncloud"
	"github.com/NaverCloudPlatform/ncloud-sdk-go-v2/services/vserver"
)

// Sentinel errors returned by Cloud methods when a resource does not exist.
var (
	ErrVolumeNotFound         = errors.New("volume not found")
	ErrSnapshotNotFound       = errors.New("snapshot not found")
	ErrServerInstanceNotFound = errors.New("server instance not found")

	// ErrAttachmentLimitReached is returned by AttachVolume when the target
	// server cannot take another block storage attachment; the driver maps
	// it to the RESOURCE_EXHAUSTED CSI code. The real implementation
	// classifies the Ncloud API's returnCode 1101015 ("Storage allocation
	// exceeded") into this sentinel — measured on the live API by
	// attaching a 21st data volume to a server whose spec
	// reports MaxBlockStorageCount=20.
	ErrAttachmentLimitReached = errors.New("server block storage attachment limit reached")

	// ErrNameInUse is returned by CreateVolume and CreateSnapshot when the
	// API refuses the name because a resource already carries it --
	// returnCode 1103047 for block storage, 1100015 for snapshots, both
	// HTTP 400 (measured on the live gateway, 2026-10-06). The driver
	// reads it as "created by a request this one did not see", not as a
	// failure: the resource behind the name is the one to return.
	ErrNameInUse = errors.New("resource name already in use")
)

const (
	// Polling interval and timeout for asynchronous Ncloud API operations.
	apiInterval = 5 * time.Second
	apiTimeout  = 1 * time.Minute

	// httpTimeout bounds every single Ncloud API HTTP round trip. The SDK
	// neither accepts a context nor sets a client timeout, so without this
	// a black-holed connection blocks forever while holding the rate-limit
	// mutex, freezing every subsequent controller RPC.
	httpTimeout = 30 * time.Second

	// listPageSize is the page size for Ncloud list APIs. The API's own
	// default pageSize is 1, so every list call must paginate explicitly
	// or it silently returns a single row.
	listPageSize int32 = 1000

	// maxListPages bounds pagination loops against a misbehaving backend.
	maxListPages = 1000

	// Ncloud block storage status codes (5-char truncated codes from the Ncloud API).
	StatusAttached = "ATTAC" // Volume is attached to a server instance.
	StatusCreated  = "CREAT" // Volume exists but is not attached.
	OpNormal       = "NULL"  // No operation in progress.
	// OpDetach is the only operation that takes the device away from a
	// node. Measured on the live API: while a detach runs the volume keeps
	// StatusAttached and only the operation code changes, so this is what
	// distinguishes "attached and usable" from "about to lose the device".
	OpDetach = "DETAC"
	// OpAttach runs while a device is being added to a node. Measured on
	// the live API: for the ~27 seconds it takes, the volume still reports
	// StatusCreated while serverInstanceNo already names the target, so
	// status alone reads as "not attached" at the exact moment the volume
	// is being claimed.
	OpAttach = "ATTAC"
)

type Volume struct {
	ID               string
	Name             string
	SizeBytes        int64
	StatusCode       string
	OperationCode    string
	VolumeTypeCode   string
	ServerInstanceNo string
	RegionCode       string
	ZoneCode         string
}

type VolumeOptions struct {
	Name        string
	SizeGiB     int32
	VolumeType  string
	ZoneCode    string
	SnapshotID  string
	Description string
}

type Snapshot struct {
	ID             string
	Name           string
	SourceVolumeID string
	SizeBytes      int64
	StatusCode     string
	OperationCode  string
	CreatedAt      time.Time
	ReadyToUse     bool
}

type SnapshotOptions struct {
	Name           string
	SourceVolumeID string
	Description    string
}

type ServerInstance struct {
	ServerInstanceNo     string
	ZoneCode             string
	RegionCode           string
	MaxBlockStorageCount int32
}

var _ Cloud = &ncloudCloud{}

// collectPages drains a paginated Ncloud list API. fetch is called with
// 1-based page numbers and must return the page items and the API-reported
// total row count. Iteration stops when the accumulated count reaches the
// reported total or a page comes back empty/short.
func collectPages[T any](fetch func(pageNo int32) (items []T, totalRows int32, err error)) ([]T, error) {
	var all []T
	for pageNo := int32(1); pageNo <= maxListPages; pageNo++ {
		items, totalRows, err := fetch(pageNo)
		if err != nil {
			return nil, err
		}
		all = append(all, items...)

		// An empty page ends the iteration. If the API still claims more
		// rows exist than were collected, returning the partial list would
		// silently turn into wrong answers downstream (e.g. a false NotFound
		// in lookups), so that disagreement is an error; the caller/CO
		// retries. Collecting more than totalRows is benign — it just means
		// resources were deleted while paginating — so it only logs.
		if len(items) == 0 {
			if totalRows > 0 && int32(len(all)) < totalRows {
				return nil, fmt.Errorf("list API reported totalRows=%d but pages ended after %d items; the list may have changed during pagination", totalRows, len(all))
			}
			if totalRows > 0 && int32(len(all)) > totalRows {
				klog.Warningf("list API reported totalRows=%d but %d items were collected; the list shrank during pagination", totalRows, len(all))
			}
			return all, nil
		}

		// While the API reports more rows, keep paging even when a page
		// comes back shorter than requested (the server may cap pageSize
		// below the requested value).
		if totalRows > 0 {
			if int32(len(all)) >= totalRows {
				return all, nil
			}
			continue
		}

		// No totalRows signal: a short page is the only end marker.
		if int32(len(items)) < listPageSize {
			return all, nil
		}
	}
	return nil, fmt.Errorf("list did not terminate after %d pages", maxListPages)
}

// errUnparsedAPIResponse marks a 2xx response the SDK handed back without
// parsing. The vendored client skips unmarshalling when the body contains
// `{"error"` and still returns a nil error, so the caller receives a
// zero-valued payload: an empty list that reads as "no such volume", and
// the driver answers a delete or an unpublish with success, or creates a
// second volume for a name that already exists.
//
// Every response of this API carries returnCode in its envelope --
// measured on the live gateway for the list, detail and spec endpoints,
// including a list whose result is legitimately empty (returnCode "0",
// totalRows 0). A nil returnCode therefore means nothing was parsed, not
// that the account holds nothing.
var errUnparsedAPIResponse = errors.New("the API returned a body the SDK could not parse")

// apiReturnCodeSuccess is the envelope returnCode of every successful
// response. Measured on the live gateway (2026-10-06) for all seven
// mutation endpoints the driver calls -- create, attach, change (attached
// and detached), snapshot create and delete, detach, delete -- and for
// the list, detail and spec endpoints: "0", returnMessage "success".
const apiReturnCodeSuccess = "0"

// apiError is a 2xx response whose envelope carries a returnCode other
// than "0". The gateway answers errors with HTTP 4xx and a
// `{"responseError": {...}}` body, which the SDK returns as a plain
// error. The same body under a 2xx status is not rejected by the SDK: it
// unwraps the outer key like any success envelope and fills in only
// returnCode and returnMessage, so the payload is empty and the nil
// check alone would read it as "no such volume" -- or as a successful
// delete, attach or detach. Reproduced against the vendored decode path
// (2026-10-06).
type apiError struct {
	op      string
	code    string
	message string
}

func (e *apiError) Error() string {
	return fmt.Sprintf("%s: the API reported returnCode %s (%q) in a 2xx response; refusing to read it as an empty result", e.op, e.code, e.message)
}

func checkAPIResponse(op string, returnCode, returnMessage *string) error {
	if returnCode == nil {
		return fmt.Errorf("%s: %w (no returnCode in the envelope); refusing to read it as an empty result", op, errUnparsedAPIResponse)
	}
	if *returnCode != apiReturnCodeSuccess {
		return &apiError{op: op, code: *returnCode, message: ncloud.StringValue(returnMessage)}
	}
	return nil
}

// ncloudCloud implements the Cloud interface using the Ncloud VPC Server API.
// Concurrent create/attach/detach calls are rate limited to one per
// second each: the mutex is held across a 1-second sleep and released
// after it, so the next caller cannot start until the interval has
// passed. A caller whose context is already cancelled releases without
// sleeping -- see the guards at each call site.
type ncloudCloud struct {
	client     *vserver.V2ApiService
	regionCode string
	attachMu   sync.Mutex
	detachMu   sync.Mutex
	createMu   sync.Mutex
}

func NewNcloudCloud(accessKey, secretKey, regionCode, apiUrl string) (Cloud, error) {
	apiKeys := &ncloud.APIKey{
		AccessKey: accessKey,
		SecretKey: secretKey,
	}
	config := vserver.NewConfiguration(apiKeys)
	// The SDK falls back to http.DefaultClient (no timeout) when HTTPClient
	// is unset; see httpTimeout for why that must not happen here.
	config.HTTPClient = &http.Client{Timeout: httpTimeout}
	if apiUrl != "" {
		// The value is the gateway origin, the same contract as the SDK's own
		// NCLOUD_API_GW override: the service path is appended here, so a
		// value that already carries it would double the path and fail every
		// call. A trailing slash is tolerated.
		config.BasePath = strings.TrimRight(apiUrl, "/") + "/vserver/v2"
		klog.V(2).Infof("API Gateway URL overridden to: %s", config.BasePath)
	}
	apiClient := vserver.NewAPIClient(config)

	return &ncloudCloud{
		client:     apiClient.V2Api,
		regionCode: regionCode,
	}, nil
}

func (c *ncloudCloud) CreateVolume(ctx context.Context, opts *VolumeOptions) (*Volume, error) {
	c.createMu.Lock()

	// The mutex serializes API calls at one per second; a caller whose
	// RPC was cancelled while queueing must not burn the API slot. The
	// pacing sleep is registered only once the call is really going
	// out: deferring it before this check would hold the slot for a
	// second per cancelled call, and a provisioner that times out and
	// retries would stall the calls queued behind it.
	if err := ctx.Err(); err != nil {
		c.createMu.Unlock()
		return nil, err
	}
	defer func() {
		time.Sleep(time.Second)
		c.createMu.Unlock()
	}()

	req := &vserver.CreateBlockStorageInstanceRequest{
		RegionCode:                 ncloud.String(c.regionCode),
		BlockStorageName:           ncloud.String(opts.Name),
		BlockStorageSize:           ncloud.Int32(opts.SizeGiB),
		BlockStorageVolumeTypeCode: ncloud.String(opts.VolumeType),
	}
	if opts.Description != "" {
		req.BlockStorageDescription = ncloud.String(opts.Description)
	}
	// ZoneCode is omitempty in the SDK; only set it when a valid code is known.
	// Passing an empty string would serialize as "zoneCode": "" (non-nil pointer),
	// which may cause the API to reject the request.
	if opts.ZoneCode != "" {
		req.ZoneCode = ncloud.String(opts.ZoneCode)
	}

	if opts.SnapshotID != "" {
		req.BlockStorageSnapshotInstanceNo = ncloud.String(opts.SnapshotID)
	}

	resp, err := c.client.CreateBlockStorageInstance(req)
	if err == nil {
		err = checkAPIResponse("createBlockStorageInstance", resp.ReturnCode, resp.ReturnMessage)
	}
	if err != nil {
		if apiReturnCode(err) == volumeNameInUseReturnCode {
			return nil, fmt.Errorf("create block storage %q: %v: %w", opts.Name, err, ErrNameInUse)
		}
		return nil, fmt.Errorf("failed to create block storage: %w", err)
	}

	if len(resp.BlockStorageInstanceList) == 0 {
		return nil, fmt.Errorf("create block storage returned empty list")
	}

	return toVolume(resp.BlockStorageInstanceList[0]), nil
}

func (c *ncloudCloud) DeleteVolume(ctx context.Context, volumeID string) error {
	resp, err := c.client.DeleteBlockStorageInstances(&vserver.DeleteBlockStorageInstancesRequest{
		RegionCode:                 ncloud.String(c.regionCode),
		BlockStorageInstanceNoList: []*string{ncloud.String(volumeID)},
	})
	if err != nil {
		return err
	}
	return checkAPIResponse("deleteBlockStorageInstances", resp.ReturnCode, resp.ReturnMessage)
}

// listBlockStorageInstances pages through GetBlockStorageInstanceList with
// the given filters applied to every page. Pages are assumed stable in the
// API's default (creation) order; no explicit sort field is documented.
func (c *ncloudCloud) listBlockStorageInstances(configure func(*vserver.GetBlockStorageInstanceListRequest)) ([]*vserver.BlockStorageInstance, error) {
	return collectPages(func(pageNo int32) ([]*vserver.BlockStorageInstance, int32, error) {
		req := &vserver.GetBlockStorageInstanceListRequest{
			RegionCode: ncloud.String(c.regionCode),
			PageNo:     ncloud.Int32(pageNo),
			PageSize:   ncloud.Int32(listPageSize),
		}
		if configure != nil {
			configure(req)
		}
		resp, err := c.client.GetBlockStorageInstanceList(req)
		if err != nil {
			return nil, 0, err
		}
		if err := checkAPIResponse("getBlockStorageInstanceList", resp.ReturnCode, resp.ReturnMessage); err != nil {
			return nil, 0, err
		}
		return resp.BlockStorageInstanceList, ncloud.Int32Value(resp.TotalRows), nil
	})
}

func (c *ncloudCloud) GetVolumeByID(ctx context.Context, volumeID string) (*Volume, error) {
	instances, err := c.listBlockStorageInstances(func(req *vserver.GetBlockStorageInstanceListRequest) {
		req.BlockStorageInstanceNoList = []*string{ncloud.String(volumeID)}
	})
	if err != nil {
		return nil, err
	}

	for _, inst := range instances {
		if ncloud.StringValue(inst.BlockStorageInstanceNo) == volumeID {
			return toVolume(inst), nil
		}
	}

	return nil, ErrVolumeNotFound
}

func (c *ncloudCloud) GetVolumeByName(ctx context.Context, name string) (*Volume, error) {
	instances, err := c.listBlockStorageInstances(func(req *vserver.GetBlockStorageInstanceListRequest) {
		req.BlockStorageName = ncloud.String(name)
	})
	if err != nil {
		return nil, err
	}

	for _, inst := range instances {
		if ncloud.StringValue(inst.BlockStorageName) == name {
			return toVolume(inst), nil
		}
	}

	return nil, ErrVolumeNotFound
}

func (c *ncloudCloud) ListVolumes(ctx context.Context) ([]*Volume, error) {
	instances, err := c.listBlockStorageInstances(nil)
	if err != nil {
		return nil, err
	}

	var volumes []*Volume
	for _, inst := range instances {
		volumes = append(volumes, toVolume(inst))
	}
	return volumes, nil
}

func (c *ncloudCloud) AttachVolume(ctx context.Context, volumeID, serverInstanceNo string) error {
	c.attachMu.Lock()

	// The mutex serializes API calls at one per second; a caller whose
	// RPC was cancelled while queueing must not burn the API slot. The
	// pacing sleep is registered only once the call is really going
	// out: deferring it before this check would hold the slot for a
	// second per cancelled call, and a caller that times out and
	// retries would stall the calls queued behind it.
	if err := ctx.Err(); err != nil {
		c.attachMu.Unlock()
		return err
	}
	defer func() {
		time.Sleep(time.Second)
		c.attachMu.Unlock()
	}()

	resp, err := c.client.AttachBlockStorageInstance(&vserver.AttachBlockStorageInstanceRequest{
		RegionCode:             ncloud.String(c.regionCode),
		ServerInstanceNo:       ncloud.String(serverInstanceNo),
		BlockStorageInstanceNo: ncloud.String(volumeID),
	})
	if err == nil {
		err = checkAPIResponse("attachBlockStorageInstance", resp.ReturnCode, resp.ReturnMessage)
	}
	if err != nil && isAttachmentLimitError(err) {
		return fmt.Errorf("attach volume %s to server %s: %v: %w", volumeID, serverInstanceNo, err, ErrAttachmentLimitReached)
	}
	return err
}

// Ncloud API returnCodes the driver classifies. All three are answered
// with HTTP 400 (measured on the live gateway: the attach limit on
// 2026-09-18, the two name collisions on 2026-10-06).
const (
	// "Storage allocation exceeded": the server is at its block storage
	// attachment limit.
	attachmentLimitReturnCode = "1101015"
	// "The storage instance name is already in use."
	volumeNameInUseReturnCode = "1103047"
	// "The instance name is already in use." (snapshots)
	snapshotNameInUseReturnCode = "1100015"
)

// returnCodePattern finds the returnCode in the body text the SDK puts in
// a non-2xx error ("Status: 400 Bad Request, Body: {...}").
var returnCodePattern = regexp.MustCompile(`"returnCode"\s*:\s*"(\d+)"`)

// apiReturnCode extracts the Ncloud returnCode from an SDK error in either
// shape it takes: the body text of a non-2xx answer, or an apiError from
// a 2xx envelope (see checkAPIResponse). Empty when there is none.
func apiReturnCode(err error) string {
	if err == nil {
		return ""
	}
	var apiErr *apiError
	if errors.As(err, &apiErr) {
		return apiErr.code
	}
	if m := returnCodePattern.FindStringSubmatch(err.Error()); m != nil {
		return m[1]
	}
	return ""
}

func isAttachmentLimitError(err error) bool {
	return apiReturnCode(err) == attachmentLimitReturnCode
}

// httpStatusPattern matches the status the SDK writes into a non-2xx
// error: "Status: 400 Bad Request, Body: ...".
var httpStatusPattern = regexp.MustCompile(`\bStatus: ([1-5]\d\d) `)

// IsRejectedByAPI reports whether err is the API refusing a request with
// an HTTP 4xx answer. For a create that is the one failure which says the
// resource was not created. A transport error, a timeout, a 5xx answer or
// a body the SDK could not parse leaves the outcome open -- the backend
// may have created the resource -- and callers must not treat it as
// final (see the driver's createRPCError).
func IsRejectedByAPI(err error) bool {
	if err == nil {
		return false
	}
	m := httpStatusPattern.FindStringSubmatch(err.Error())
	return m != nil && m[1][0] == '4'
}

func (c *ncloudCloud) DetachVolume(ctx context.Context, volumeID string) error {
	c.detachMu.Lock()

	// The mutex serializes API calls at one per second; a caller whose
	// RPC was cancelled while queueing must not burn the API slot. The
	// pacing sleep is registered only once the call is really going
	// out: deferring it before this check would hold the slot for a
	// second per cancelled call, and a caller that times out and
	// retries would stall the calls queued behind it.
	if err := ctx.Err(); err != nil {
		c.detachMu.Unlock()
		return err
	}
	defer func() {
		time.Sleep(time.Second)
		c.detachMu.Unlock()
	}()

	resp, err := c.client.DetachBlockStorageInstances(&vserver.DetachBlockStorageInstancesRequest{
		RegionCode:                 ncloud.String(c.regionCode),
		BlockStorageInstanceNoList: []*string{ncloud.String(volumeID)},
	})
	if err != nil {
		return err
	}
	return checkAPIResponse("detachBlockStorageInstances", resp.ReturnCode, resp.ReturnMessage)
}

func (c *ncloudCloud) EnsureDetachVolume(ctx context.Context, volumeID string) (*Volume, error) {
	ctx, cancel := context.WithTimeout(ctx, apiTimeout)
	defer cancel()

	klog.V(2).Infof("EnsureDetachVolume: volumeID=%s", volumeID)

	ticker := time.NewTicker(apiInterval)
	defer ticker.Stop()

	// Check-then-wait so the first inspection happens immediately instead
	// of after a full poll interval.
	for {
		vol, err := c.GetVolumeByID(ctx, volumeID)
		switch {
		case errors.Is(err, ErrVolumeNotFound):
			return nil, ErrVolumeNotFound
		case err != nil:
			klog.Warningf("EnsureDetachVolume: GetVolumeByID error, retrying: volumeID=%s, err=%v", volumeID, err)
		case strings.ToUpper(vol.OperationCode) != OpNormal:
			klog.Warningf("EnsureDetachVolume: volume operation in progress: volumeID=%s", volumeID)
		case strings.ToUpper(vol.StatusCode) == StatusCreated:
			return vol, nil
		case strings.ToUpper(vol.StatusCode) != StatusAttached:
			klog.Warningf("EnsureDetachVolume: volume not in attached state, waiting: volumeID=%s, status=%s", volumeID, vol.StatusCode)
		default:
			if err := c.DetachVolume(ctx, volumeID); err != nil {
				return nil, err
			}
		}

		select {
		case <-ticker.C:
		case <-ctx.Done():
			return nil, fmt.Errorf("timeout waiting for volume detach: %s", volumeID)
		}
	}
}

// ResizeVolume uses changeBlockStorageInstance, the public-gateway resize
// path for KVM volumes. The seemingly better-named
// changeBlockStorageVolumeSize rejects KVM volumes outright (1153003
// "Hypervisor not supported", verified on the live API); the original
// nks-csi avoided that split only because the API it used took an
// explicit HypervisorCode parameter.
func (c *ncloudCloud) ResizeVolume(ctx context.Context, volumeID string, newSizeGiB int32) error {
	resp, err := c.client.ChangeBlockStorageInstance(&vserver.ChangeBlockStorageInstanceRequest{
		RegionCode:             ncloud.String(c.regionCode),
		BlockStorageInstanceNo: ncloud.String(volumeID),
		BlockStorageSize:       ncloud.Int32(newSizeGiB),
	})
	if err != nil {
		return err
	}
	return checkAPIResponse("changeBlockStorageInstance", resp.ReturnCode, resp.ReturnMessage)
}

// WaitForVolumeResize polls until the volume reports at least minSizeBytes
// with no operation in progress. ChangeBlockStorageInstance is asynchronous
// — it only acknowledges the request — so expansion must not be reported
// complete to the CO until the backend actually converges.
func (c *ncloudCloud) WaitForVolumeResize(ctx context.Context, volumeID string, minSizeBytes int64) error {
	ctx, cancel := context.WithTimeout(ctx, apiTimeout)
	defer cancel()

	ticker := time.NewTicker(apiInterval)
	defer ticker.Stop()

	for {
		vol, err := c.GetVolumeByID(ctx, volumeID)
		switch {
		case errors.Is(err, ErrVolumeNotFound):
			return fmt.Errorf("volume %s disappeared while waiting for resize: %w", volumeID, ErrVolumeNotFound)
		case err != nil:
			klog.Warningf("WaitForVolumeResize: GetVolumeByID error, retrying: volumeID=%s, err=%v", volumeID, err)
		case strings.ToUpper(vol.OperationCode) == OpNormal && vol.SizeBytes >= minSizeBytes:
			return nil
		}

		select {
		case <-ticker.C:
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for volume %s to resize", volumeID)
		}
	}
}

func (c *ncloudCloud) WaitForVolumeStatus(ctx context.Context, volumeID string, desiredStatuses []string) error {
	ctx, cancel := context.WithTimeout(ctx, apiTimeout)
	defer cancel()

	ticker := time.NewTicker(apiInterval)
	defer ticker.Stop()

	// Check-then-wait so the first inspection happens immediately instead
	// of after a full poll interval.
	for {
		vol, err := c.GetVolumeByID(ctx, volumeID)
		switch {
		case errors.Is(err, ErrVolumeNotFound):
			return fmt.Errorf("volume %s not found: %w", volumeID, ErrVolumeNotFound)
		case err != nil:
			klog.Warningf("WaitForVolumeStatus: GetVolumeByID error, retrying: volumeID=%s, err=%v", volumeID, err)
		case strings.ToUpper(vol.OperationCode) != OpNormal:
			klog.Warningf("WaitForVolumeStatus: volume operation in progress: volumeID=%s", volumeID)
		default:
			for _, s := range desiredStatuses {
				if strings.ToUpper(vol.StatusCode) == s {
					return nil
				}
			}
		}

		select {
		case <-ticker.C:
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for volume status: %s", volumeID)
		}
	}
}

func (c *ncloudCloud) CreateSnapshot(ctx context.Context, opts *SnapshotOptions) (*Snapshot, error) {
	resp, err := c.client.CreateBlockStorageSnapshotInstance(&vserver.CreateBlockStorageSnapshotInstanceRequest{
		RegionCode:                      ncloud.String(c.regionCode),
		BlockStorageSnapshotName:        ncloud.String(opts.Name),
		BlockStorageSnapshotDescription: ncloud.String(opts.Description),
		OriginalBlockStorageInstanceNo:  ncloud.String(opts.SourceVolumeID),
	})
	if err == nil {
		err = checkAPIResponse("createBlockStorageSnapshotInstance", resp.ReturnCode, resp.ReturnMessage)
	}
	if err != nil {
		if apiReturnCode(err) == snapshotNameInUseReturnCode {
			return nil, fmt.Errorf("create snapshot %q: %v: %w", opts.Name, err, ErrNameInUse)
		}
		return nil, fmt.Errorf("failed to create snapshot: %w", err)
	}

	if len(resp.BlockStorageSnapshotInstanceList) == 0 {
		return nil, fmt.Errorf("create snapshot returned empty list")
	}

	return toSnapshot(resp.BlockStorageSnapshotInstanceList[0])
}

func (c *ncloudCloud) DeleteSnapshot(ctx context.Context, snapshotID string) error {
	resp, err := c.client.DeleteBlockStorageSnapshotInstances(&vserver.DeleteBlockStorageSnapshotInstancesRequest{
		RegionCode:                         ncloud.String(c.regionCode),
		BlockStorageSnapshotInstanceNoList: []*string{ncloud.String(snapshotID)},
	})
	if err != nil {
		return err
	}
	return checkAPIResponse("deleteBlockStorageSnapshotInstances", resp.ReturnCode, resp.ReturnMessage)
}

// listSnapshotInstances pages through GetBlockStorageSnapshotInstanceList
// with the given filters applied to every page. Pages are assumed stable in
// the API's default (creation) order; no explicit sort field is documented.
func (c *ncloudCloud) listSnapshotInstances(configure func(*vserver.GetBlockStorageSnapshotInstanceListRequest)) ([]*vserver.BlockStorageSnapshotInstance, error) {
	return collectPages(func(pageNo int32) ([]*vserver.BlockStorageSnapshotInstance, int32, error) {
		req := &vserver.GetBlockStorageSnapshotInstanceListRequest{
			RegionCode: ncloud.String(c.regionCode),
			PageNo:     ncloud.Int32(pageNo),
			PageSize:   ncloud.Int32(listPageSize),
		}
		if configure != nil {
			configure(req)
		}
		resp, err := c.client.GetBlockStorageSnapshotInstanceList(req)
		if err != nil {
			return nil, 0, err
		}
		if err := checkAPIResponse("getBlockStorageSnapshotInstanceList", resp.ReturnCode, resp.ReturnMessage); err != nil {
			return nil, 0, err
		}
		return resp.BlockStorageSnapshotInstanceList, ncloud.Int32Value(resp.TotalRows), nil
	})
}

func (c *ncloudCloud) GetSnapshotByID(ctx context.Context, snapshotID string) (*Snapshot, error) {
	instances, err := c.listSnapshotInstances(func(req *vserver.GetBlockStorageSnapshotInstanceListRequest) {
		req.BlockStorageSnapshotInstanceNoList = []*string{ncloud.String(snapshotID)}
	})
	if err != nil {
		return nil, err
	}

	for _, inst := range instances {
		if ncloud.StringValue(inst.BlockStorageSnapshotInstanceNo) == snapshotID {
			return toSnapshot(inst)
		}
	}

	return nil, ErrSnapshotNotFound
}

func (c *ncloudCloud) GetSnapshotByName(ctx context.Context, name string) (*Snapshot, error) {
	instances, err := c.listSnapshotInstances(func(req *vserver.GetBlockStorageSnapshotInstanceListRequest) {
		req.BlockStorageSnapshotName = ncloud.String(name)
	})
	if err != nil {
		return nil, err
	}

	for _, inst := range instances {
		if ncloud.StringValue(inst.BlockStorageSnapshotName) == name {
			return toSnapshot(inst)
		}
	}

	return nil, ErrSnapshotNotFound
}

func (c *ncloudCloud) GetSnapshotsByVolumeID(ctx context.Context, volumeID string) ([]*Snapshot, error) {
	instances, err := c.listSnapshotInstances(func(req *vserver.GetBlockStorageSnapshotInstanceListRequest) {
		req.OriginalBlockStorageInstanceNoList = []*string{ncloud.String(volumeID)}
	})
	if err != nil {
		return nil, err
	}

	return toSnapshots(instances)
}

func (c *ncloudCloud) ListSnapshots(ctx context.Context) ([]*Snapshot, error) {
	instances, err := c.listSnapshotInstances(nil)
	if err != nil {
		return nil, err
	}

	return toSnapshots(instances)
}

func toSnapshots(instances []*vserver.BlockStorageSnapshotInstance) ([]*Snapshot, error) {
	var snapshots []*Snapshot
	for _, inst := range instances {
		snap, err := toSnapshot(inst)
		if err != nil {
			return nil, err
		}
		snapshots = append(snapshots, snap)
	}
	return snapshots, nil
}

func (c *ncloudCloud) GetServerInstance(ctx context.Context, serverInstanceNo string) (*ServerInstance, error) {
	resp, err := c.client.GetServerInstanceDetail(&vserver.GetServerInstanceDetailRequest{
		ServerInstanceNo: ncloud.String(serverInstanceNo),
		RegionCode:       ncloud.String(c.regionCode),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get server instance: %w", err)
	}
	if err := checkAPIResponse("getServerInstanceDetail", resp.ReturnCode, resp.ReturnMessage); err != nil {
		return nil, err
	}

	if len(resp.ServerInstanceList) == 0 || resp.ServerInstanceList[0].ZoneCode == nil {
		return nil, fmt.Errorf("%w or zone code unavailable: %s", ErrServerInstanceNotFound, serverInstanceNo)
	}

	inst := resp.ServerInstanceList[0]
	si := &ServerInstance{
		ServerInstanceNo: serverInstanceNo,
		ZoneCode:         ncloud.StringValue(inst.ZoneCode),
		RegionCode:       ncloud.StringValue(inst.RegionCode),
	}

	if inst.ServerSpecCode != nil {
		specResp, err := c.client.GetServerSpecDetail(&vserver.GetServerSpecDetailRequest{
			ServerSpecCode: inst.ServerSpecCode,
			RegionCode:     ncloud.String(c.regionCode),
		})
		// A failed lookup is not "the spec reports no limit": swallowing it
		// would hand the caller a zero limit that reads as a definitive
		// answer, and the node startup would skip its retries and blame the
		// spec instead of the outage. Callers that do not need the limit
		// (the publish path only classifies a missing node) ignore it.
		if err != nil {
			return nil, fmt.Errorf("failed to get server spec %s for instance %s: %w",
				ncloud.StringValue(inst.ServerSpecCode), serverInstanceNo, err)
		}
		if err := checkAPIResponse("getServerSpecDetail", specResp.ReturnCode, specResp.ReturnMessage); err != nil {
			return nil, fmt.Errorf("server spec %s for instance %s: %w",
				ncloud.StringValue(inst.ServerSpecCode), serverInstanceNo, err)
		}
		if len(specResp.ServerSpecList) > 0 && specResp.ServerSpecList[0].BlockStorageMaxCount != nil {
			si.MaxBlockStorageCount = ncloud.Int32Value(specResp.ServerSpecList[0].BlockStorageMaxCount)
		}
	}

	return si, nil
}

func toVolume(inst *vserver.BlockStorageInstance) *Volume {
	return &Volume{
		ID:               ncloud.StringValue(inst.BlockStorageInstanceNo),
		Name:             ncloud.StringValue(inst.BlockStorageName),
		SizeBytes:        ncloud.Int64Value(inst.BlockStorageSize),
		StatusCode:       blockStorageStatusCode(inst),
		OperationCode:    blockStorageOperationCode(inst),
		VolumeTypeCode:   blockStorageVolumeTypeCode(inst),
		ServerInstanceNo: ncloud.StringValue(inst.ServerInstanceNo),
		RegionCode:       ncloud.StringValue(inst.RegionCode),
		ZoneCode:         ncloud.StringValue(inst.ZoneCode),
	}
}

func blockStorageStatusCode(inst *vserver.BlockStorageInstance) string {
	if inst.BlockStorageInstanceStatus != nil && inst.BlockStorageInstanceStatus.Code != nil {
		return ncloud.StringValue(inst.BlockStorageInstanceStatus.Code)
	}
	return ""
}

func blockStorageOperationCode(inst *vserver.BlockStorageInstance) string {
	if inst.BlockStorageInstanceOperation != nil && inst.BlockStorageInstanceOperation.Code != nil {
		return ncloud.StringValue(inst.BlockStorageInstanceOperation.Code)
	}
	return ""
}

func blockStorageVolumeTypeCode(inst *vserver.BlockStorageInstance) string {
	if inst.BlockStorageVolumeType != nil && inst.BlockStorageVolumeType.Code != nil {
		return ncloud.StringValue(inst.BlockStorageVolumeType.Code)
	}
	return ""
}

func toSnapshot(inst *vserver.BlockStorageSnapshotInstance) (*Snapshot, error) {
	createDateStr := ncloud.StringValue(inst.CreateDate)
	var createdAt time.Time
	if createDateStr != "" {
		var err error
		createdAt, err = time.Parse(time.RFC3339, createDateStr)
		if err != nil {
			createdAt, err = time.Parse("2006-01-02T15:04:05-0700", createDateStr)
			if err != nil {
				// An unparseable date on one snapshot must not fail whole
				// list/lookup calls; degrade to the zero time instead.
				klog.Warningf("failed to parse create date %q of snapshot %s(%s); using zero time",
					createDateStr, ncloud.StringValue(inst.BlockStorageSnapshotName),
					ncloud.StringValue(inst.BlockStorageSnapshotInstanceNo))
				createdAt = time.Time{}
			}
		}
	}

	statusCode := snapshotStatusCode(inst)
	operationCode := snapshotOperationCode(inst)
	readyToUse := statusCode == "CREAT" && operationCode == "NULL"

	return &Snapshot{
		ID:             ncloud.StringValue(inst.BlockStorageSnapshotInstanceNo),
		Name:           ncloud.StringValue(inst.BlockStorageSnapshotName),
		SourceVolumeID: ncloud.StringValue(inst.OriginalBlockStorageInstanceNo),
		SizeBytes:      ncloud.Int64Value(inst.BlockStorageSnapshotVolumeSize),
		StatusCode:     statusCode,
		OperationCode:  operationCode,
		CreatedAt:      createdAt,
		ReadyToUse:     readyToUse,
	}, nil
}

func snapshotStatusCode(inst *vserver.BlockStorageSnapshotInstance) string {
	if inst.BlockStorageSnapshotInstanceStatus != nil && inst.BlockStorageSnapshotInstanceStatus.Code != nil {
		return ncloud.StringValue(inst.BlockStorageSnapshotInstanceStatus.Code)
	}
	return ""
}

func snapshotOperationCode(inst *vserver.BlockStorageSnapshotInstance) string {
	if inst.BlockStorageSnapshotInstanceOperation != nil && inst.BlockStorageSnapshotInstanceOperation.Code != nil {
		return ncloud.StringValue(inst.BlockStorageSnapshotInstanceOperation.Code)
	}
	return ""
}
