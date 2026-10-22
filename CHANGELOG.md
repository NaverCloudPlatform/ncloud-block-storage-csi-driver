# Changelog

## unreleased

## v1.0.0 - 2026.10.22

Initial open-source release of the Ncloud Block Storage CSI Driver.

### Features

- CSI Spec v1.12.0 compliant
- Volume lifecycle management (create, delete, attach, detach)
- Volume expansion (ext4 and xfs), online — mounted volumes grow
  without a pod restart
- Volume snapshots (create, delete, restore)
- Raw block volume support
- Topology-aware provisioning (standard `topology.kubernetes.io/zone`
  key; the legacy `zoneNo` key is still advertised for existing PVs)
- Support for CB1, CB2, FB1, FB2 volume types
  (the XEN-generation type names `SSD`/`HDD` are rejected as
  unsupported)
- ext4 (default) and xfs filesystems; xfs via a StorageClass with
  `csi.storage.k8s.io/fstype: xfs`, host-OS-based fallback for
  statically provisioned volumes without a fsType
- `MAX_VOLUMES_PER_NODE` override for the per-node attachment limit
  (`maxVolumesPerNode` chart value)

### Behaviour

- Volume expansion is advertised as `ONLINE`; mounted volumes grow
  without a pod restart, and online growth is verified end to end
- csi-provisioner runs with `--strict-topology=true`
- CreateVolume idempotency validates capacity range, volume type, and
  requisite topology; CreateSnapshot validates the source volume;
  ValidateVolumeCapabilities verifies the requested capabilities
- `ControllerPublishVolume` answers `FAILED_PRECONDITION` when the volume
  is attached to another node instead of detaching it there and taking
  it: releasing an attachment is the CO's job, and the steal could pull a
  mounted device out from under a live node
- Volumes report the `blockStorageVolumeType` VolumeContext key; no
  `blockStorageDiskType` key is set (its `SSD`/`HDD` vocabulary is
  XEN-generation)
- The `type` StorageClass parameter is matched case-insensitively by name
  as well as by value, so `Type:` provisions what it asks for instead of
  silently falling back to the default; conflicting spellings of the same
  key are rejected
- Unknown StorageClass parameter names are rejected instead of silently
  ignored, so a typo or an unimplemented parameter fails loudly. Keys
  Kubernetes owns pass through untouched: the reserved
  `csi.storage.k8s.io/*` namespace, the deprecated `csiXxxSecret*`
  spellings, and a bare `fstype` — external-provisioner still reads that
  one and forwards it
- The legacy `zoneNo` topology key is advertised alongside
  `topology.kubernetes.io/zone`, and `regionNo` when the `legacyRegionNo`
  chart value is set, so PVs whose nodeAffinity requires those labels
  stay schedulable on new nodes. Both legacy keys are deprecated
- Volumes and snapshots the driver creates carry the description
  `Created by Ncloud Block Storage CSI driver`, so the console tells
  them apart from resources made by hand
- `ListVolumes` and volume health monitoring (controller and node
  `VOLUME_CONDITION`) are not implemented — no external-health-monitor
  consumer ships; a detach requested by Kubernetes always proceeds
  (there is no `Node.Status.VolumesInUse` pre-check)

### Robustness and deployment

- Go toolchain 1.26.10 and golang.org/x/net v0.60.0, which carry the
  October 2026 Go security fixes (GO-2026-6605 through GO-2026-6617 and
  GO-2026-6809) that govulncheck reports against the previous versions
- CSI error codes aligned with the spec: missing volumes/snapshots map to
  `NotFound` across publish/expand/snapshot RPCs; read-only publish
  requests return `InvalidArgument`; a size outside the volume type's
  range returns `OUT_OF_RANGE` from CreateVolume as it already did from
  ControllerExpandVolume; boundary inputs never panic the driver
  (recovery interceptor as a defense line)
- A 2xx response the SDK could not parse is an error, not an empty
  result. The vendored client skips unmarshalling a body containing
  `{"error"` and returns no error with it, so a list call came back empty
  — which the driver read as "no such volume", answering a delete or an
  unpublish with success and creating a duplicate for an existing name.
  The envelope's `returnCode`, which every real response carries,
  separates the two. A `responseError` envelope under a 2xx status, which
  the client does unwrap, is caught by the same check because its
  `returnCode` is not `"0"` — the code of every successful response,
  mutation endpoints included
- The node RPCs that touch the device or filesystem (stage, unstage,
  publish, unpublish, expand) take a per-volume guard and answer
  `ABORTED` for a concurrent request. The kubelet retries after two
  minutes, the same deadline the mount and resize helpers run under, so a
  retry could reach `mkfs`, `mount` or `resize2fs` while the first call
  was still inside it
- The controller RPCs take a per-resource guard as well (the volume or
  snapshot ID; the Ncloud name for a create) and answer `ABORTED` for a concurrent
  request. ControllerPublishVolume can outlast the attacher's 60-second
  deadline while it waits for a volume to finish creating or for an
  attach to settle, and an unpublish that ran beside it before the API
  showed the attach read "not attached" and succeeded — leaving an
  attachment nothing would detach. The guard lives in one process: during
  a rolling update the old pod's calls finish beside the new pod's, out
  of its sight
- CreateVolume and CreateSnapshot no longer answer `INTERNAL` — which the
  provisioner and the snapshotter treat as final and stop tracking — when
  they do not know whether the resource was created. A transport error or
  timeout, a 5xx answer and a failed lookup answer `UNAVAILABLE`; a name
  the API reports as taken (`1103047` for volumes, `1100015` for
  snapshots, measured on the live gateway) returns the resource behind it,
  or `ABORTED` until it is readable; a failed re-read of a resource that
  was just created is answered from the create response. Only an explicit
  rejection by the API (HTTP 4xx) stays `INTERNAL` — and a volume the API
  reports in a zone outside the driver's table, a driver inconsistency no
  retry resolves, answered with the volume ID for the operator. With a
  final error, a PVC or VolumeSnapshot deleted before the next retry left
  the backend resource behind for good
- An attach whose RPC deadline passed, or was cancelled, while it waited
  its turn behind other attaches answers `DEADLINE_EXCEEDED` or
  `CANCELLED` at once instead of spending two more API calls on the node
  lookup and answering `INTERNAL`; an attach that did go out keeps the
  classification of its own error, even after the deadline
- The node DaemonSet gives the driver and registrar containers a startup
  probe (360 s). The node plugin resolves its instance, zone and
  attachment limit before it serves, and the health endpoints open only
  once it does; with a slow Ncloud API those lookups outlast the liveness
  budget, and the container was restarted before it could log what it
  was waiting for
- The StorageClass parameter check passes through exactly the keys
  Kubernetes owns — the `csi.storage.k8s.io/` namespace and the eight
  deprecated `csi…SecretName`/`csi…SecretNamespace` spellings
  external-provisioner v6.1.0 still reads — instead of anything beginning
  with `csi`, so a typo such as `csitype: FB1` is rejected rather than
  silently ignored
- `--defaultFsType` is validated at startup (`ext4`, `xfs` or empty);
  anything else used to surface as a failed `mkfs` on the first stage of
  a volume without an fsType. `regionCode` is upper-cased before use, and
  `apiUrl` tolerates a trailing slash — it is the gateway origin, with the
  `/vserver/v2` service path appended by the driver, which the flag help,
  the chart and the README now say
- The runtime image carries `org.opencontainers.image.*` labels (source,
  version, revision, licenses)
- A shutdown signal that arrives before the gRPC server exists is no
  longer swallowed: the driver records it and declines to start, instead
  of serving until the kubelet escalates to `SIGKILL`
- ControllerUnpublishVolume answers `ABORTED` while an attach is still
  running instead of reporting success. The volume reads as created, not
  attached, for the ~27 seconds the attach takes, and a success there
  lets the CO drop the attachment record while the attach completes
  behind it — leaving a device nothing would detach
- ControllerPublishVolume waits for a volume to finish being created
  before attaching. The API refuses an attach on a volume still building
  (HTTP 400, returnCode 1103026), which cost a failed call, a node
  lookup, and the attacher's backoff on every slow create
- `regionCode` is validated against the node's zone at startup: a region
  that is not the zone's fails the node plugin instead of surfacing later
  as volumes that cannot be found or attached
- The device-discovery wait is a chart value
  (`node.deviceDiscoveryTimeout`); it was previously reachable only by
  editing the binary's arguments, though the README described it as an
  operator knob
- Size messages name an exact quantity. Rendering to one decimal made
  the 16,380 GiB ceiling and the 16 TiB request it rejects both read
  "16Ti", so the rejection looked self-contradictory
- CSI names longer than the 30-character Ncloud limit are shortened
  deterministically (21-character prefix + 8-hex-digit SHA-256 tag)
  instead of rejected, for volumes and snapshots alike; names within the
  limit — everything the shipped chart generates — pass through unchanged
- NodeGetVolumeStats verifies the volume actually backs the queried path
  and returns `NotFound` otherwise (an unmounted path previously answered
  success with an abnormal volume condition)
- ControllerPublishVolume returns `NotFound` for a nonexistent node;
  NodeExpandVolume treats `capacity_range` as optional per spec
- The csi-sanity conformance suite (csi-test v5.5.0) runs as part of
  `make test`, in-process against stateful in-memory fakes — with both
  mount and raw-block test volumes and the node attach-limit spec
  (`ControllerPublishVolume` answers `RESOURCE_EXHAUSTED` when the
  backend reports the node cannot take another attachment; the live
  Ncloud API error — HTTP 400, returnCode 1101015 "Storage allocation
  exceeded", measured at the 20-attachment limit — is classified into
  the same path)
- `values.schema.json` validates the Helm values surface: typos in value
  names and out-of-range or misspelled values (`storageClass.fsType`, ports, …)
  fail at render time instead of being silently ignored
- The node DaemonSet can run with a separate least-privilege credential
  Secret (`credentials.node.secretName`) — the node plugin only reads
  the server instance/spec at startup and never mutates resources
- Driver container log verbosity is a chart value (`controller.logLevel`,
  `node.logLevel`, default 2); the CSI sidecars log at a fixed `--v=2`
  (livenessprobe sets no verbosity flag)
- The node DaemonSet's health endpoints (livenessprobe, node-driver-
  registrar) bind to the node loopback interface only; the DaemonSet runs
  with hostNetwork, and they previously listened on every node interface
- A resize that is still converging when the wait deadline passes is
  reported as `ABORTED` (retryable) instead of `Internal`
- Controller RPCs validate the volume ID format: malformed IDs are
  treated as already-deleted (delete, unpublish) or `NotFound` (publish,
  expand, validate, snapshot) instead of surfacing API rejections as
  `Internal` on every retry
- `NodePublishVolume` refuses (FailedPrecondition) to bind-mount a
  staging path that is not actually mounted; `ControllerPublishVolume`'s
  already-attached fast path answers `ABORTED` when a detach is in flight
  (the volume keeps its attached status while the device is on its way
  out) but still succeeds during operations that leave the attachment
  alone, such as a snapshot
- A `findmnt` failure other than "exited 1 with no output" is an error
  again instead of "not mounted" — the fail-open reading could skip an
  unmount and detach a still-mounted disk
- A volume that disappears while `DeleteVolume` waits out the detach is
  treated as deleted
- `IsMounted` canonicalizes the request path before comparing with
  findmnt's canonical output — a symlinked kubelet root-dir (or a
  trailing slash) no longer reads a mounted path as "not mounted"
- Snapshot RPCs hold the volume RPCs' malformed-ID contract (delete =
  already gone, list = empty); `ListSnapshots` honors both filters when
  both are set; an idempotent `CreateVolume` retry validates a claimed
  snapshot source instead of echoing it; a volume without a reported
  zone omits the zone topology segment instead of publishing an empty
  one that no node can satisfy
- Mount-layer failures surface as `Internal` with context instead of
  leaking as `Unknown`; device-identity stat failures are no longer
  folded into `NotFound`; `NodeExpandVolume` strips findmnt's bind
  subpath like the statistics path; the formatted-check read is bounded
  by the command deadline
- Rate-limited cloud calls check for a cancelled request after acquiring
  the serialization slot instead of burning an API call for a caller
  that already gave up
- The volume type table named the wrong media: the Common types (`CB1`,
  `CB2`) are the SSD tier whose IOPS scale with the volume (100-16,000)
  and the Fixed types (`FB1`, `FB2`) are the HDD tier with a flat 500
  IOPS — the documentation had those reversed, so a user picking `FB1`
  for performance got the opposite. The size ceiling is 16,380 GiB, not
  a round 16 TiB; a 16 TiB request rounded past the API limit and is now
  rejected with the real number
- Zone handling covers the financial (`FKR-1`, `FKR-2`) and government
  (`KRS-1`) platforms, and a zone the driver does not know is refused
  instead of guessed: the node plugin fails to start on such a node, and a
  StorageClass or scheduler topology naming one is rejected with
  `INVALID_ARGUMENT` before any API call. Advertising an unmapped value
  made the kubelet reject the plugin with a topology value collision
- `--topology-zone-format` and the `topologyZoneFormat` chart value are
  removed; `topology.kubernetes.io/zone` always carries the zone number,
  which is what the Ncloud cloud controller manager labels nodes with and
  what existing PVs with the legacy `zoneNo` affinity record. The `zoneCode`
  format collided with that label wherever the CCM runs, and on a cluster
  without it the number works just as well
- The IMDS client acquires its session token once and reuses it (node
  startup made one token request per value before), falls back to a
  token-less GET when the token endpoint fails — the metadata service
  serves the values either way, so a broken token endpoint no longer
  stops the node plugin — and renews a token the service stops accepting
  once before giving up
- Chart name-typed values render through `quote` (a name like `"true"`
  or `"a: b"` stayed a string), the CI render gates catch block-style
  RBAC rules and non-localhost health bindings, the public workflow
  checks out with `persist-credentials: false`, and the builder image is
  digest-pinned with `GOTOOLCHAIN=local`
- The node DaemonSet mounts no ServiceAccount token
  (`automountServiceAccountToken: false`), and chart-injected `apiUrl` /
  `regionCode` values are rendered with proper quoting
- `NodeStageVolume` (and block-mode publish) waits for the attached
  device's `by-id` symlink for up to 60 seconds
  (`--device-discovery-timeout`) instead of failing on the first miss —
  the hypervisor hot-plug lands well after the attach API returns
- The per-node attachment limit is never guessed: `MAX_VOLUMES_PER_NODE`
  skips the server spec lookup, an API-reported limit is used as is, and
  startup fails (after brief retries) when neither is available — the
  previous silent default of 20 is removed
- The `noformat` volume attribute is ignored: devices that already carry
  a filesystem are never re-formatted, and unformatted devices are always
  formatted
- Ncloud list APIs paginate explicitly (the API default returns a single
  row); one unparseable snapshot date no longer fails whole list calls
- Faster attach/detach/unmount paths: status polls check immediately
  instead of waiting a full poll interval first
- Helm chart coexistence: `snapshotController.enabled`,
  `volumeSnapshotClass.*` and `storageClass.create` toggles for clusters
  with existing snapshot infrastructure; CSIDriver states
  `fsGroupPolicy: ReadWriteOnceWithFSType` explicitly; provisioner RBAC
  matches the official external-provisioner v6.1.0 manifest (PV `patch`)
- Runtime base image bumped to alpine 3.24 (3.20 reached end of life)
  and pinned by digest
- XFS volumes are formatted with the xfsprogs LTS 5.15 feature preset so
  filesystems stay mountable on the node kernel families NKS ships (xfsprogs
  7.x defaults enable superblock features newer kernels-only), and are
  mounted with `nouuid` so a snapshot-restored volume can mount on the
  same node as its origin (both verified end to end)
- The unused Kubernetes API client was removed from the driver; the
  binary no longer requires in-cluster credentials, which also enables
  out-of-cluster runs (e.g. csi-sanity)
- Read-only publishes are actually read-only: the runtime image ships
  util-linux `mount`/`umount` (busybox mount skips the read-only remount
  for bind mounts), guarded by a CI smoke test
- Volume IDs are validated as numeric instance numbers before any device
  path construction (a crafted static-PV volumeHandle could previously
  resolve to an arbitrary node device)
- Every Ncloud API HTTP round trip carries a timeout, and external
  filesystem commands run under a deadline with forced-unmount
  escalation, so a hung backend or a stalled command does not hold a
  request open indefinitely (a child blocked in uninterruptible disk I/O
  is the limit: the deadline fires, but the kernel releases the process
  only when the I/O returns)
- blkid runs in probe mode with a readability cross-check so an
  unreadable device is never mistaken for an empty disk and formatted
- `ControllerExpandVolume` waits for the asynchronous backend resize to
  converge before reporting success; limit-only capacity ranges round
  down to the 10 GiB allocation unit instead of overshooting the limit
- The already-satisfied expand fast path also waits for an in-flight
  backend operation to settle before reporting success (a retried expand
  could otherwise report success while the backend resize was still
  running and could still fail)
- Volume types are validated uniformly against the supported set
  (CB1/CB2/FB1/FB2): requests naming anything else — including the
  XEN-generation names `SSD`/`HDD` — are rejected as unsupported, and
  expanding a volume whose API-reported type is outside the set (an
  existing XEN volume) fails fast with a clear error. This KVM-only
  driver can neither create nor attach XEN volumes. There is no
  `SSD`→CB1 alias: a StorageClass with `type: SSD` must be recreated with
  `CB1`
- Chart: RBAC matches the official sidecar manifests, leader-election
  leases are namespace-scoped, driver containers carry resources,
  scheduling knobs (`controller.*`, `node.*`) and the node hostNetwork
  health ports are configurable, and `kubeVersion` relaxes to >= 1.25
