# Ncloud Block Storage CSI Driver

A Container Storage Interface ([CSI](https://github.com/container-storage-interface/spec)) driver for Naver Cloud Platform Block Storage. It provisions Ncloud VPC block storage volumes for PersistentVolumeClaims, attaches them to the nodes that use them, grows them online and takes snapshots of them.

## Features

- **Dynamic provisioning**: a block storage volume per PersistentVolumeClaim, in the zone of the node that uses it (`topology.kubernetes.io/zone`)
- **Attach and detach**, with the per-node attachment limit taken from the server spec
- **Online expansion** of ext4 and xfs volumes; mounted volumes grow without a pod restart
- **Snapshots**: create, delete and restore through the VolumeSnapshot API
- **Volume types** `CB1`, `CB2`, `FB1` and `FB2`; **filesystems** ext4 (default) and xfs
- **Raw block volumes** (`volumeMode: Block`) and volume usage statistics (`NodeGetVolumeStats`)
- CSI spec v1.12.0

Not supported: `ReadWriteMany` and `ReadOnlyMany` access modes, cloning a claim from another claim, and XEN-generation volumes. See [Known limitations](#known-limitations).

## Volume types

| Type | Family | Media | IOPS | Min size | Max size |
|------|--------|-------|------|----------|----------|
| `CB1` | Common Block Storage (default) | SSD | 100–16,000, scales with volume size | 10 GiB | 16,380 GiB |
| `CB2` | Common Block Storage | SSD | 100–16,000, scales with volume size | 10 GiB | 16,380 GiB |
| `FB1` | Fixed Block Storage | HDD | 500, flat | 100 GiB | 16,380 GiB |
| `FB2` | Fixed Block Storage | HDD | 500, flat | 100 GiB | 16,380 GiB |

The media follows the family, not the number: both Common types are SSD and both Fixed types are HDD, so `FB1` is the throughput-oriented HDD tier rather than a faster `CB1`. The figures come from `getBlockStorageVolumeTypeList` on the KR region; the KVM type guide documents `CB1` and `FB1`, while `CB2`/`FB2` appear in the pricing table and are accepted by the API with the same limits. The maximum is 16,380 GiB, not a round 16 TiB: a 16 TiB request rounds up past the ceiling and is rejected.

Volumes are provisioned in 10 GiB steps: a requested size that is not a multiple of 10 GiB is rounded up (a 15 Gi claim gets a 20 GiB volume), and a claim without a size gets 20 GiB. Volume and snapshot names are limited to 30 characters on the Ncloud API. The chart's sidecar arguments keep generated names (`pvc-…`, `snapshot-…`) within that limit so console names match the Kubernetes names; a longer name is not rejected but shortened deterministically (a 21-character prefix plus an 8-hex-digit SHA-256 tag), so keep a custom `--volume-name-prefix` short.

Only the four types above are accepted. The XEN-generation names `SSD` and `HDD` are rejected: this KVM-only driver cannot create XEN volumes.

## Installing with Helm

### Prerequisites

- Kubernetes v1.25 or newer (the lower bound of the bundled external-snapshotter v8.x components; the chart is validated on v1.34+)
- A Kubernetes cluster on Ncloud VPC and Ncloud API credentials (access key and secret key)
- On the financial or government platform, that platform's API gateway in `apiUrl` (see [Helm values](#helm-values)); the default is the public gateway
- VolumeSnapshot CRDs. `helm install` installs them from the chart's `crds/`; as with every Helm chart, `helm upgrade` does not upgrade them and `helm uninstall` leaves them in place, so upgrade them with `kubectl apply` when moving to a newer chart. Set `snapshotController.enabled=false` when the cluster already runs its own snapshot-controller

> **Note:** Helm 4 installs CRDs with server-side apply. When the VolumeSnapshot CRDs already exist under `kubectl apply` ownership (the usual state on NKS clusters, where the platform ships them), the installation fails with a field-manager conflict on `.spec.versions`; add `--skip-crds` to `helm install` in that case. Helm 3 skips existing CRDs and needs no flag.

### 1. Build and push the driver image

A public container image is not published yet, so build and push the image to a registry your cluster can pull from (for example Ncloud Container Registry):

```bash
make image push REGISTRY=<your-registry>
```

Every `helm install` below requires `--set image.csi_plugin=<your-registry>/ncloud-block-storage-csi-driver:v1.0.0`.

### 2. Provide Ncloud API credentials

**Option A: an existing Secret** (recommended for production)

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: ncloud-csi-creds
  namespace: kube-system
stringData:
  accessKey: "your-access-key"
  secretKey: "your-secret-key"
```

```bash
kubectl apply -f secret.yaml
helm install ncloud-block-storage-csi-driver deploy/chart/ncloud-block-storage-csi-driver \
  --namespace kube-system \
  --set image.csi_plugin=<your-registry>/ncloud-block-storage-csi-driver:v1.0.0 \
  --set regionCode=KR
```

**Option B: let the chart create the Secret**

```bash
helm install ncloud-block-storage-csi-driver deploy/chart/ncloud-block-storage-csi-driver \
  --namespace kube-system \
  --set image.csi_plugin=<your-registry>/ncloud-block-storage-csi-driver:v1.0.0 \
  --set regionCode=KR \
  --set credentials.create=true \
  --set credentials.accessKey=your-access-key \
  --set credentials.secretKey=your-secret-key
```

The Secret is read from the release namespace. Credentials passed with `--set` end up in your shell history and in the Helm release Secret, which is why Option A is recommended.

The chart creates a StorageClass and a VolumeSnapshotClass, both named `ncloud-block-storage`, and marks both as the cluster default.

### 3. Verify the installation

```bash
kubectl get pods -n kube-system -l 'app in (ncloud-block-storage-csi-controller, ncloud-block-storage-csi-node)'
kubectl get csidrivers
kubectl get storageclass
```

## Usage

[`examples/`](examples/) has ready-to-apply manifests for each of the workflows below.

A PersistentVolumeClaim on the default StorageClass:

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: my-pvc
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 10Gi
  storageClassName: ncloud-block-storage
```

A snapshot of it, and a new claim restored from the snapshot:

```yaml
apiVersion: snapshot.storage.k8s.io/v1
kind: VolumeSnapshot
metadata:
  name: my-snapshot
spec:
  volumeSnapshotClassName: ncloud-block-storage
  source:
    persistentVolumeClaimName: my-pvc
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: my-pvc-restored
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 10Gi
  storageClassName: ncloud-block-storage
  dataSource:
    name: my-snapshot
    kind: VolumeSnapshot
    apiGroup: snapshot.storage.k8s.io
```

## StorageClass parameters

| Parameter | Meaning | Default |
|-----------|---------|---------|
| `type` | Volume type: `CB1`, `CB2`, `FB1` or `FB2`. Key and value are case-insensitive | `CB1` |
| `csi.storage.k8s.io/fstype` | Filesystem: `ext4` or `xfs` | `ext4` |

Unknown parameter names are rejected at `CreateVolume` with `INVALID_ARGUMENT` rather than ignored, so a typo (`tpye:`) or a parameter this driver does not implement (`encrypted:`) fails loudly instead of silently provisioning the default type. Keys Kubernetes owns are exempt: the `csi.storage.k8s.io/` namespace, which external-provisioner validates itself, and the deprecated spellings it still accepts (a bare `fstype`, and the camel-case secret references such as `csiProvisionerSecretName`), so StorageClasses written for the original driver keep working.

Dynamically provisioned volumes default to ext4. The chart's StorageClass pins `csi.storage.k8s.io/fstype: ext4` explicitly so that every PersistentVolume records a filesystem type: with the default `fsGroupPolicy` (`ReadWriteOnceWithFSType`), kubelet applies `fsGroup` ownership only when the PV fsType is set, and an empty fsType would break volume writes for non-root pods. For xfs (the convention on RHEL-family node images such as Rocky or NAVIX), create a second StorageClass with `csi.storage.k8s.io/fstype: xfs`; [`examples/storageclass-xfs.yaml`](examples/storageclass-xfs.yaml) is one. Statically provisioned PersistentVolumes that omit `spec.csi.fsType` follow the host OS convention (xfs on RHEL-family distributions, ext4 otherwise).

## Helm values

| Value | Meaning | Default |
|-------|---------|---------|
| `image.csi_plugin` | CSI driver image (**required**) | `""` |
| `image.csi_provisioner`, `image.csi_attacher`, `image.csi_snapshotter`, `image.csi_resizer`, `image.liveness_probe`, `image.csi_node_driver_registrar`, `image.snapshot_controller` | Sidecar and snapshot-controller images, for private mirrors | `registry.k8s.io/sig-storage/…` (csi-provisioner v6.1.0, csi-attacher v4.10.0, csi-snapshotter v8.4.0, csi-resizer v2.0.0, livenessprobe v2.17.0, csi-node-driver-registrar v2.15.0, snapshot-controller v8.4.0) |
| `imagePullSecrets` | Image pull secrets attached to all chart ServiceAccounts | `[]` |
| `regionCode` | Ncloud region of the cluster (`KR`, `SGN`, `JPN`, `USWN`, `DEN`, `FKR`, `KRS`). The node plugin refuses to start when it is not the region of the node's zone | `KR` |
| `apiUrl` | API gateway origin, scheme and host only. Empty is the public gateway `https://ncloud.apigw.ntruss.com`; the financial platform uses `https://fin-ncloud.apigw.fin-ntruss.com` and the government platform `https://ncloud.apigw.gov-ntruss.com` | `""` |
| `credentials.secretName` | Secret in the release namespace holding `accessKey` and `secretKey` | `ncloud-csi-creds` |
| `credentials.create`, `credentials.accessKey`, `credentials.secretKey` | Have the chart create that Secret | `false`, `""`, `""` |
| `credentials.node.secretName`, `credentials.node.create`, `credentials.node.accessKey`, `credentials.node.secretKey` | A separate, read-only credential for the node DaemonSet (see [Known limitations](#known-limitations)); empty reuses `credentials.secretName` | `""`, `false`, `""`, `""` |
| `maxVolumesPerNode` | Per-node attachment limit override; skips the server spec lookup when set | `0` (use the API-reported limit) |
| `legacyRegionNo` | Numeric `regionNo` to advertise as a legacy node topology key, for clusters whose PersistentVolumes carry `regionNo`/`zoneNo` node affinity from an earlier driver; leave empty otherwise | `""` |
| `controller.replicas` | Controller replicas; the sidecars use leader election | `1` |
| `controller.logLevel`, `node.logLevel` | klog verbosity of the driver containers (the sidecars run at `--v=2`) | `2` |
| `controller.resources`, `node.resources` | Resources of the driver containers | requests 10m/40Mi; limits 500Mi (controller), 256Mi (node) |
| `controller.nodeSelector`, `controller.tolerations`, `controller.affinity` | Scheduling of the controller pods (`kubernetes.io/os: linux` and the critical-addon tolerations are always set) | `{}`, `[]`, `{}` |
| `node.nodeSelector`, `node.tolerations` | Scheduling of the node DaemonSet | `{}`, tolerate every `NoSchedule`/`NoExecute` taint |
| `node.deviceDiscoveryTimeout` | How long `NodeStageVolume` waits for the attached device to appear, as a quoted Go duration (`"90s"`; `"0"` fails on the first miss, passed with `--set-string`) | `""` (the binary's 60 s) |
| `node.healthPort`, `node.registrarHealthPort` | Health ports of the node DaemonSet, which runs with hostNetwork (loopback-bound); change them when another hostNetwork CSI driver uses them | `9808`, `9809` |
| `snapshotController.enabled` | Deploy the bundled snapshot-controller | `true` |
| `storageClass.create`, `storageClass.name`, `storageClass.isDefault` | The StorageClass the chart creates | `true`, `ncloud-block-storage`, `true` |
| `storageClass.type`, `storageClass.fsType`, `storageClass.reclaimPolicy`, `storageClass.volumeBindingMode`, `storageClass.allowVolumeExpansion` | Its parameters and policies | `CB1`, `ext4`, `Delete`, `WaitForFirstConsumer`, `true` |
| `volumeSnapshotClass.create`, `volumeSnapshotClass.name`, `volumeSnapshotClass.isDefault`, `volumeSnapshotClass.deletionPolicy` | The VolumeSnapshotClass the chart creates | `true`, `ncloud-block-storage`, `true`, `Delete` |

`values.yaml` documents each value in more detail. Two things to know when upgrading a release:

- `storageClass.type`, `storageClass.fsType`, `storageClass.reclaimPolicy` and `storageClass.volumeBindingMode` land in fields Kubernetes does not let an existing StorageClass change, so changing them makes `helm upgrade` fail (`updates to parameters are forbidden`). Delete the StorageClass first (existing PersistentVolumes are not affected) and upgrade, or set `storageClass.create=false` and manage the class yourself. `storageClass.isDefault` and `storageClass.allowVolumeExpansion` can be changed in place.
- The sidecar arguments are fixed in the templates: `--volume-name-uuid-length=26` and `--snapshot-name-uuid-length=21` keep generated names within the 30-character Ncloud limit, and the provisioner runs with `--timeout=60s`, `--retry-interval-start=15s`, `--extra-create-metadata` and `--strict-topology=true`, the resizer with `--handle-volume-inuse-error=false`.

## Topology

Volumes are provisioned in the zone of the node that first uses them, through the standard `topology.kubernetes.io/zone` label and the default `WaitForFirstConsumer` binding mode.

Two vocabularies name an Ncloud zone. The block storage API and the instance metadata service use *zone codes* (`KR-2`); the Ncloud cloud controller manager labels nodes with the *zone number* (`3`). No public API exposes the number, so the driver carries the table below: the node plugin advertises the number (any other value makes the kubelet reject the plugin with a topology value collision), and the controller turns the numbers it receives from the scheduler back into zone codes for the API. On a cluster without the CCM the same number is what the kubelet records from the driver, so nothing else has to be configured.

| Platform | Zones (code = number) |
|----------|----------------------|
| Public | `KR-1`=2, `KR-2`=3, `KR-3`=4, `SGN-1`=70, `SGN-4`=74, `SGN-5`=75, `JPN-1`=80, `JPN-4`=84, `JPN-5`=85, `USWN-1`=90, `DEN-1`=100 |
| Financial | `FKR-1`=110, `FKR-2`=130 |
| Government | `KRS-1`=135, plus the public `KR-1` and `KR-2` rows |

A node in a zone outside this table fails the node plugin at startup, and a StorageClass or scheduler topology naming one is rejected with `INVALID_ARGUMENT` before any API call: the driver has no number to advertise and no code to send, and guessing either produces a PV no node can satisfy. Supporting a new zone is a driver release. For PersistentVolumes whose node affinity uses the legacy `zoneNo`/`regionNo` keys of an earlier driver, the node plugin also advertises `zoneNo` (same value) and, when `legacyRegionNo` is set, `regionNo`. Both legacy keys are deprecated and will be removed in a future major version.

## Behaviour notes

- **A volume attached to another node is not taken away.** `ControllerPublishVolume` answers `FAILED_PRECONDITION` naming the node that holds the volume: releasing an attachment is the orchestrator's job (`ControllerUnpublishVolume`), and pulling a device out from under a node that still has it mounted corrupts that filesystem. In a healthy cluster the attach/detach controller unpublishes from the old node first, so this path is not reached. A volume stranded on a node that is gone is released by letting the attach/detach controller finish (taint the node `node.kubernetes.io/out-of-service`) or by detaching it in the Ncloud console.
- **Devices are formatted once.** An unformatted device is formatted with the PV's filesystem at first use; a device that already carries a filesystem is never re-formatted. Re-formatting a volume means recreating it.
- **`ListVolumes` and volume health monitoring are not implemented.** The RPC returns `UNIMPLEMENTED` and the capability is not advertised.
- **Resources carry a description.** Volumes and snapshots the driver creates carry `Created by Ncloud Block Storage CSI driver`, so the console tells them apart from resources made by hand.

## Known limitations

- **The zone table is fixed at build time.** A zone outside the table in [Topology](#topology), a new one or a customer-dedicated environment, fails the node plugin at startup rather than advertising a value the kubelet would reject. The financial and government platform rows are covered by the table only and have not been exercised end to end by this project. The same table backs the startup check on `regionCode`.
- **Expansion completes when the guest sees the new size.** The backend resizes attached volumes and `NodeExpandVolume` grows the filesystem online, but only once the hypervisor presents the new device size to the guest; until then `NodeExpandVolume` keeps retrying (`ABORTED`, by design) and a pod waiting on the expansion stays pending.
- **Snapshot-sourced volume idempotency.** When a `CreateVolume` retry finds an existing volume with the requested name, the driver cannot verify that the volume's origin matches the requested snapshot source: the pinned SDK (v1.6.27) does not expose the `blockStorageSnapshotInstanceNo` field the API guide documents on a block storage instance (a later SDK release, v1.6.31, does). The request is treated as a retry.
- **Per-node attachment limits are never guessed.** An explicit `maxVolumesPerNode` is used as is and skips the server spec lookup, which also makes it the escape hatch when the Ncloud API is unreachable at node startup. Otherwise the limit the server spec API reports is advertised, and the node plugin fails to start when neither is available: a wrong limit would stick in `CSINode.allocatable` and strand pods in `ContainerCreating` with nothing pointing at the cause.
- **Attached devices are awaited, not assumed.** After the attach API returns, the hypervisor hot-plug and the udev `by-id` symlink can take tens of seconds, so `NodeStageVolume` (and block-mode publish) polls for the device for up to 60 seconds (`node.deviceDiscoveryTimeout`) instead of failing on the first miss and waiting out kubelet's retry backoff.
- **Credential scope.** The node plugin only reads the server instance and server spec at startup; it never creates, attaches or deletes resources. `credentials.node.secretName` gives the DaemonSet a separate least-privilege key (for example a sub-account restricted to VPC Server read permissions) instead of the controller's read-write key.
- **Access modes and content sources.** Volumes are `ReadWriteOnce` only: `ReadWriteMany` and `ReadOnlyMany` claims are rejected at `CreateVolume` (`INVALID_ARGUMENT`), since block storage attaches to one server at a time. A claim can be created from a VolumeSnapshot but not cloned from another claim. A PersistentVolume with `spec.csi.readOnly: true` is rejected at attach time; make the mount read-only on the pod (`volumeMounts[].readOnly`) instead.
- **A snapshot a server image was created from cannot be deleted.** The Ncloud API refuses to delete a KVM snapshot that a server image was built from until that image is deleted. If a server image is made from a snapshot this driver created, `DeleteSnapshot` keeps failing (`INTERNAL`) and the VolumeSnapshotContent stays behind until the image is deleted, or until the content's deletion policy is switched to `Retain`, which lets it be removed with the snapshot left in place.

## Development

Go 1.26+ and Docker with buildx are required.

```bash
make compile                        # linux/amd64 binary -> bin/
make test                           # unit tests
make image REGISTRY=<registry>      # container image
make push REGISTRY=<registry>       # push it
helm lint --strict deploy/chart/ncloud-block-storage-csi-driver \
  --set image.csi_plugin=example.com/ncloud-block-storage-csi-driver:dev
```

Running the binary outside the chart, the runtime image and dependency management are described in [CONTRIBUTING.md](CONTRIBUTING.md).

## Origin

`nks-csi`, the in-house NKS driver this project descends from, started in 2019 as a fork of [digitalocean/csi-digitalocean](https://github.com/digitalocean/csi-digitalocean) v0.4.1 (Apache-2.0), was merged up to its v1.1.2 the same year, and later took individual changes from v1.2.0: volume statistics, raw block publishing, device lookup under `/dev/disk/by-id` and capability validation. This driver was restructured from that code rather than written from scratch. Nine Go files still descend from DigitalOcean-authored ones: the mount helper (`pkg/mount/mount.go`, `pkg/mount/safe_mounter.go`), the CSI server skeleton (`pkg/driver/controller.go`, `node.go`, `driver.go`, `identity.go`, `validation.go`), the capacity-range helper in `pkg/utils/util.go` and `cmd/main.go`; so do the `Makefile` and the `Dockerfile`. Each of them keeps the original copyright notice next to this project's header and states that it was modified; [CHANGELOG.md](CHANGELOG.md) describes the driver's behaviour release by release. The Ncloud API client (`pkg/cloud`), the topology, metadata and OS-detection code, the rest of the Helm chart and the tests are NAVER Cloud's own work.

In the Helm chart, the snapshot CRDs (`crds/snapshot-crd.yaml`) and the snapshot-controller Deployment and RBAC (`templates/snapshot-controller.yaml`, `templates/snapshot-sa.yaml`) are modified from [kubernetes-csi/external-snapshotter](https://github.com/kubernetes-csi/external-snapshotter) v8.4.0 (Apache-2.0), as each file says at its top.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

Apache License 2.0. See [LICENSE](LICENSE).
```
Ncloud Block Storage CSI Driver
Copyright (c) 2026-present NAVER Cloud Corp.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```
