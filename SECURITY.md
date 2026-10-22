# Security Policy

## Supported versions

Fixes go to the most recent release. There is no long-term support branch and
older tags are not patched: upgrade to the latest release first, then report
if the problem is still there.

## Reporting a vulnerability

Report privately through GitHub, not in a public issue:

**[Open a draft security advisory](https://github.com/NaverCloudPlatform/ncloud-block-storage-csi-driver/security/advisories/new)**
— or use *Security → Advisories → Report a vulnerability* on this repository.

Only the maintainers can see a report filed this way, and it stays private
until a fix is released.

Helpful in a report: the driver image (`kubectl -n kube-system get deploy
ncloud-block-storage-csi-controller -o jsonpath='{.spec.template.spec.containers[*].image}'`),
the chart version (`helm -n kube-system list`), and the smallest StorageClass
and claim that show the problem. Leave API keys, server instance numbers and
cluster identifiers out of it.

You will get an acknowledgement that the report was received, and an update
when it is triaged. If a fix is needed, the advisory is where it is
coordinated and published.

## Already documented, not a finding

These are known properties of the design, described in the README, and a
report about them will be closed as such:

- **One credential serves the whole cluster.** The controller's API key
  creates, attaches, resizes, snapshots and deletes block storage for every
  namespace. Give the node DaemonSet a separate least-privilege key with
  `credentials.node.secretName` — it only reads the server instance and spec
  at startup — and restrict access to the Secrets. See
  [Credential scope](README.md#known-limitations).
- **Deleting a PersistentVolume deletes the block storage.** A dynamically
  provisioned volume is removed from the Ncloud account when its claim is
  deleted under the default `Delete` reclaim policy; `Retain` is the
  protection, as with Kubernetes' reclaim policies in general.
- **The node plugin runs privileged and uses `hostNetwork`.** Mounting block
  devices into pod directories needs a privileged container, and the node
  plugin reaches the instance metadata service over the host network to
  learn which server it is running on. Its health endpoints bind to the node
  loopback interface only.
- **Volumes are formatted on first use.** A device that the driver finds
  unformatted is formatted during `NodeStageVolume`. The driver never
  re-formats a device that already carries a filesystem, and the original
  driver's `noformat` attribute — which could force a destructive re-format
  on every stage — is not carried over. See
  [Known limitations](README.md#known-limitations).
- **The zone table is fixed at build time.** A zone the driver does not know
  fails the node plugin at startup rather than advertising a topology value
  the kubelet would reject. Supporting a new zone is a driver release. See
  [Known limitations](README.md#known-limitations).
