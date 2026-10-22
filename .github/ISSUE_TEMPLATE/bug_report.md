---
name: Bug report
about: Report a problem with the driver
labels: bug
---

**What happened**

**What you expected to happen**

**How to reproduce it (as minimally and precisely as possible)**

**Environment**
- Driver version (image tag):
- Kubernetes version (`kubectl version`):
- Cluster type (NKS / self-managed on Ncloud VPC):
- Node OS / kernel (`kubectl get nodes -o wide`):
- Install method and values overrides (helm):

**Relevant logs**

<!-- controller: kubectl -n kube-system logs deploy/ncloud-block-storage-csi-controller -c ncloud-block-storage-csi-driver -->
<!-- node:       kubectl -n kube-system logs ds/ncloud-block-storage-csi-node -c ncloud-block-storage-csi-driver -->
<!-- Please redact credentials and account-specific identifiers. -->
