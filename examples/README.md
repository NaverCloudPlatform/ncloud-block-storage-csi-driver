# Examples

Ready-to-apply manifests for the common workflows described in the main
README:

| File | What it shows |
|------|---------------|
| `pvc.yaml` | Dynamic provisioning with the default (ext4) StorageClass |
| `pod.yaml` | Mounting the volume in a pod |
| `storageclass-xfs.yaml` | A dedicated StorageClass for XFS volumes |
| `snapshot.yaml` | Taking a VolumeSnapshot of a PVC |
| `pvc-restore.yaml` | Restoring a new volume from a snapshot |

```bash
kubectl apply -f examples/pvc.yaml -f examples/pod.yaml
```
