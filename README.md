# Initialize permission in CephFS volumes

This permission initializing for CephFS volumes needs to be done since the volumes provisioned by Manila only give write access to `root`, while OpenShift apps run
as `non-root` users. So with this, we make sure that an OpenShift application will be able to write to `CephFS` volumes.

We want to set permissions on the volume's root as follows for PVs to be useable by Openshift applications running with the
[default `restricted SCC`](https://docs.openshift.com/container-platform/3.11/architecture/additional_concepts/authorization.html#security-context-constraints):

* `chmod 777` so all users can create files and folders - this is the critical requirement, without apps would need to run as root to use the CephFS volumes!

This [`init-permissions` controller](https://gitlab.cern.ch/paas-tools/storage/init-permission-cephfs-volumes) watches for persistent volume events. When a PV is created,
the OPA adds an annotation whether the PV has to be initialized or not. When a PV has this [annotation](https://gitlab.cern.ch/paas-tools/infrastructure/cephfs-csi-deployment/merge_requests/14/diffs#78974f0b4b977b71e9739855193caa5832b6f852_8_23) set to `true`, the controller grants permissions to it. This happens by launching a
[Job](https://kubernetes.io/docs/concepts/workloads/controllers/jobs-run-to-completion/) that mounts the CephFS volume into a pod and
[runs `chown`/`chmod` on the volume's root](https://its.cern.ch/jira/browse/CIPAAS-543). Once this is done, the volume is unmounted
and the Job deleted. The application that requested that volume can now mount the volume and write to it with its Openshift-assigned non-root user.

## Parametrized values

In order to interact with this parametrized values, the only requirement is to add the pertinent flag during the execution (e.g. -namespaceJob another-namespace)

*Possible values:*

- namespaceJob: defaults to `paas-infra-cephfs`, namespace where the creation of the jobs and initialization of the PVs happen.
  We also need it to collect the secrets of the PVs, by default we store this information in the namespace where the `manila-provisioner` is.
  We need this secret information to mount the CephFS volumes into the job to initialize it.
  This only works if the job runs in the same namespace where we store secrets, more info in [here](https://docs.ceph.com/docs/kraken/cephfs/fstab/)
- saInitPermissionsCephfs: default to `cephfs-init-permissions-jobs`. It is described in the serviceAccount section of this document.
- nodeSelectorJob: default to `role=standard`, selects the dedicated node where the jobs should land. We split the string into a `key` and a `value`
  separated with a `=`. We do this to have a fully compatibility in future deployments.

## ServiceAccounts

- `cephfs-init-permissions-jobs`: a serviceAccount with that exact name must exist in the namespace identified by `namespaceJob`.
  in order to run the Jobs that grant permission on new volumes. The serviceAccounts must have permissions to run privileged containers.
  In practice this serviceAccount is created by the [CephFS CSI Helm deployment](https://gitlab.cern.ch/paas-tools/infrastructure/cephfs-csi-deployment).

- Service account where the controller is running in [CephFS SCC deployment](https://gitlab.cern.ch/paas-tools/infrastructure/cephfs-csi-deployment).
  This service account must be `privileged` to be able to mount volumes inside the pods created by the jobs.

## Deployment

This permission initializing for CephFS volumes is deployed with `helm` as a subchart of [CephFS csi deployment](https://gitlab.cern.ch/paas-tools/infrastructure/cephfs-csi-deployment).
The namespace used to be deployed is by default `paas-infra-cephfs`, in all the clusters.
