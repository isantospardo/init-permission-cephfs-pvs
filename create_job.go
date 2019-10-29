~
package main

import (
        "fmt"
        "strings"

        batch_v1 "k8s.io/api/batch/v1"
        v1 "k8s.io/api/core/v1"
        meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
        "k8s.io/apimachinery/pkg/types"
        "k8s.io/klog"
)

// We need to create this job to get an abstraction layer.
// We need this abstraction layer to check the stats of the creation. Once a pod succeeds, the job also succeeds,
func InitialiseJobObject(volumeName, jobNamespace, saInitPermissionsCephfs, namespaceGroupID, pvUserID, pvUserKey string, volumeUID types.UID, pvOptions *v1.CSIPersistentVolumeSource, numberJobRetries int32) *batch_v1.Job {

        // Run pod as privileged, we need it to mount the volume into the pod in order to be initialized,
        // by default the pods do not have this possibility as OpenShift apps run as `non-root` users
        isPrivileged := true
        runAsUser := int64(0)

        // Selects the dedicated node where the jobs should land. We define this as a configurable parameter.
        nodeSelectorKey := strings.Split(*nodeSelectorJobPtr, "=")[0]
        nodeSelectorValue := strings.Split(*nodeSelectorJobPtr, "=")[1]

        return &batch_v1.Job{
                TypeMeta: meta_v1.TypeMeta{Kind: "Job"},
                ObjectMeta: meta_v1.ObjectMeta{
                        Name:      fmt.Sprintf("%s%s", prefixJobCreation, volumeName),
                        Namespace: jobNamespace,
                        // Sets the garbage collector to mark the child objet dependent of the parent object, this makes sure that when the parent object is deleted,
                        // the child object is also deleted.
                        OwnerReferences: []meta_v1.OwnerReference{meta_v1.OwnerReference{
                                Name:       volumeName,
                                Kind:       "PersistentVolume",
                                UID:        volumeUID,
                                APIVersion: "v1",
                        }},
                },
                Spec: batch_v1.JobSpec{
                        // BackoffLimit has to be the same than numberJobRetries.
                        // job.Status.Failed increases for each failure of the init-permission pod until it reaches BackoffLimit.
                        // At that point, Kubernetes gives up trying to run the job and we'll consider the job failed.
                        BackoffLimit: &numberJobRetries,
                        Template: v1.PodTemplateSpec{
                                // Set labelStorageClassInitPermissionPtr label to filter the pods
                                // Set the volumeName and volumeUID labels to identify where the volume comes from
                                ObjectMeta: meta_v1.ObjectMeta{
                                        Labels: map[string]string{
                                                labelStorageClassInitPermissionPtr: "true",
                                                "volumeName":                       volumeName,
                                                "volumeUID":                        fmt.Sprintf("%s", volumeUID)},
                                },

                                Spec: v1.PodSpec{
                                        RestartPolicy:      "Never",
                                        ServiceAccountName: saInitPermissionsCephfs,
                                        NodeSelector: map[string]string{
                                                nodeSelectorKey: nodeSelectorValue,
                                        },
                                        Containers: []v1.Container{
                                                {
                                                        Name:  "alpine",
                                                        Image: "alpine",
                                                        Command: []string{
                                                                "/bin/sh",
                                                        },
                                                        // List of vars to be taken by the execution of granting permissions
                                                        Env: []v1.EnvVar{
                                                                {
                                                                        Name:  "CEPHFS_MONITORS",
                                                                        Value: pvOptions.VolumeAttributes["monitors"],
                                                                },
                                                                {
                                                                        Name:  "CEPHFS_ROOTPATH",
                                                                        Value: pvOptions.VolumeAttributes["rootPath"],
                                                                },
                                                                {
                                                                        Name:  "CEPHFS_USERID",
                                                                        Value: pvUserID,
                                                                },
                                                                {
                                                                        Name:  "CEPHFS_USERKEY",
                                                                        Value: pvUserKey,
                                                                },
                                                                {
                                                                        Name:  "CEPHFS_SET_OWNER_UID",
                                                                        Value: "root",
                                                                },
                                                                {
                                                                        Name:  "CEPHFS_SET_OWNER_GID",
                                                                        Value: namespaceGroupID,
                                                                },
                                                        },
                                                        // Executes granting permissions
                                                        Args: []string{
                                                                "-c",
                                                                `set -e; mount -t ceph "${CEPHFS_MONITORS}:${CEPHFS_ROOTPATH}" -o name="${CEPHFS_USERID}",secret="${CEPHFS_USERKEY}" /mnt; chmod 777 /mnt; chown "${CEPHFS_SET_OWNER_UID}:${CEPHFS_SET_OWNER_GID}" /mnt; umount /mnt`,
                                                        },
                                                        // Runs the pod as privileged
                                                        SecurityContext: &v1.SecurityContext{
                                                                Privileged: &isPrivileged,
                                                                RunAsUser:  &runAsUser,
                                                        },
                                                },
                                        },
                                },
                        },
                },
        }
}

// CreateJob adds the logic to create a Job, gets the userID and the userKey and
// adds annotations to the PV, indicating that the job has been created correctly
func CreateJob(j job) error {

        // Get global kubeclient
        clientset := kubeclient.kubeclient

        // Get the volume object
        volumeObj, err := clientset.CoreV1().PersistentVolumes().Get(j.volumeName, meta_v1.GetOptions{})
        if err != nil {
                klog.Errorf("ERROR: getting volume object %s ", err)
                return err
        }

        // TODO: https://gitlab.cern.ch/paas-tools/storage/init-permission-cephfs-volumes/issues/8
        namespaceGroupID := "0"

        // Get the userID and userKey from the secret in PV nodeStageSecretRef, we need this to collect the authentication information to mount CephFS volumes.
        // It only works if the job runs in the same namespace where we store secrets
        pvOptions := volumeObj.Spec.CSI
        pvUserKeys, err := clientset.CoreV1().Secrets(pvOptions.NodeStageSecretRef.Namespace).Get(pvOptions.NodeStageSecretRef.Name, meta_v1.GetOptions{})
        if err != nil {
                klog.Errorf("ERROR: getting nodeStageSecretRef of the PV %s, probably the job is not running in the same namespace where we store secrets", err)
                return err
        }

        job := InitialiseJobObject(j.volumeName, *namespaceJobPtr, *saInitPermissionsCephfsPtr, namespaceGroupID, string(pvUserKeys.Data["userID"]), string(pvUserKeys.Data["userKey"]), volumeObj.ObjectMeta.UID, pvOptions, numberJobRetries)

        klog.Infof("INFO: Creating: Job %s in Namespace: %s \n", job.GetName(), *namespaceJobPtr)

        // Creates the job
        _, err = clientset.BatchV1().Jobs(*namespaceJobPtr).Create(job)
        if err != nil {
                klog.Errorf("ERROR: creating Job %s in namespace %s, %v \n", job.GetName(), *namespaceJobPtr, err)
                err := setAnnotationsPV(j.volumeName, errorPVGrantPermPtr, err.Error())
                if err != nil {
                        klog.Errorf("ERROR: patching PV %s", err)
                        return err
                }
                return err
        }

        // Sets the annotation to specify the job has been created successfully
        klog.Infof("INFO: Job %s created correctly in namespace: %s \n", job.GetName(), job.GetNamespace())
        err = setAnnotationsPV(j.volumeName, okPVGrantPermPtr, annotationPVPodSucceededPtr)
        if err != nil {
                klog.Errorf("ERROR: patching PV %s", err)
                return err
        }
        return nil
}
