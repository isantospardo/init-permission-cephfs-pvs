package main

import (
        meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
        "k8s.io/klog"
)

// Deletes the Job earlier created when granting permissions has run successfully or not in order to clean it up
func deleteJob(jobName, jobNamespace string) error {
        // Get global kubeclient
        clientset := kubeclient.kubeclient

        // Set the deletion propagation to background, Kubernetes deletes the owner object
        // immediately and the garbage collector then deletes the dependents in the background
        deletePolicy := meta_v1.DeletePropagationBackground
        err := clientset.BatchV1().Jobs(jobNamespace).Delete(jobName, &meta_v1.DeleteOptions{PropagationPolicy: &deletePolicy})
        if err != nil {
                klog.Infof("ERROR deleting job %s in namespace %s ", jobName, jobNamespace)
                return err
        }
        return nil
}
