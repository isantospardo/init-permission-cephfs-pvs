package main

import (
        "fmt"

        "k8s.io/apimachinery/pkg/types"
        "k8s.io/klog"
)

// Sets annotations to the Persistent Volume
func setAnnotationsPV(pvName, annotationKey, annotationValue string) error {
        // Get global kubeclient
        clientset := kubeclient.kubeclient

        patch := []byte(fmt.Sprintf(`{"metadata": {"annotations": {"%s": %q}}}`, annotationKey, annotationValue))
        _, err := clientset.CoreV1().PersistentVolumes().Patch(pvName, types.StrategicMergePatchType, patch)
        if err != nil {
                klog.Errorf("ERROR: patching PV %s", err)
                return err
        }
        return nil
}
