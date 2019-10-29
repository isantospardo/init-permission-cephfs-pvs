package main

import (
        "fmt"
        "time"

        batch_v1 "k8s.io/api/batch/v1"
        meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
        "k8s.io/apimachinery/pkg/util/runtime"
        "k8s.io/apimachinery/pkg/util/wait"
        "k8s.io/client-go/tools/cache"
        "k8s.io/client-go/util/workqueue"
        "k8s.io/klog"
)

// Global channel for the creation of a Jobs queue
var JobsCreationQueue = make(chan job)

type job struct {
        nameJob    string
        volumeName string
}

type ControllerJob struct {
        indexer  cache.Indexer
        queue    workqueue.RateLimitingInterface
        informer cache.Controller
}

func NewControllerJob(queue workqueue.RateLimitingInterface, indexer cache.Indexer, informer cache.Controller) *ControllerJob {
        return &ControllerJob{
                informer: informer,
                indexer:  indexer,
                queue:    queue,
        }
}

func (c *ControllerJob) processNextJob() bool {
        // Wait until there is a new item in the working queue
        key, quit := c.queue.Get()
        if quit {
                return false
        }
        // Tell the queue that we are done with processing this key. This unblocks the key for other workers
        // This allows safe parallel processing because two Jobs with the same key are never processed in
        // parallel.
        defer c.queue.Done(key)

        // Invoke the method containing the business logic
        err := c.watchEventJobs(key.(string))
        // Handle the error if something went wrong during the execution of the business logic
        c.handleErrJob(err, key)
        return true
}

// watchEventJobs is the business logic of the Job controller.
// watch for creation, deletion, update events of Jobs
func (c *ControllerJob) watchEventJobs(key string) error {
        obj, exists, err := c.indexer.GetByKey(key)
        if err != nil {
                klog.Errorf("ERROR: Fetching object with key %s from store failed with %v", key, err)
                return err
        }
        if !exists {
                klog.Infof("INFO: Job %s does not exist anymore\n", key)
        } else {
                // Check only per filtered Jobs, filtering them here we avoid to check multiple times in different functions
                // the label was integrated during the creation of the job
                if obj.(*batch_v1.Job).GetLabels()[labelStorageClassInitPermissionPtr] == "true" {
                        err = cleanupGrantPermission(obj.(*batch_v1.Job))
                        if err != nil {
                                klog.Errorf("ERROR: Cleaning up Jobs and Permissions %s", err)
                                return err
                        }
                }
        }
        return nil
}

// handleErrJob checks if an error happened and makes sure we will retry later with a limit of retries.
func (c *ControllerJob) handleErrJob(err error, key interface{}) {
        if err == nil {
                // This ensures that future processing of updates for this key is not delayed because of
                // an outdated error history.
                c.queue.Forget(key)
                return
        }

        // This controller retries handleErrRetry times if something goes wrong. After that, it stops trying.
        if c.queue.NumRequeues(key) < handleErrRetry {
                klog.Infof("Error syncing Job %s ", key)

                // Re-enqueue the key rate limited. Based on the rate limiter on the
                // queue and the re-enqueue history, the key will be processed later again.
                c.queue.AddRateLimited(key)
                return
        }

        c.queue.Forget(key)
        // Report to an external entity that, even after several retries, we could not successfully process this key
        runtime.HandleError(err)
        klog.Infof("Dropping Job %q out of the queue: %v", key, err)
}

// Start controller to watch for Jobs
func (c *ControllerJob) RunJob(threadiness int, stopCh chan struct{}) {
        defer runtime.HandleCrash()

        // Let the workers stop when we are done
        defer c.queue.ShutDown()
        klog.Info("INFO: Starting Job controller")

        go c.informer.Run(stopCh)

        // Wait for all involved caches to be synced, before processing items from the queue is started
        if !cache.WaitForCacheSync(stopCh, c.informer.HasSynced) {
                runtime.HandleError(fmt.Errorf("Timed out waiting for caches to sync"))
                return
        }

        for i := 0; i < threadiness; i++ {
                go wait.Until(c.runWorkerJob, time.Second, stopCh)
        }

        <-stopCh
        klog.Info("INFO: Stopping Job controller")
}

// createJobsFromQueue is a worker loop which creates jobs from the JobsCreationQueue
func createJobsFromQueue() {

        // For each of the Jobs loaded in the queue, a Job will be created in charge of granting the permissions
        for j := range JobsCreationQueue {
                err := CreateJob(j)
                if err != nil {
                        klog.Errorf("ERROR: creating Job %s", err)
                }
        }
}

func (c *ControllerJob) runWorkerJob() {
        for c.processNextJob() {
        }
}

// Once the Job has been successfully created and the Volume has been granted with the right permissions, delete the Job created
func cleanupGrantPermission(job *batch_v1.Job) error {

        // Get global kubeclient
        clientset := kubeclient.kubeclient

        volumeName := job.GetLabels()["volumeName"]
        jobName := job.GetName()
        jobNamespace := job.GetNamespace()

        // Get the volume object to get and set annotations
        volumeObj, err := clientset.CoreV1().PersistentVolumes().Get(volumeName, meta_v1.GetOptions{})
        if err != nil {
                klog.Errorf("ERROR: getting volume object %s ", err)
                return err
        }

        // Annotates the PV with `permission-initialized`, this marks the PV for its consequent deletion
        if volumeObj.ObjectMeta.Annotations[annotationPVInitPermissionPtr] == annotationPVPermissionBeingInitializedPtr ||
                volumeObj.ObjectMeta.Annotations[annotationPVPermissionFailedPtr] != annotationPVPodRestartedPtr {

                // Check if volumeUID of the Job is equal to the PV one, this is done because there would be a scenario where the PV
                // is deleted and re-created with the same name, but different UID, with this we make sure it will grant permissions to the right one
                if job.GetLabels()["volumeUID"] == string(volumeObj.GetUID()) {
                        if job.Status.Succeeded == 1 {

                                klog.Infof("INFO: Permissions granted to volume: %s in namespace: %s\n", volumeName, jobNamespace)

                                // Patch the PV to annotate the registered values
                                klog.Infof("INFO: Ready to cleanup job %s in namespace %s", jobName, jobNamespace)

                                // Deletes the job
                                err = deleteJob(jobName, jobNamespace)
                                if err != nil {
                                        klog.Errorf("ERROR: deleting job %s in namespace %s ", jobName, jobNamespace)
                                        return err
                                }
                                // Annotates the PV and specify that the PV has been initialized correctly and the permissions have been granted
                                err := setAnnotationsPV(volumeName, annotationPVInitPermissionPtr, annotationPVInitializedPermissionPtr)
                                if err != nil {
                                        klog.Errorf("ERROR: patching PV %s", err)
                                        return err
                                }

                                // In order to prevent a loop creation of jobs, a numberJobRetries was set, in the case this number of retries
                                // is reached, the job will be deleted and an annotation will be set in order to add the reason of the failure
                        } else if job.Status.Failed >= numberJobRetries {
                                klog.Errorf("ERROR: Permissions not granted to volume: %s in namespace: %s\n", volumeName, jobNamespace)

                                // Annotates the PV and specify that the PV has not been initialized correctly and the permissions have not been granted
                                err := setAnnotationsPV(volumeName, annotationPVPermissionFailedPtr, fmt.Sprintf("%s-job:%s", annotationPVPodRestartedPtr, jobName))
                                if err != nil {
                                        klog.Errorf("ERROR: patching PV %s", err)
                                        return err
                                }

                                // Deletes the job
                                klog.Infof("Ready to cleanup job %s in namespace %s", jobName, jobNamespace)
                                err = deleteJob(jobName, jobNamespace)
                                if err != nil {
                                        klog.Errorf("ERROR deleting job %s in namespace %s ", jobName, jobNamespace)
                                        return err
                                }
                        }
                        // If non of the last conditions are reached, we wait for the next event to happen on that job.
                        // It can be that the job still didn't succeed or the numberJobRetries was not reached yet.
                }
        }

