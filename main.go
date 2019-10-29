        // Note that when we finally process the item from the workqueue, we might see a newer version
package main

import (
        "flag"
        "fmt"
        "time"

        batch_v1 "k8s.io/api/batch/v1"
        v1 "k8s.io/api/core/v1"

        "k8s.io/apimachinery/pkg/fields"
        "k8s.io/apimachinery/pkg/util/runtime"
        "k8s.io/apimachinery/pkg/util/wait"
        "k8s.io/client-go/tools/cache"
        "k8s.io/client-go/util/workqueue"
        "k8s.io/klog"
)

var (
        // Register values for command-line flag parsing, by default we add a value
        // In order to interact with this, it is only required to add the pertinent flag when starting the process (e.g. -namespaceJob another-namespace)
        namespaceJobPtr            = flag.String("namespaceJob", "paas-infra-cephfs", "Namespace where new Jobs/Pods are being created")
        saInitPermissionsCephfsPtr = flag.String("saInitPermissionsCephfs", "cephfs-init-permissions-jobs", "Indicates the serviceAccount which will create the jobs")
        nodeSelectorJobPtr         = flag.String("nodeSelectorJob", "role=standard", "Selects the dedicated node where the jobs should land")
)

const (
        // Define a maximum number of retries for handling errors
        handleErrRetry = 10

        // Number of reties of a job when creating it in order not to have an infinite loop
        numberJobRetries = int32(6)

        // Indicates the prefix name to set to the new jobs.
        prefixJobCreation = "init-perms-"

        // PV annotations
        annotationPVInitPermissionPtr             = "init-permission-cephfs-volumes.cern.ch/init-permission"
        annotationPVPermissionFailedPtr           = "init-permission-cephfs-volumes.cern.ch/init-permission-failed"
        annotationPVPermissionBeingInitializedPtr = "permissions-being-initialized"
        annotationPVInitializedPermissionPtr      = "permissions-initialized"
        errorPVGrantPermPtr                       = "init-permission-cephfs-volumes.cern.ch/error-granting-permissions"
        okPVGrantPermPtr                          = "init-permission-cephfs-volumes.cern.ch/ok-granting-permissions"
        annotationPVPodRestartedPtr               = "permissions-not-initialized-pod-restarted-several-times"
        annotationPVPodSucceededPtr               = "job-created-correctly"

        // Annotation added by the OPA into the PV. If the value is "true", it triggers this controller which grants non-root users permissions on new volumes
        // https://gitlab.cern.ch/paas-tools/infrastructure/cephfs-csi-deployment/merge_requests/14/diffs#78974f0b4b977b71e9739855193caa5832b6f852_8_23
        annotationPVtriggersInitPermissionPtr = "init-permission-volumes.cern.ch/init-permission"

        // Set label on Job, this will filter jobs only with this label to avoid checking multiple times in different functions
        // We integrate this label during the creation of the job
        labelStorageClassInitPermissionPtr = "init-permission-volumes.cern.ch/init-permission"
)

type ControllerPV struct {
        indexer  cache.Indexer
        queue    workqueue.RateLimitingInterface
        informer cache.Controller
        work     chan interface{}
}

func NewControllerPV(queue workqueue.RateLimitingInterface, indexer cache.Indexer, informer cache.Controller, work chan interface{}) *ControllerPV {
        return &ControllerPV{
                informer: informer,
                indexer:  indexer,
                queue:    queue,
                work:     work,
        }
}

func (c *ControllerPV) processNextPV() bool {
        // Wait until there is a new item in the working queue
        key, quit := c.queue.Get()
        if quit {
                return false
        }
        // Tell the queue that we are done with processing this key. This unblocks the key for other workers
        // This allows safe parallel processing because two PersistentVolume with the same key are never processed in
        // parallel.
        defer c.queue.Done(key)

        // Invoke the method containing the business logic
        err := c.watchEventPVs(key.(string))
        // Handle the error if something went wrong during the execution of the business logic
        c.handleErrPV(err, key)
        return true
}

// watchEventPVs is the business logic of the PV controller.
// watch for creation, deletion, update events of PVs
func (c *ControllerPV) watchEventPVs(key string) error {

        obj, exists, err := c.indexer.GetByKey(key)
        if err != nil {
                klog.Errorf("ERROR: Fetching object with key %s from store failed with %v", key, err)
                return err
        }
        if !exists {
                klog.Infof("INFO: PersistentVolume %s does not exist anymore \n", key)
        } else {
                volumeName := obj.(*v1.PersistentVolume).GetName()

                // Get the volume object to get and set annotations
                volumeObj := obj.(*v1.PersistentVolume)

                // Check if PV has the annotation to be initialized.
                if volumeObj.ObjectMeta.Annotations[annotationPVtriggersInitPermissionPtr] == "true" {

                        // Add Jobs to the queue when specific annotations are not set in the PV
                        if volumeObj.ObjectMeta.Annotations[annotationPVInitPermissionPtr] != annotationPVPermissionBeingInitializedPtr &&
                                volumeObj.ObjectMeta.Annotations[annotationPVInitPermissionPtr] != annotationPVInitializedPermissionPtr &&
                                volumeObj.ObjectMeta.Annotations[annotationPVInitPermissionPtr] != annotationPVPodRestartedPtr {

                                klog.Infof("INFO: Annotating volume: %s with annotation: %s: %s \n", volumeName, annotationPVInitPermissionPtr, annotationPVPermissionBeingInitializedPtr)

                                // Patch the PV to annotate that is being initialized
                                err := setAnnotationsPV(volumeName, annotationPVInitPermissionPtr, annotationPVPermissionBeingInitializedPtr)
                                if err != nil {
                                        klog.Errorf("ERROR: patching PV %s", err)
                                        return err
                                }

                                klog.Infof("INFO: Volume: %s annotated with annotation: %s : %s \n", volumeName, annotationPVInitPermissionPtr, annotationPVPermissionBeingInitializedPtr)

                                // It makes the JobCreationQueue work in parallel with the rest of the queues.
                                // When the condition success and we receive a PV, the worker starts an asynchronous job creation of the JobCreationQueue elements.
                                // This avoids waiting for the JobCreationQueue to initialize the PVs and handle the queuePVs elements faster.
                                nameJob := fmt.Sprintf("Job-%s", volumeName)
                                JobsCreationQueue <- job{nameJob, volumeName}
                        }
                }
        }
        return nil
}

func (c *ControllerPV) handleErrPV(err error, key interface{}) {
        if err == nil {
                // This ensures that future processing of updates for this key is not delayed because of
                // an outdated error history.
                c.queue.Forget(key)
                return
        }

        // This controller retries handleErrRetry times if something goes wrong. After that, it stops trying.
        if c.queue.NumRequeues(key) < handleErrRetry {
                klog.Infof("Error syncing PersistentVolume %v: %v", key, err)

                // Re-enqueue the key rate limited. Based on the rate limiter on the
                // queue and the re-enqueue history, the key will be processed later again.
                c.queue.AddRateLimited(key)
                return
        }

        c.queue.Forget(key)

        // Report to an external entity that, even after several retries, we could not successfully process this key
        runtime.HandleError(err)
        klog.Infof("INFO: Dropping PersistentVolume %q out of the queue: %v", key, err)
}

// Start controller to check for PVs
func (c *ControllerPV) RunPV(threadiness int, stopCh chan struct{}) {
        defer runtime.HandleCrash()

        // Let the workers stop when we are done
        defer c.queue.ShutDown()
        klog.Info("INFO: Starting PersistentVolume controller")

        go c.informer.Run(stopCh)

        // Wait for all involved caches to be synced, before processing items from the queue is started
        if !cache.WaitForCacheSync(stopCh, c.informer.HasSynced) {
                runtime.HandleError(fmt.Errorf("Timed out waiting for caches to sync"))
                return
        }

        for i := 0; i < threadiness; i++ {
                go wait.Until(c.runWorkerPV, time.Second, stopCh)
        }
        <-stopCh
        klog.Info("INFO: Stopping PersistentVolume controller")
}

func (c *ControllerPV) runWorkerPV() {
        for c.processNextPV() {
        }
}

func main() {

        // Initializing global flags for klog
        klog.InitFlags(nil)

        // Parses the command line into the defined flags
        flag.Parse()

        // Get global kubeclient
        clientset := kubeclient.kubeclient

        // Create the PersistentVolume watcher
        PersistentVolumeListWatcher := cache.NewListWatchFromClient(clientset.CoreV1().RESTClient(), "persistentvolumes", v1.NamespaceAll, fields.Everything())

        // Create the workqueue for the PV watcher
        queuePVs := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())

        // Bind the workqueue to a cache with the help of an informer. This way we make sure that
        // whenever the cache is updated, the PersistentVolume key is added to the workqueue.
        // Note that when we finally process the item from the workqueue, we might see a newer version
        // of the PersistentVolume than the version which was responsible for triggering the update.
        indexerPV, informerPV := cache.NewIndexerInformer(PersistentVolumeListWatcher, &v1.PersistentVolume{}, 0, cache.ResourceEventHandlerFuncs{
                AddFunc: func(obj interface{}) {
                        key, err := cache.MetaNamespaceKeyFunc(obj)
                        if err == nil {
                                queuePVs.Add(key)
                        }
                },
                UpdateFunc: func(old interface{}, new interface{}) {
                        key, err := cache.MetaNamespaceKeyFunc(new)
                        if err == nil {
                                queuePVs.Add(key)
                        }
                },
                DeleteFunc: func(obj interface{}) {
                        // IndexerInformer uses a delta queue, therefore for deletes we have to use this
                        // key function.
                        key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
                        if err == nil {
                                queuePVs.Add(key)
                        }
                },
        }, cache.Indexers{})

        // Make a PV queue work
        workPV := make(chan interface{}, 100)

        controller := NewControllerPV(queuePVs, indexerPV, informerPV, workPV)

        // Now let's start the controller for the PV
        stopPV := make(chan struct{})
        defer close(stopPV)
        go controller.RunPV(1, stopPV)

        // Creates the Job watcher
        JobListWatcher := cache.NewListWatchFromClient(clientset.BatchV1().RESTClient(), "jobs", *namespaceJobPtr, fields.Everything())

        // Creates the workqueue for the Jobs watcher
        queueJobs := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())

        // Bind the workqueue to a cache with the help of an informer. This way we make sure that
        // whenever the cache is updated, the Job key is added to the workqueue.
        // Note that when we finally process the item from the workqueue, we might see a newer version
        // of the Job than the version which was responsible for triggering the update.
        indexerJob, informerJob := cache.NewIndexerInformer(JobListWatcher, &batch_v1.Job{}, 0, cache.ResourceEventHandlerFuncs{
                AddFunc: func(obj interface{}) {
                        key, err := cache.MetaNamespaceKeyFunc(obj)
                        if err == nil {
                                queueJobs.Add(key)
                        }
                },
                UpdateFunc: func(old interface{}, new interface{}) {
                        key, err := cache.MetaNamespaceKeyFunc(new)
                        if err == nil {
                                queueJobs.Add(key)
                        }
                },
                DeleteFunc: func(obj interface{}) {
                        // IndexerInformer uses a delta queue, therefore for deletes we have to use this
                        // key function.
                        key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
                        if err == nil {
                                queueJobs.Add(key)
                        }
                },
        }, cache.Indexers{})

        ControllerJob := NewControllerJob(queueJobs, indexerJob, informerJob)

        // Now let's start the controller for the Jobs
        stopJob := make(chan struct{})
        defer close(stopJob)
        go ControllerJob.RunJob(1, stopJob)

        // Start worker loop. It creates jobs from the JobsCreationQueue
        go createJobsFromQueue()

        // Close Jobs finalized
        defer close(JobsCreationQueue)

