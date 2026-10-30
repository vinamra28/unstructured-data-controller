/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	operatorv1alpha1 "github.com/redhat-data-and-ai/unstructured-data-controller/api/v1alpha1"
	"github.com/redhat-data-and-ai/unstructured-data-controller/internal/controller/controllerutils"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/awsclienthandler"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/filestore"
	udctelemetry "github.com/redhat-data-and-ai/unstructured-data-controller/pkg/telemetry"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/unstructured"
)

const (
	// Lowercase to match controller-runtime's own metric label; see ObserveReconciler.
	DestinationSyncerControllerName = "destinationsync"
)

// DestinationSyncerReconciler reconciles a DestinationSyncer object
type DestinationSyncerReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	// Metrics records reconcile telemetry. Nil disables recording.
	Metrics   *udctelemetry.Provider
	fileStore *filestore.FileStore
}

// +kubebuilder:rbac:groups=operator.dataverse.redhat.com,namespace=unstructured-controller-namespace,resources=destinationsyncers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=operator.dataverse.redhat.com,namespace=unstructured-controller-namespace,resources=destinationsyncers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=operator.dataverse.redhat.com,namespace=unstructured-controller-namespace,resources=destinationsyncers/finalizers,verbs=update
// +kubebuilder:rbac:groups=operator.dataverse.redhat.com,namespace=unstructured-controller-namespace,resources=unstructureddatapipelines,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups="",namespace=unstructured-controller-namespace,resources=secrets,verbs=get;list;watch

func (r *DestinationSyncerReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info("reconciling", "controller", DestinationSyncerControllerName)

	isHealthy, err := IsConfigCRHealthy(ctx, r.Client, req.Namespace)
	if err != nil {
		logger.Error(err, "failed to check if ControllerConfig CR is healthy")
		return ctrl.Result{}, err
	}
	if !isHealthy {
		logger.Info("ControllerConfig CR is not ready yet, will try again in a bit ...")
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	destinationSyncCR := &operatorv1alpha1.DestinationSyncer{}
	if err := r.Get(ctx, req.NamespacedName, destinationSyncCR); err != nil {
		logger.Error(err, "failed to get DestinationSyncer CR")
		return ctrl.Result{}, err
	}

	if err := controllerutils.StatusPatch(ctx, r.Client, destinationSyncCR, func() {
		destinationSyncCR.SetWaiting()
	}); err != nil {
		logger.Error(err, "failed to update DestinationSyncer CR status")
		return ctrl.Result{}, err
	}

	fs, err := filestore.New(ctx, dataStorageDirectory, dataStorageBucket)
	if err != nil {
		if IsAWSClientNotInitializedError(err) {
			logger.Info("ControllerConfig has not initialized AWS clients yet, will try again in a bit ...")
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}
		logger.Error(err, "failed to create filestore")
		return r.handleError(ctx, destinationSyncCR, err)
	}
	r.fileStore = fs
	pipelineName, err := controllerutils.ParentPipelineNameFromOwnerReference(destinationSyncCR)
	if err != nil {
		return r.handleError(ctx, destinationSyncCR, err)
	}
	destConfig := destinationSyncCR.Spec.DestinationSyncerConfig
	if destConfig.Type != operatorv1alpha1.TypeS3 {
		return r.handleError(ctx, destinationSyncCR, fmt.Errorf("unsupported destination type: %s", destConfig.Type))
	}
	awsConfig, err := controllerutils.AWSConfigFromSecret(ctx, r.Client, destinationSyncCR.Spec.SecretRef, destinationSyncCR.Namespace, "DESTINATION_S3_")
	if err != nil {
		return r.handleError(ctx, destinationSyncCR, fmt.Errorf("failed to get destination S3 credentials: %w", err))
	}
	destinationS3Client, err := awsclienthandler.NewS3Client(ctx, awsConfig)
	if err != nil {
		return r.handleError(ctx, destinationSyncCR, fmt.Errorf("failed to create destination S3 client: %w", err))
	}

	// Two counters, because the status field and the metric answer different
	// questions and must not be conflated.
	//
	// totalFilesListed feeds Status.FilesProcessed, which is an existing
	// user-visible field: it is rendered by the `Files` printcolumn and
	// FilesProcessedChangedPredicate gates Update events on it, which
	// SourceCrawler, DocumentProcessor, ChunksGenerator and
	// VectorEmbeddingsGenerator all rely on to wake. Counting only new
	// uploads here would freeze the field once the corpus is stable and
	// silently stop waking those four, so it keeps counting the listing.
	//
	// totalFilesUploaded feeds udc.files.processed, which is new and should
	// be a true delta so it means the same thing as in every other
	// controller.
	var totalFilesListed, totalFilesUploaded int64
	for _, dep := range destinationSyncCR.Spec.DependsOn {
		destination := &unstructured.S3Destination{
			S3Client:  destinationS3Client,
			Bucket:    destConfig.S3DestinationConfig.Bucket,
			Prefix:    destConfig.S3DestinationConfig.Prefix,
			StageName: dep.Name,
		}

		inputPath := unstructured.StagePath(pipelineName, dep.Name)
		filePaths, err := r.fileStore.ListFilesInPath(ctx, inputPath)
		if err != nil {
			// Earlier stages may already have uploaded; the next attempt skips
			// those as unchanged, so the metric has to take them now or lose
			// them for good.
			r.Metrics.RecordFilesProcessed(ctx, DestinationSyncerControllerName, totalFilesUploaded)
			return r.handleError(ctx, destinationSyncCR,
				fmt.Errorf("listing files for stage %s: %w", dep.Name, err))
		}
		totalFilesListed += int64(len(filePaths))
		logger.Info("files to ingest to destination", "stage", dep.Name, "count", len(filePaths))

		// uploadedCount is the delta: files already at the destination with a
		// matching checksum are skipped. Accumulated before the error check
		// for the same reason as above.
		uploadedCount, err := destination.SyncFilesToDestination(ctx, r.fileStore, filePaths)
		totalFilesUploaded += int64(uploadedCount)
		if err != nil {
			r.Metrics.RecordFilesProcessed(ctx, DestinationSyncerControllerName, totalFilesUploaded)
			return r.handleError(ctx, destinationSyncCR,
				fmt.Errorf("ingesting files for stage %s: %w", dep.Name, err))
		}
		logger.Info("successfully ingested files to destination",
			"stage", dep.Name, "uploaded", uploadedCount, "listed", len(filePaths))
	}

	successMessage := fmt.Sprintf("successfully reconciled destination sync: %s", destinationSyncCR.Name)
	if err := controllerutils.StatusPatch(ctx, r.Client, destinationSyncCR, func() {
		destinationSyncCR.Status.FilesProcessed += totalFilesListed
		destinationSyncCR.UpdateStatus(successMessage, nil)
	}); err != nil {
		// The uploads happened whether or not the status patch landed, and a
		// retry will not re-upload them, so the metric is recorded either way.
		r.Metrics.RecordFilesProcessed(ctx, DestinationSyncerControllerName, totalFilesUploaded)
		return r.handleError(ctx, destinationSyncCR,
			fmt.Errorf("updating DestinationSyncer status: %w", err))
	}
	r.Metrics.RecordFilesProcessed(ctx, DestinationSyncerControllerName, totalFilesUploaded)

	return ctrl.Result{}, nil
}

func (r *DestinationSyncerReconciler) handleError(ctx context.Context, destinationSyncCR *operatorv1alpha1.DestinationSyncer, err error) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Error(err, "encountered error")
	reconcileErr := err
	if updateErr := controllerutils.StatusPatch(ctx, r.Client, destinationSyncCR, func() {
		destinationSyncCR.UpdateStatus("", reconcileErr)
	}); updateErr != nil {
		logger.Error(updateErr, "failed to update DestinationSyncer CR status")
		return ctrl.Result{}, updateErr
	}
	return ctrl.Result{}, reconcileErr
}

func (r *DestinationSyncerReconciler) findDependents(ctx context.Context, obj client.Object) []reconcile.Request {
	list := &operatorv1alpha1.DestinationSyncerList{}
	if err := r.List(ctx, list, client.InNamespace(obj.GetNamespace())); err != nil {
		return nil
	}
	changedName := obj.GetName()
	var requests []reconcile.Request
	for _, item := range list.Items {
		pipelineName, err := controllerutils.ParentPipelineNameFromOwnerReference(&item)
		if err != nil {
			continue
		}
		for _, dep := range item.Spec.DependsOn {
			if pipelineName+"-"+dep.Name == changedName {
				requests = append(requests, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: item.Name, Namespace: item.Namespace},
				})
				break
			}
		}
	}
	return requests
}

func (r *DestinationSyncerReconciler) findSecretDependents(ctx context.Context, obj client.Object) []reconcile.Request {
	list := &operatorv1alpha1.DestinationSyncerList{}
	if err := r.List(ctx, list, client.InNamespace(obj.GetNamespace())); err != nil {
		return nil
	}
	secretName := obj.GetName()
	var requests []reconcile.Request
	for _, item := range list.Items {
		if item.Spec.SecretRef == secretName {
			requests = append(requests, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: item.Name, Namespace: item.Namespace},
			})
		}
	}
	return requests
}

// SetupWithManager sets up the controller with the Manager.
func (r *DestinationSyncerReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&operatorv1alpha1.DestinationSyncer{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Watches(&operatorv1alpha1.SourceCrawler{}, handler.EnqueueRequestsFromMapFunc(r.findDependents), builder.WithPredicates(controllerutils.FilesProcessedChangedPredicate{})).
		Watches(&operatorv1alpha1.DocumentProcessor{}, handler.EnqueueRequestsFromMapFunc(r.findDependents), builder.WithPredicates(controllerutils.FilesProcessedChangedPredicate{})).
		Watches(&operatorv1alpha1.ChunksGenerator{}, handler.EnqueueRequestsFromMapFunc(r.findDependents), builder.WithPredicates(controllerutils.FilesProcessedChangedPredicate{})).
		Watches(&operatorv1alpha1.VectorEmbeddingsGenerator{}, handler.EnqueueRequestsFromMapFunc(r.findDependents), builder.WithPredicates(controllerutils.FilesProcessedChangedPredicate{})).
		Watches(&corev1.Secret{}, handler.EnqueueRequestsFromMapFunc(r.findSecretDependents)).
		Named(DestinationSyncerControllerName).
		Complete(r.Metrics.ObserveReconciler(DestinationSyncerControllerName, r))
}
