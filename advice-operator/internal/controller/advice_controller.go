// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"

	"go.uber.org/zap"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/source"

	heimdallv1alpha1 "github.com/dsx-ai-factory/nvidia-recovery/advice-operator/api/v1alpha1"
	"github.com/dsx-ai-factory/nvidia-recovery/advice-operator/internal/cache"
)

// AdviceReconciler reconciles Advice objects based on the Heimdall API state.
// It receives events from the AdviceSyncer and ensures cluster state matches API state.
type AdviceReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	// APIReader bypasses the cache for owner lookups so deletions are observed immediately.
	APIReader client.Reader

	// Cache provides access to the latest API advice data
	Cache *cache.AdviceCache

	// Logger records reconciliation outcomes.
	Logger *zap.Logger
}

const (
	labelJobID        = "heimdall.nvidia.com/job-id"
	labelAttemptIndex = "heimdall.nvidia.com/attempt-index"
)

// +kubebuilder:rbac:groups=heimdall.nvidia.com,resources=advices,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=heimdall.nvidia.com,resources=advices/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=heimdall.nvidia.com,resources=advices/finalizers,verbs=update
// +kubebuilder:rbac:groups=run.ai,resources=distributedworkloads,verbs=get

// Reconcile ensures the Advice CRD matches the desired state from the Heimdall API.
// It handles create, update, and delete operations based on the API cache state.
func (r *AdviceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := r.logger().With(zap.String("advice", req.String()))

	// The Name is the HeimdallID
	heimdallID := req.Name
	namespace := req.Namespace

	// Check if this advice exists in the API cache
	apiAdvice := r.Cache.Get(heimdallID)

	// Get the existing Advice CRD if it exists
	var existingAdvice heimdallv1alpha1.Advice
	err := r.Get(ctx, req.NamespacedName, &existingAdvice)
	existsInCluster := err == nil

	if err != nil && !apierrors.IsNotFound(err) {
		logger.Error("failed to get Advice", zap.Error(err))
		return ctrl.Result{}, err
	}

	// Handle deletion: if not in API but exists in cluster, delete
	if apiAdvice == nil {
		if existsInCluster {
			logger.Info("deleting advice no longer in API")
			if err := r.Delete(ctx, &existingAdvice); err != nil {
				if !apierrors.IsNotFound(err) {
					logger.Error("failed to delete advice", zap.Error(err))
					return ctrl.Result{}, err
				}
			}
		}
		// Nothing to do - not in API and not in cluster
		return ctrl.Result{}, nil
	}

	// Build the desired Advice CRD
	advice := &heimdallv1alpha1.Advice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      heimdallID,
			Namespace: namespace,
		},
	}

	// Get the desired state from the API advice
	desiredAdvice, err := apiAdvice.ToAdviceCRD()
	if err != nil {
		logger.Error("failed to convert API advice to CRD", zap.Error(err))
		return ctrl.Result{}, nil // Don't requeue - invalid advice data
	}

	if len(desiredAdvice.OwnerReferences) > 0 {
		ownerRef := desiredAdvice.OwnerReferences[0]
		ownerExists, err := r.ownerExists(ctx, namespace, ownerRef)
		if err != nil {
			logger.Error("failed to look up advice owner",
				zap.String("ownerAPIVersion", ownerRef.APIVersion),
				zap.String("ownerKind", ownerRef.Kind),
				zap.String("ownerName", ownerRef.Name),
				zap.Error(err))
			return ctrl.Result{}, err
		}

		if !ownerExists {
			logger.Info("deleting advice for missing owner",
				zap.String("ownerAPIVersion", ownerRef.APIVersion),
				zap.String("ownerKind", ownerRef.Kind),
				zap.String("ownerName", ownerRef.Name),
				zap.String("ownerUID", string(ownerRef.UID)))
			if existsInCluster {
				if err := r.Delete(ctx, &existingAdvice); err != nil {
					if !apierrors.IsNotFound(err) {
						logger.Error("failed to delete orphaned advice", zap.Error(err))
						return ctrl.Result{}, err
					}
				}
			}
			return ctrl.Result{}, nil
		}
	}

	// Create or update the Advice CRD
	result, err := controllerutil.CreateOrUpdate(ctx, r.Client, advice, func() error {
		// Copy labels, annotations, and spec from desired state
		if advice.Labels == nil {
			advice.Labels = make(map[string]string)
		}
		for k, v := range desiredAdvice.Labels {
			advice.Labels[k] = v
		}

		// Copy annotations and spec from desired state
		if advice.Annotations == nil {
			advice.Annotations = make(map[string]string)
		}
		for k, v := range desiredAdvice.Annotations {
			advice.Annotations[k] = v
		}
		advice.OwnerReferences = desiredAdvice.OwnerReferences
		advice.Spec = desiredAdvice.Spec

		return nil
	})

	if err != nil {
		logger.Error("failed to create or update Advice", zap.Error(err))
		return ctrl.Result{}, err
	}

	switch result {
	case controllerutil.OperationResultCreated:
		logger.Info("created Advice CRD", zap.String("jobID", apiAdvice.JobID))
	case controllerutil.OperationResultUpdated:
		logger.Info("updated Advice CRD", zap.String("jobID", apiAdvice.JobID))
	case controllerutil.OperationResultNone:
		logger.Debug("Advice CRD unchanged")
	}

	return ctrl.Result{}, nil
}

func (r *AdviceReconciler) logger() *zap.Logger {
	if r.Logger == nil {
		return zap.NewNop().Named("advice-controller")
	}
	return r.Logger.Named("advice-controller")
}

func (r *AdviceReconciler) ownerExists(ctx context.Context, namespace string, ownerRef metav1.OwnerReference) (bool, error) {
	owner := &unstructured.Unstructured{}
	owner.SetGroupVersionKind(schema.FromAPIVersionAndKind(ownerRef.APIVersion, ownerRef.Kind))

	var reader client.Reader = r.Client
	if r.APIReader != nil {
		reader = r.APIReader
	}

	err := reader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: ownerRef.Name}, owner)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}

	return owner.GetUID() == ownerRef.UID, nil
}

// SetupWithManager sets up the controller with the Manager.
// It watches Advice CRDs and also receives events from the syncer via a channel.
func (r *AdviceReconciler) SetupWithManager(mgr ctrl.Manager, events <-chan event.GenericEvent) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&heimdallv1alpha1.Advice{}, builder.WithPredicates(adviceLabelPredicate())).
		// Watch for events from the syncer
		WatchesRawSource(source.Channel(events, &handler.EnqueueRequestForObject{})).
		Named("advice").
		Complete(r)
}

func hasAdviceTrackingLabels(labels map[string]string) bool {
	return labels[labelJobID] != "" && labels[labelAttemptIndex] != ""
}

func adviceLabelPredicate() predicate.Funcs {
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			return e.Object != nil && hasAdviceTrackingLabels(e.Object.GetLabels())
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			return (e.ObjectNew != nil && hasAdviceTrackingLabels(e.ObjectNew.GetLabels())) ||
				(e.ObjectOld != nil && hasAdviceTrackingLabels(e.ObjectOld.GetLabels()))
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			return e.Object != nil && hasAdviceTrackingLabels(e.Object.GetLabels())
		},
		GenericFunc: func(e event.GenericEvent) bool {
			return e.Object != nil && hasAdviceTrackingLabels(e.Object.GetLabels())
		},
	}
}
