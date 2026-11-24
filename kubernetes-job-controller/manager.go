// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"maps"
	"net/http"
	"slices"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/go-logr/zapr"
	"go.uber.org/zap"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// newManager discovers served workload kinds, reads the backend's open
// attempts for them, and registers one publication controller per kind. The
// inventory read completes before the manager exists, so every controller
// starts with the attempts it may need to close.
func newManager(ctx context.Context, cfg *rest.Config, namespace string, backend jobBackend, logger *zap.Logger) (manager.Manager, error) {
	httpClient, err := rest.HTTPClientFor(cfg)
	if err != nil {
		return nil, errors.Wrap(err, "initialize Kubernetes transport")
	}
	mapper, err := apiutil.NewDynamicRESTMapper(cfg, httpClient)
	if err != nil {
		return nil, errors.Wrap(err, "initialize Kubernetes resource mapper")
	}

	podSelector, err := labels.Parse(jobIDLabel)
	if err != nil {
		return nil, errors.Wrap(err, "build Pod label selector")
	}
	selectors := map[client.Object]cache.ByObject{
		&corev1.Pod{}: {Label: podSelector},
	}
	var kinds []schema.GroupVersionKind
	for _, kind := range []schema.GroupVersionKind{jobSetKind, pyTorchJobKind} {
		if _, err := mapper.RESTMapping(kind.GroupKind(), kind.Version); err != nil {
			if meta.IsNoMatchError(err) {
				logger.Info("workload kind unavailable", zap.String("kind", kind.String()))
				continue
			}
			return nil, errors.Wrapf(err, "discover workload kind %s", kind.String())
		}
		kinds = append(kinds, kind)
		selectors[workloadObject(kind)] = cache.ByObject{Label: labels.SelectorFromSet(labels.Set{enrollmentLabel: "true"})}
	}

	inventory, err := loadOpenAttempts(ctx, backend, namespace, kinds, logger)
	if err != nil {
		return nil, err
	}

	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Logger:  zapr.NewLogger(logger),
		Metrics: metricsserver.Options{BindAddress: "0"},
		MapperProvider: func(*rest.Config, *http.Client) (meta.RESTMapper, error) {
			return mapper, nil
		},
		Cache: cache.Options{
			DefaultNamespaces:           map[string]cache.Config{namespace: {}},
			ByObject:                    selectors,
			ReaderFailOnMissingInformer: true,
		},
	})
	if err != nil {
		return nil, errors.Wrap(err, "initialize workload manager")
	}
	if len(kinds) == 0 {
		return mgr, nil
	}

	// Register every index before startup. Controller-runtime waits for the
	// event sources to sync before starting any reconcile workers.
	indexer := mgr.GetFieldIndexer()
	for _, kind := range kinds {
		if err := indexer.IndexField(ctx, workloadObject(kind), jobIDLabel, jobIDIndex); err != nil {
			return nil, errors.Wrapf(err, "index %s by job ID", kind.Kind)
		}
	}
	for _, kind := range kinds {
		tracked := inventory[kind]
		if tracked == nil {
			tracked = make(attempts)
		}
		r := &workloadReconciler{
			cache: mgr.GetCache(), live: mgr.GetAPIReader(), backend: backend, kind: kind, kinds: kinds,
			logger: logger, placement: newPlacementIndex(kind), tracked: tracked,
		}
		logger.Info("recovered open workload attempts", zap.String("kind", kind.Kind), zap.Int("workloads", len(tracked)))
		if err := r.setup(mgr); err != nil {
			return nil, errors.Wrapf(err, "register %s controller", kind.Kind)
		}
	}
	return mgr, nil
}

func (r *workloadReconciler) setup(mgr manager.Manager) error {
	b := ctrl.NewControllerManagedBy(mgr).
		Named(strings.ToLower(r.kind.Kind)+"-publication").
		WithOptions(controller.Options{MaxConcurrentReconciles: 1}).
		Watches(&corev1.Pod{}, r.placementEvents(r.placement.updatePod)).
		WatchesRawSource(seedSource(slices.Collect(maps.Keys(r.tracked))))

	for _, kind := range r.kinds {
		obj := workloadObject(kind)
		// Workload events also revisit any peer sharing their job ID. This
		// lets a survivor publish when a duplicate is removed or relabeled.
		b.Watches(obj, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []reconcile.Request {
			requests := r.requestsForJobID(ctx, obj)
			if kind == r.kind {
				requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(obj)})
			}
			return requests
		}))
	}

	if r.kind == jobSetKind {
		b.WatchesMetadata(&batchv1.Job{}, r.placementEvents(r.placement.updateJob))
	}
	return b.Complete(r)
}

// Controller-runtime waits for the initial events from both informers before
// reconciling. Later events enqueue only workloads whose node set changed.
func (r *workloadReconciler) placementEvents(update func(client.Object, client.Object) membershipChanges) handler.Funcs {
	enqueue := func(ctx context.Context, q workqueue.TypedRateLimitingInterface[reconcile.Request], old, current client.Object) {
		for key := range update(old, current) {
			for _, request := range r.requestsForPlacement(ctx, key) {
				q.Add(request)
			}
		}
	}
	return handler.Funcs{
		CreateFunc: func(ctx context.Context, e event.CreateEvent, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			enqueue(ctx, q, nil, e.Object)
		},
		UpdateFunc: func(ctx context.Context, e event.UpdateEvent, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			enqueue(ctx, q, e.ObjectOld, e.ObjectNew)
		},
		DeleteFunc: func(ctx context.Context, e event.DeleteEvent, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			enqueue(ctx, q, e.Object, nil)
		},
	}
}

func jobIDIndex(obj client.Object) []string {
	if id := obj.GetLabels()[jobIDLabel]; id != "" {
		return []string{id}
	}
	return nil
}

func (r *workloadReconciler) requestsForJobID(ctx context.Context, obj client.Object) []reconcile.Request {
	id := obj.GetLabels()[jobIDLabel]
	if id == "" {
		return nil
	}
	return r.requestsForPlacement(ctx, placementKey{ownerKey: ownerKey{namespace: obj.GetNamespace()}, id: id})
}

func (r *workloadReconciler) requestsForPlacement(ctx context.Context, key placementKey) []reconcile.Request {
	workloads, err := workloadsWithID(ctx, r.cache, r.kind, key.namespace, key.id)
	if err != nil {
		if ctx.Err() == nil {
			r.logger.Warn("map event to workload", zap.String("kind", r.kind.Kind), zap.Error(err))
		}
		return nil
	}
	requests := make([]reconcile.Request, 0, len(workloads))
	for _, workload := range workloads {
		if key.uid != "" && workload.GetUID() != key.uid {
			continue
		}
		requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&workload)})
	}
	return requests
}
