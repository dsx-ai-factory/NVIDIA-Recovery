// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package syncer

import (
	"context"
	"time"

	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"

	heimdallv1alpha1 "github.com/dsx-ai-factory/nvidia-recovery/advice-operator/api/v1alpha1"
	"github.com/dsx-ai-factory/nvidia-recovery/advice-operator/internal/cache"
	heimdallclient "github.com/dsx-ai-factory/nvidia-recovery/advice-operator/internal/client"
)

// AdviceSyncer implements manager.Runnable.
// It periodically polls the Heimdall API and emits reconcile events via a channel.
// The actual CRUD operations are handled by the AdviceReconciler.
type AdviceSyncer struct {
	K8sClient      client.Client
	HeimdallClient heimdallclient.AdviceSource
	PollInterval   time.Duration
	Namespaces     []string // empty means all namespaces

	// Cache stores the latest API advice data for reconciler access
	Cache *cache.AdviceCache

	// Events is the channel to send reconcile trigger events
	Events chan event.GenericEvent

	// Logger records synchronization outcomes.
	Logger *zap.Logger
}

// Start implements manager.Runnable.
// The Manager will call this method and expects it to block until the context is cancelled.
func (s *AdviceSyncer) Start(ctx context.Context) error {
	logger := s.logger()
	nsInfo := "*"
	if len(s.Namespaces) > 0 {
		nsInfo = "specific namespaces"
	}
	logger.Info("starting advice syncer", zap.Duration("interval", s.PollInterval), zap.String("namespaces", nsInfo))

	ticker := time.NewTicker(s.PollInterval)
	defer ticker.Stop()

	// Perform initial sync
	if err := s.syncAdvice(ctx); err != nil {
		logger.Error("initial sync failed", zap.Error(err))
	}

	// Periodic sync loop
	for {
		select {
		case <-ctx.Done():
			logger.Info("stopping advice syncer")
			return nil
		case <-ticker.C:
			if err := s.syncAdvice(ctx); err != nil {
				logger.Error("sync failed", zap.Error(err))
				// Continue - don't crash on transient errors
			}
		}
	}
}

// syncAdvice fetches advice from the API, updates the cache, and triggers reconciliation.
func (s *AdviceSyncer) syncAdvice(ctx context.Context) error {
	logger := s.logger().Named("sync")
	logger.Debug("starting advice sync")

	// Fetch all advice from the Heimdall API
	apiAdvice, err := s.HeimdallClient.ListAdvice(ctx)
	if err != nil {
		return err
	}

	apiAdvice = s.filterAdviceByNamespace(apiAdvice)

	logger.Info("fetched advice from API", zap.Int("count", len(apiAdvice)))

	// Update the cache and get change lists
	added, updated, removed := s.Cache.Update(apiAdvice)

	logger.Info("cache updated",
		zap.Int("added", len(added)),
		zap.Int("updated", len(updated)),
		zap.Int("removed", len(removed)))

	// Emit reconcile events for all items that need processing
	s.emitEventsForIDs(ctx, added)
	s.emitEventsForIDs(ctx, updated)

	// For removed items, emit events using namespace info captured before removal
	s.emitEventsForRemoved(ctx, removed)

	logger.Debug("completed advice sync")
	return nil
}

func (s *AdviceSyncer) filterAdviceByNamespace(advice []heimdallclient.APIAdvice) []heimdallclient.APIAdvice {
	if len(s.Namespaces) == 0 {
		return advice
	}

	allowed := make(map[string]struct{}, len(s.Namespaces))
	for _, namespace := range s.Namespaces {
		allowed[namespace] = struct{}{}
	}

	filtered := make([]heimdallclient.APIAdvice, 0, len(advice))
	for _, item := range advice {
		namespace := item.GetNamespace()
		if namespace == "" {
			namespace = "default"
		}
		if _, ok := allowed[namespace]; ok {
			filtered = append(filtered, item)
		}
	}

	return filtered
}

// emitEventsForIDs sends generic events to trigger reconciliation for the given IDs.
// Uses blocking sends to ensure events are not dropped.
func (s *AdviceSyncer) emitEventsForIDs(ctx context.Context, ids []string) {
	logger := s.logger()

	for _, id := range ids {
		adv := s.Cache.Get(id)
		if adv == nil {
			continue
		}

		namespace := adv.GetNamespace()
		if namespace == "" {
			namespace = "default"
		}

		// Create a minimal Advice object for the event
		// The reconciler will fetch the actual state
		adviceObj := &heimdallv1alpha1.Advice{
			ObjectMeta: metav1.ObjectMeta{
				Name:      id,
				Namespace: namespace,
			},
		}

		evt := event.GenericEvent{Object: adviceObj}
		select {
		case s.Events <- evt:
			logger.Debug("emitted reconcile event", zap.String("id", id), zap.String("namespace", namespace))
		case <-ctx.Done():
			return
		}
	}
}

// emitEventsForRemoved sends events for advice items that were removed from the API.
// The namespace info was captured before the items were removed from the cache.
func (s *AdviceSyncer) emitEventsForRemoved(ctx context.Context, removed []cache.RemovedAdvice) {
	logger := s.logger()

	for _, item := range removed {
		adviceObj := &heimdallv1alpha1.Advice{
			ObjectMeta: metav1.ObjectMeta{
				Name:      item.ID,
				Namespace: item.Namespace,
			},
		}

		evt := event.GenericEvent{Object: adviceObj}
		select {
		case s.Events <- evt:
			logger.Debug("emitted removal event", zap.String("id", item.ID), zap.String("namespace", item.Namespace))
		case <-ctx.Done():
			return
		}
	}
}

func (s *AdviceSyncer) logger() *zap.Logger {
	if s.Logger == nil {
		return zap.NewNop().Named("advice-syncer")
	}
	return s.Logger.Named("advice-syncer")
}
