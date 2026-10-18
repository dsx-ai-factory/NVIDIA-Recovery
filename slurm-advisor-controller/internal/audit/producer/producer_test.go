// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package producer_test

import (
	"context"
	"testing"
	"time"

	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/audit"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/audit/destinations"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/audit/producer"
	recoveryaudit "github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/audit/recovery"
	watchdogaudit "github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/audit/watchdog"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/reconcile"
	watchdogengine "github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/watchdog"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestStartDeliversControllerAndWatchdogAuditEvents(t *testing.T) {
	core, observed := observer.New(zap.InfoLevel)
	logger := zap.New(core)

	sink, stop := producer.Start(producer.Options{
		Destination:   destinations.NewLogging(logger),
		MaxBatchSize:  1,
		BufferSize:    3,
		FlushInterval: time.Hour,
		Logger:        zap.NewNop(),
	})

	controllerAudit := recoveryaudit.NewObserver(sink)
	application := reconcile.AdviceApplicationObservation{
		AdviceID:     "controller-advice",
		JobID:        "controller-job",
		AttemptIndex: 2,
		Outcome:      reconcile.AdviceApplicationOutcomeApplied,
		Actions: []reconcile.SchedulerActionObservation{{
			Action:      reconcile.SchedulerActionRelease,
			Outcome:     reconcile.SchedulerActionOutcomeSuccess,
			CompletedAt: time.Date(2026, time.September, 2, 20, 0, 0, 0, time.UTC),
		}},
	}
	controllerAudit.ObserveAdviceSchedulerAction(application, application.Actions[0])
	controllerAudit.ObserveAdviceApplication(application)

	watchdogAudit := watchdogaudit.NewObserver(sink)
	_, releaseScope := watchdogAudit.StartReleaseScope(
		context.Background(),
		watchdogengine.ReleaseAttributes{JobID: "watchdog-job", AttemptIndex: 4},
	)
	releaseScope.EndReleaseScope(watchdogengine.ReleaseResult{
		CompletedAt: time.Date(2026, time.September, 2, 20, 1, 0, 0, time.UTC),
	})
	stop()

	entries := observed.All()
	if len(entries) != 3 {
		t.Fatalf("delivered logs = %d, want 3 before Stop returns", len(entries))
	}
	eventsByType := make(map[string]map[string]any, 3)
	for _, entry := range entries {
		fields := entry.ContextMap()
		eventsByType[fields["event_type"].(string)] = fields
	}
	controllerEvent := eventsByType[string(audit.AdviceSchedulerActionCompleted)]
	if controllerEvent["heimdall_job_id"] != "controller-advice" || controllerEvent["event_producer"] != string(audit.ProducerSlurmAdvisorController) {
		t.Fatalf("controller event = %#v, want Advice action audit envelope", controllerEvent)
	}
	watchdogEvent := eventsByType[string(audit.WatchdogReleaseCompleted)]
	if watchdogEvent["event_producer"] != string(audit.ProducerSlurmWatchdog) {
		t.Fatalf("watchdog event = %#v, want watchdog release audit envelope", watchdogEvent)
	}
}
