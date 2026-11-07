// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package reconcile

import (
	"strings"
	"time"

	pb "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/go/heimdall/v1"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/cluster"
)

// seedAccountingIdentities retains identity evidence for relevant attempts after
// the one-time bootstrap. It never retains scheduler state for action decisions.
// Old anchors disappear once the attempt is absent from every discovery source.
func (c *Controller) seedAccountingIdentities(active []cluster.JobExecutionRecord, stored []*pb.Job, advice []*pb.Advice, seen []cluster.JobAttemptRef) {
	receiver, ok := c.JobReader.(cluster.AccountingIdentityReceiver)
	if !ok {
		return
	}
	keep := make(map[cluster.JobAttemptRef]struct{})
	for _, ref := range seen {
		keep[ref] = struct{}{}
	}
	if c.accountingIdentities == nil {
		c.accountingIdentities = make(map[cluster.JobAttemptRef]cluster.JobExecutionRecord)
	}
	remember := func(job cluster.JobExecutionRecord) {
		ref := jobAttemptRef(job)
		keep[ref] = struct{}{}
		if !strings.Contains(job.JobID, "_") || job.SubmitTime == nil || job.StartTime == nil {
			return
		}
		c.accountingIdentities[ref] = cluster.JobExecutionRecord{JobID: job.JobID, AttemptIndex: job.AttemptIndex, SubmitTime: job.SubmitTime, StartTime: job.StartTime}
	}
	for _, job := range stored {
		if job.GetSchedulerMetadata().GetFields()["scheduler"].GetStringValue() != cluster.SchedulerSlurm {
			continue
		}
		remember(accountingAnchor(job.GetJobId(), job.GetAttemptIndex(), job.GetSchedulerMetadata().AsMap()))
	}
	for _, item := range advice {
		if _, skip := c.ownershipSkip(item); skip {
			continue
		}
		remember(accountingAnchor(item.GetJobId(), item.GetAttemptIndex(), item.GetSchedulerMetadata().AsMap()))
	}
	for _, job := range active {
		remember(job)
	}
	anchors := make([]cluster.JobExecutionRecord, 0, len(c.accountingIdentities))
	for ref, anchor := range c.accountingIdentities {
		if _, ok := keep[ref]; !ok {
			delete(c.accountingIdentities, ref)
			continue
		}
		anchors = append(anchors, anchor)
	}
	receiver.SetAccountingIdentities(anchors)
}

func accountingAnchor(jobID string, attempt int32, metadata map[string]any) cluster.JobExecutionRecord {
	job := cluster.JobExecutionRecord{JobID: jobID, AttemptIndex: attempt}
	for key, target := range map[string]**time.Time{"slurm_submit_time": &job.SubmitTime, "slurm_start_time": &job.StartTime} {
		text, ok := metadata[key].(string)
		if !ok {
			continue
		}
		value, err := time.Parse(time.RFC3339, text)
		if err == nil && value.Unix() > 0 {
			*target = &value
		}
	}
	return job
}
