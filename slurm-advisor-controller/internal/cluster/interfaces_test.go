// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package cluster_test

import (
	"testing"

	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/cluster"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/slurm"
)

func TestSlurmFactoryReturnsClusterContract(t *testing.T) {
	_ = cluster.Cluster(slurm.NewCluster(slurm.Options{}))
}
