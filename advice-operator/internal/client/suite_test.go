// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package client_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestClient(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Client Suite")
}

// All specs in this package can run in parallel since they don't share state.
// Use `ginkgo -p` or `go test -p` to run tests in parallel across packages.
// Individual specs are parallelizable by default in Ginkgo v2.
