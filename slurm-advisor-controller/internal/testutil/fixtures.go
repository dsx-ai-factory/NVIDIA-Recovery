// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package testutil

import (
	"os"
	"path/filepath"
	"testing"
)

func ReadFixture(t testing.TB, path ...string) []byte {
	t.Helper()

	payload, err := os.ReadFile(filepath.Join(path...))
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}

	return payload
}
