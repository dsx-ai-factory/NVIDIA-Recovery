// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package jobidentity

import "testing"

func TestHeimdallJobIDIsStablePerAttempt(t *testing.T) {
	t.Parallel()

	first := HeimdallJobID("123", 2)
	if first == "" || first != HeimdallJobID("123", 2) {
		t.Fatalf("HeimdallJobID() = %q, want stable nonempty ID", first)
	}
	if first == HeimdallJobID("123", 3) || first == HeimdallJobID("456", 2) {
		t.Fatal("HeimdallJobID() did not distinguish job attempts")
	}
}
