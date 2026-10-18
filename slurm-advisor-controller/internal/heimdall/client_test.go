// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package heimdall

import "testing"

func TestResolvePaginationOptionsUsesPositivePageLimit(t *testing.T) {
	got := ResolvePaginationOptions(WithPageLimit(42))

	if got.PageLimit != 42 {
		t.Fatalf("PageLimit = %d, want 42", got.PageLimit)
	}
}

func TestResolvePaginationOptionsDefaultsNonPositivePageLimit(t *testing.T) {
	for _, limit := range []int32{0, -1} {
		got := ResolvePaginationOptions(WithPageLimit(limit))

		if got.PageLimit != DefaultPageLimit {
			t.Fatalf("PageLimit with input %d = %d, want %d", limit, got.PageLimit, DefaultPageLimit)
		}
	}
}
