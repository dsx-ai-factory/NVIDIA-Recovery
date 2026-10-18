// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Package jobidentity derives stable identities for scheduler job attempts.
package jobidentity

import (
	"fmt"

	"github.com/google/uuid"
)

// HeimdallJobID returns the deterministic UUID used for a scheduler job attempt.
func HeimdallJobID(jobID string, attemptIndex int32) string {
	return uuid.NewSHA1(
		uuid.NameSpaceURL,
		[]byte(fmt.Sprintf("%s-%d", jobID, attemptIndex)),
	).String()
}
