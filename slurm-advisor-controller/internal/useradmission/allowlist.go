// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Package useradmission shares the temporary initial-rollout username selection
// between SAC and the watchdog. This control may be deprecated without notice.
package useradmission

import (
	"slices"
	"strings"

	"github.com/cockroachdb/errors"
)

// Allowlist matches exact, case-sensitive submitting usernames. An empty list
// disables selection; a configured list does not match an unknown username.
// Job policy determines precedence and never uses this to hide monitored jobs.
type Allowlist []string

// Decode implements envconfig's decoder for comma-separated usernames. It trims
// entries and removes duplicates, but rejects separator-only input so a typo
// cannot silently disable username selection.
func (allowlist *Allowlist) Decode(value string) error {
	if strings.TrimSpace(value) == "" {
		*allowlist = nil
		return nil
	}
	var users Allowlist
	for _, user := range strings.Split(value, ",") {
		user = strings.TrimSpace(user)
		if user != "" && !slices.Contains(users, user) {
			users = append(users, user)
		}
	}
	if len(users) == 0 {
		return errors.New("user allowlist must contain a username or be empty to disable filtering")
	}
	*allowlist = users
	return nil
}

// Admits reports whether the scheduler's submitting username matches the list.
func (allowlist Allowlist) Admits(user string) bool {
	return len(allowlist) == 0 || (user != "" && slices.Contains(allowlist, user))
}
