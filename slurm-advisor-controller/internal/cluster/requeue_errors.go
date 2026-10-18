// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package cluster

import "github.com/cockroachdb/errors"

// ErrJobRequeuePermanent identifies a rejected requeue that cannot succeed by
// repeating the same request. Other command failures receive a bounded retry
// budget; cancellation of the caller's context is handled separately.
var ErrJobRequeuePermanent = errors.New("job requeue permanently rejected")
