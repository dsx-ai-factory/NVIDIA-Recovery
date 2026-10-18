// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package audit

import "context"

// EventDestination synchronously delivers one batch, including any retries and
// backoff required by its transport. Implementations must not retain or mutate
// events after Deliver returns. A returned delivered count must be between zero
// and len(events), inclusive. A nil error means every event was delivered. A
// returned error is terminal for the undelivered portion of the supplied batch.
type EventDestination[T any] interface {
	Deliver(ctx context.Context, events []T) (delivered int, err error)
}
