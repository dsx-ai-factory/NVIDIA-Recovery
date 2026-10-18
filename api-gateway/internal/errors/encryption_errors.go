// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package errors

import (
	"fmt"
)

type EncryptionError struct {
	Err     error  `json:"-"` // Actual error
	Message string `json:"message,omitempty"`
}

func ErrEncryptionFailed(err error, reason string) *EncryptionError {
	return &EncryptionError{
		Message: reason,
		Err:     err,
	}
}

func ErrEncryptionKeyNotFound(keyId string) *EncryptionError {
	return &EncryptionError{
		Err: fmt.Errorf("key (keyId: %s) not found", keyId),
	}
}

func ErrEncryptionKeySetMisconfigured(reason string) *EncryptionError {
	return &EncryptionError{
		Err:     fmt.Errorf("keyset misconfigured"),
		Message: reason,
	}
}

func (e *EncryptionError) Error() string {
	return e.Message + " - " + e.Err.Error()
}

func (e *EncryptionError) Equal(err error) bool {
	ee, ok := err.(*EncryptionError)
	if !ok {
		return false
	}
	return ee.Err.Error() == e.Err.Error()
}
