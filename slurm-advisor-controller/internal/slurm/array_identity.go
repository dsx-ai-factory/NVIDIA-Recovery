// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package slurm

import (
	"bytes"
	"encoding/json"
	"strconv"

	"github.com/cockroachdb/errors"
)

// identityNumber handles Slurm's integer and typed-number identity encodings.
// A wrapper without a set flag can identify an array, but cannot identify a
// scheduled task: synthetic pending rows must never turn into task zero.
type identityNumber struct {
	Set         bool `json:"set"`
	Infinite    bool `json:"infinite"`
	Number      int  `json:"number"`
	implicitSet bool
	unset       bool
}

// UnmarshalJSON accepts integer identities and preserves typed presence flags.
func (field *identityNumber) UnmarshalJSON(data []byte) error {
	*field = identityNumber{}
	data = bytes.TrimSpace(data)
	if bytes.Equal(data, []byte("null")) {
		return nil
	}
	var integer int
	if err := json.Unmarshal(data, &integer); err == nil {
		field.Set, field.Number = true, integer
		return nil
	}
	var wrapped struct {
		Set      json.RawMessage `json:"set"`
		Infinite json.RawMessage `json:"infinite"`
		Number   json.RawMessage `json:"number"`
	}
	if err := json.Unmarshal(data, &wrapped); err != nil {
		return errors.Wrap(err, "decode Slurm array identity number")
	}
	for _, member := range []struct {
		name   string
		raw    json.RawMessage
		target any
	}{
		{"set", wrapped.Set, &field.Set},
		{"infinite", wrapped.Infinite, &field.Infinite},
		{"number", wrapped.Number, &field.Number},
	} {
		if member.raw == nil {
			continue
		}
		if bytes.Equal(bytes.TrimSpace(member.raw), []byte("null")) {
			return errors.Newf("Slurm array identity %s cannot be null", member.name)
		}
		if err := json.Unmarshal(member.raw, member.target); err != nil {
			return errors.Wrapf(err, "decode Slurm array identity %s", member.name)
		}
	}
	field.implicitSet = wrapped.Set == nil
	field.unset = wrapped.Set != nil && !field.Set
	if wrapped.Number == nil && !field.unset {
		return errors.New("Slurm array identity number is missing")
	}
	return nil
}

// slurmJobIdentity never substitutes a raw job ID once an array is identified.
// Unset task wrappers represent aggregate pending rows; missing or malformed
// task identities instead fail discovery so the caller can report the problem.
func slurmJobIdentity(rawID int, array, task identityNumber) (jobID, arrayJobID, arrayTaskID string, include bool, err error) {
	if array.Infinite || ((array.Set || array.implicitSet) && invalidIdentityNumber(array.Number)) {
		return "", "", "", false, errors.New("invalid Slurm array job ID")
	}
	if (!array.Set && !array.implicitSet) || array.Number == 0 {
		return strconv.Itoa(rawID), "", "", true, nil
	}
	arrayJobID = strconv.Itoa(array.Number)
	if task.Infinite {
		return "", "", "", false, errors.New("infinite Slurm array task ID")
	}
	if task.unset {
		return "", arrayJobID, "", false, nil
	}
	if !task.Set || invalidIdentityNumber(task.Number) {
		return "", "", "", false, errors.New("missing or invalid Slurm array task ID")
	}
	arrayTaskID = strconv.Itoa(task.Number)
	return arrayJobID + "_" + arrayTaskID, arrayJobID, arrayTaskID, true, nil
}

func invalidIdentityNumber(value int) bool {
	// Slurm reserves NO_VAL (UINT32_MAX-1) and INFINITE (UINT32_MAX).
	return value < 0 || uint64(value) >= uint64(^uint32(0))-1
}
