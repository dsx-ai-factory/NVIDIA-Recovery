// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package job

import (
	"math"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestResolveNumericPagination(t *testing.T) {
	pageOne := int32(1)
	pageTwo := int32(2)
	pageZero := int32(0)
	pageNegative := int32(-1)
	pageOverflow := int32(math.MaxInt32)
	tokenZero := "0"
	tokenOffset := "37"
	tokenNegative := "-1"
	tokenOverflow := "2147483648"

	tests := []struct {
		name       string
		limit      int32
		pageNumber *int32
		pageToken  *string
		wantOffset int32
		wantCode   codes.Code
	}{
		{name: "defaults to first offset", limit: 25, wantOffset: 0},
		{name: "page one starts at zero", limit: 25, pageNumber: &pageOne, wantOffset: 0},
		{name: "page two starts after one limit", limit: 25, pageNumber: &pageTwo, wantOffset: 25},
		{name: "token is a zero based offset", limit: 25, pageToken: &tokenOffset, wantOffset: 37},
		{name: "rejects page zero", limit: 25, pageNumber: &pageZero, wantCode: codes.InvalidArgument},
		{name: "rejects negative page", limit: 25, pageNumber: &pageNegative, wantCode: codes.InvalidArgument},
		{name: "rejects negative token", limit: 25, pageToken: &tokenNegative, wantCode: codes.InvalidArgument},
		{name: "rejects overflowing token", limit: 25, pageToken: &tokenOverflow, wantCode: codes.InvalidArgument},
		{name: "rejects overflowing page offset", limit: 250, pageNumber: &pageOverflow, wantCode: codes.InvalidArgument},
		{name: "rejects page and token together", limit: 25, pageNumber: &pageOne, pageToken: &tokenZero, wantCode: codes.InvalidArgument},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pagination, err := resolveNumericPagination(test.limit, test.pageNumber, test.pageToken)
			if status.Code(err) != test.wantCode {
				t.Fatalf("error code = %s, want %s: %v", status.Code(err), test.wantCode, err)
			}
			if err == nil && pagination.offset != test.wantOffset {
				t.Fatalf("offset = %d, want %d", pagination.offset, test.wantOffset)
			}
			if err == nil && pagination.limit != test.limit {
				t.Fatalf("limit = %d, want %d", pagination.limit, test.limit)
			}
		})
	}
}

func TestNumericPaginationNextToken(t *testing.T) {
	tests := []struct {
		name          string
		pagination    numericPagination
		returnedCount int32
		totalCount    int32
		wantToken     string
		wantCode      codes.Code
	}{
		{
			name:          "returns next offset when records remain",
			pagination:    numericPagination{limit: 2, offset: 0},
			returnedCount: 2,
			totalCount:    3,
			wantToken:     "2",
		},
		{
			name:          "omits token on exact final page",
			pagination:    numericPagination{limit: 2, offset: 0},
			returnedCount: 2,
			totalCount:    2,
		},
		{
			name:          "omits token on partial final page",
			pagination:    numericPagination{limit: 2, offset: 2},
			returnedCount: 1,
			totalCount:    3,
		},
		{
			name:          "rejects overflowing next offset",
			pagination:    numericPagination{limit: 1, offset: math.MaxInt32},
			returnedCount: 1,
			totalCount:    math.MaxInt32,
			wantCode:      codes.Internal,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			token, err := test.pagination.nextToken(test.returnedCount, test.totalCount)
			if status.Code(err) != test.wantCode {
				t.Fatalf("error code = %s, want %s: %v", status.Code(err), test.wantCode, err)
			}
			if test.wantToken == "" {
				if token != nil {
					t.Fatalf("token = %q, want nil", *token)
				}
				return
			}
			if token == nil || *token != test.wantToken {
				t.Fatalf("token = %v, want %q", token, test.wantToken)
			}
		})
	}
}
