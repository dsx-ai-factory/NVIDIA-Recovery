// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package job

import (
	"math"
	"strconv"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type numericPagination struct {
	limit  int32
	offset int32
}

func resolveNumericPagination(limit int32, pageNumber *int32, pageToken *string) (numericPagination, error) {
	if limit <= 0 {
		return numericPagination{}, status.Error(codes.InvalidArgument, "limit must be greater than 0")
	}
	if pageNumber != nil && pageToken != nil {
		return numericPagination{}, status.Error(codes.InvalidArgument, "page number and page token are mutually exclusive")
	}

	offset := int64(0)
	switch {
	case pageNumber != nil:
		if *pageNumber <= 0 {
			return numericPagination{}, status.Error(codes.InvalidArgument, "page number must be greater than 0")
		}
		offset = int64(*pageNumber-1) * int64(limit)
	case pageToken != nil && *pageToken != "":
		parsedOffset, err := strconv.ParseInt(*pageToken, 10, 32)
		if err != nil || parsedOffset < 0 {
			return numericPagination{}, status.Error(codes.InvalidArgument, "invalid page token")
		}
		offset = parsedOffset
	}

	if offset > math.MaxInt32 {
		return numericPagination{}, status.Error(codes.InvalidArgument, "page offset exceeds supported range")
	}

	return numericPagination{limit: limit, offset: int32(offset)}, nil
}

func (p numericPagination) nextToken(returnedCount, totalCount int32) (*string, error) {
	if returnedCount < 0 || totalCount < 0 {
		return nil, status.Error(codes.Internal, "backend returned invalid pagination counts")
	}

	nextOffset := int64(p.offset) + int64(returnedCount)
	if nextOffset > math.MaxInt32 {
		return nil, status.Error(codes.Internal, "next page offset exceeds supported range")
	}
	if nextOffset >= int64(totalCount) {
		return nil, nil
	}

	token := strconv.FormatInt(nextOffset, 10)
	return &token, nil
}

func paginateItems[T any](items []T, pagination numericPagination) ([]T, *string, error) {
	if len(items) > math.MaxInt32 {
		return nil, nil, status.Error(codes.Internal, "item count exceeds supported range")
	}

	start := int64(pagination.offset)
	total := int64(len(items))
	if start >= total {
		return []T{}, nil, nil
	}

	end := start + int64(pagination.limit)
	if end > total {
		end = total
	}
	page := items[int(start):int(end)]
	nextPageToken, err := pagination.nextToken(int32(len(page)), int32(len(items)))
	if err != nil {
		return nil, nil, err
	}

	return page, nextPageToken, nil
}
