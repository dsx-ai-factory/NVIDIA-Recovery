// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package audit

import (
	"context"
	stderrors "errors"
	pb "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/go/heimdall/v1"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	auditclient "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/internal/audit"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestDownstreamErrorToStatus(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		wantCode codes.Code
	}{
		{name: "bad request", err: httpError(http.StatusBadRequest), wantCode: codes.InvalidArgument},
		{name: "too large", err: httpError(http.StatusRequestEntityTooLarge), wantCode: codes.InvalidArgument},
		{name: "unprocessable", err: httpError(http.StatusUnprocessableEntity), wantCode: codes.InvalidArgument},
		{name: "disabled", err: httpError(http.StatusNotFound), wantCode: codes.NotFound},
		{name: "timeout", err: httpError(http.StatusRequestTimeout), wantCode: codes.DeadlineExceeded},
		{name: "too early", err: httpError(http.StatusTooEarly), wantCode: codes.ResourceExhausted},
		{name: "overloaded", err: httpError(http.StatusTooManyRequests), wantCode: codes.ResourceExhausted},
		{name: "downstream failure", err: httpError(http.StatusInternalServerError), wantCode: codes.Unavailable},
		{name: "unexpected response", err: httpError(http.StatusTeapot), wantCode: codes.FailedPrecondition},
		{name: "context deadline", err: context.DeadlineExceeded, wantCode: codes.DeadlineExceeded},
		{name: "context canceled", err: context.Canceled, wantCode: codes.Canceled},
		{name: "protocol failure", err: stderrors.New("decode failed"), wantCode: codes.Internal},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := downstreamErrorToStatus(test.err)
			if got := status.Code(err); got != test.wantCode {
				t.Fatalf("code = %s, want %s: %v", got, test.wantCode, err)
			}
		})
	}
}

func httpError(statusCode int) error {
	return &auditclient.HTTPError{StatusCode: statusCode, Status: http.StatusText(statusCode)}
}

func TestReadValidationAndFailures(t *testing.T) {
	zero, negative, tooLarge := int32(0), int32(-1), int32(101)
	job, uuid := "123", "job-uuid"
	for _, request := range []*pb.GetAuditEventsRequest{
		{AttemptIndex: &zero}, {JobId: &job, AttemptIndex: &negative},
		{HeimdallJobId: &uuid, JobId: &job}, {HeimdallJobId: &uuid, AttemptIndex: &zero},
		{Limit: &zero}, {Limit: &tooLarge},
	} {
		source, err := NewAuditClientSource("http://audit.test", &http.Client{Transport: readTransport(func(*http.Request) (*http.Response, error) {
			t.Fatal("invalid request reached downstream")
			return nil, nil
		})})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := source.GetAuditEvents(context.Background(), request); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("invalid selectors %#v: %v", request, err)
		}
	}
	for _, test := range []struct {
		name           string
		status         int
		body           string
		transportError error
		want           codes.Code
	}{
		{name: "too large page", status: 200, body: strings.Repeat("x", (4<<20)+1), want: codes.ResourceExhausted},
		{name: "invalid cursor", status: 422, body: `{"detail":"private detail"}`, want: codes.InvalidArgument},
		{name: "downstream failure", status: 503, body: `{"detail":"private detail"}`, want: codes.Unavailable},
		{name: "deadline", transportError: context.DeadlineExceeded, want: codes.DeadlineExceeded},
		{name: "canceled", transportError: context.Canceled, want: codes.Canceled},
	} {
		t.Run(test.name, func(t *testing.T) {
			source, err := NewAuditClientSource("http://audit.test", &http.Client{Transport: readTransport(func(*http.Request) (*http.Response, error) {
				if test.transportError != nil {
					return nil, test.transportError
				}
				return &http.Response{StatusCode: test.status, Status: http.StatusText(test.status), Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(test.body))}, nil
			})})
			if err != nil {
				t.Fatal(err)
			}
			_, err = source.GetAuditEvents(context.Background(), &pb.GetAuditEventsRequest{})
			if status.Code(err) != test.want {
				t.Fatalf("code %v, want %v: %v", status.Code(err), test.want, err)
			}
			if strings.Contains(err.Error(), "private detail") {
				t.Fatal("downstream detail leaked")
			}
		})
	}
}

func TestReadPreservesOptionalAttemptAndContext(t *testing.T) {
	zero, job := int32(0), "123"
	for _, attempt := range []*int32{nil, &zero} {
		source, err := NewAuditClientSource("http://audit.test", &http.Client{Transport: readTransport(func(request *http.Request) (*http.Response, error) {
			if request.URL.Query().Has("attempt_index") != (attempt != nil) {
				t.Fatal("attempt presence changed")
			}
			if request.URL.Query().Get("job_id") != job {
				t.Fatal("scheduler job filter missing")
			}
			<-request.Context().Done()
			return nil, request.Context().Err()
		})})
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err = source.GetAuditEvents(ctx, &pb.GetAuditEventsRequest{JobId: &job, AttemptIndex: attempt})
		if status.Code(err) != codes.Canceled {
			t.Fatalf("cancellation lost: %v", err)
		}
	}
}

type readTransport func(*http.Request) (*http.Response, error)

func (transport readTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	return transport(request)
}

func TestReadPropagatesDeadline(t *testing.T) {
	deadline := time.Now().Add(-time.Second)
	source, err := NewAuditClientSource("http://audit.test", &http.Client{Transport: readTransport(func(request *http.Request) (*http.Response, error) {
		got, ok := request.Context().Deadline()
		if !ok || !got.Equal(deadline) {
			t.Fatal("request deadline was not propagated")
		}
		<-request.Context().Done()
		return nil, request.Context().Err()
	})})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	_, err = source.GetAuditEvents(ctx, &pb.GetAuditEventsRequest{})
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("deadline lost: %v", err)
	}
}
