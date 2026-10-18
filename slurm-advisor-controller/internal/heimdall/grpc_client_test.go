// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package heimdall_test

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	pb "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/go/heimdall/v1"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/grpcauth"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/heimdall"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestServiceAccountTokenCredentialsReadAndAttachBearerMetadata(t *testing.T) {
	tokenPath := writeTokenFile(t, "  first-token\n")
	credentials := grpcauth.NewServiceAccountTokenCredentials(tokenPath)

	got, err := credentials.GetRequestMetadata(context.Background())
	if err != nil {
		t.Fatalf("GetRequestMetadata() failed: %v", err)
	}
	if diff := cmp.Diff(map[string]string{"authorization": "Bearer first-token"}, got); diff != "" {
		t.Fatalf("GetRequestMetadata() mismatch (-want +got):\n%s", diff)
	}
	if credentials.RequireTransportSecurity() {
		t.Fatal("RequireTransportSecurity() = true, want false while the configured internal transport remains insecure")
	}
}

func TestServiceAccountTokenCredentialsObserveRotation(t *testing.T) {
	tokenPath := writeTokenFile(t, "first-token")
	credentials := grpcauth.NewServiceAccountTokenCredentials(tokenPath)

	first, err := credentials.GetRequestMetadata(context.Background())
	if err != nil {
		t.Fatalf("first GetRequestMetadata() failed: %v", err)
	}
	if err := os.WriteFile(tokenPath, []byte("second-token\n"), 0o600); err != nil {
		t.Fatalf("rotate token file: %v", err)
	}
	second, err := credentials.GetRequestMetadata(context.Background())
	if err != nil {
		t.Fatalf("second GetRequestMetadata() failed: %v", err)
	}
	if first["authorization"] != "Bearer first-token" || second["authorization"] != "Bearer second-token" {
		t.Fatalf("authorization metadata = (%q, %q), want rotated values", first["authorization"], second["authorization"])
	}
}

func TestServiceAccountTokenCredentialsFailClosedWithoutExposingToken(t *testing.T) {
	for name, tokenPath := range map[string]string{
		"unreadable": filepath.Join(t.TempDir(), "missing-token"),
		"empty":      writeTokenFile(t, " \n\t "),
	} {
		t.Run(name, func(t *testing.T) {
			credentials := grpcauth.NewServiceAccountTokenCredentials(tokenPath)
			_, err := credentials.GetRequestMetadata(context.Background())
			if err == nil {
				t.Fatal("GetRequestMetadata() succeeded, want error")
			}
			if strings.Contains(err.Error(), "first-token") {
				t.Fatalf("GetRequestMetadata() exposed token contents: %q", err)
			}
		})
	}
}

func TestServiceAccountTokenCredentialsAttachMetadataToEveryRPC(t *testing.T) {
	tokenPath := writeTokenFile(t, "first-token")
	listener := bufconn.Listen(1024 * 1024)
	var authorizations []string
	grpcServer := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		authorizations = append(authorizations, metadata.ValueFromIncomingContext(ctx, "authorization")...)
		return handler(ctx, req)
	}))
	pb.RegisterHeimdallServiceServer(grpcServer, &recordingHeimdallServer{})
	pb.RegisterAuditServiceServer(grpcServer, &acceptingAuditServer{})
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(func() {
		grpcServer.Stop()
		_ = listener.Close()
	})

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithPerRPCCredentials(grpcauth.NewServiceAccountTokenCredentials(tokenPath)),
	)
	if err != nil {
		t.Fatalf("NewClient() failed: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	client := heimdall.NewGRPCClientFromConn(conn)
	if _, err := client.GetJobs(context.Background()); err != nil {
		t.Fatalf("first GetJobs() failed: %v", err)
	}
	if _, err := client.CreateAuditEvents(context.Background(), &pb.CreateAuditEventsRequest{}); err != nil {
		t.Fatalf("first audit call failed: %v", err)
	}
	if err := os.WriteFile(tokenPath, []byte("second-token"), 0o600); err != nil {
		t.Fatalf("rotate token file: %v", err)
	}
	if _, err := client.GetJobs(context.Background()); err != nil {
		t.Fatalf("second GetJobs() failed: %v", err)
	}
	if _, err := client.CreateAuditEvents(context.Background(), &pb.CreateAuditEventsRequest{}); err != nil {
		t.Fatalf("second audit call failed: %v", err)
	}
	if diff := cmp.Diff([]string{"Bearer first-token", "Bearer first-token", "Bearer second-token", "Bearer second-token"}, authorizations); diff != "" {
		t.Fatalf("authorization metadata mismatch (-want +got):\n%s", diff)
	}
}

func writeTokenFile(t *testing.T, token string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(path, []byte(token), 0o600); err != nil {
		t.Fatalf("write token file: %v", err)
	}
	return path
}

func TestGRPCClientCreateOrUpdateJob(t *testing.T) {
	ctx := context.Background()
	server := &recordingHeimdallServer{}
	client, cleanup := newBufconnClient(t, server)
	defer cleanup()

	_, err := client.CreateOrUpdateJob(ctx, &pb.CreateOrUpdateJobRequest{JobId: "12345"})
	if err != nil {
		t.Fatalf("CreateOrUpdateJob() failed: %v", err)
	}

	if server.upsertJobID != "12345" {
		t.Fatalf("upsert job ID = %q, want 12345", server.upsertJobID)
	}
}

func TestGRPCClientListAdvicePaginates(t *testing.T) {
	ctx := context.Background()
	nextPage := "next"
	firstAdvice := &pb.Advice{HeimdallJobId: "advice-a"}
	secondAdvice := &pb.Advice{HeimdallJobId: "advice-b"}
	server := &recordingHeimdallServer{
		listAdviceResponses: map[string]*pb.ListAdviceResponse{
			"":     {Advice: []*pb.Advice{firstAdvice}, NextPageToken: &nextPage},
			"next": {Advice: []*pb.Advice{secondAdvice}},
		},
	}
	client, cleanup := newBufconnClient(t, server)
	defer cleanup()

	advice, err := client.ListAdvice(
		ctx,
		heimdall.WithPageLimit(42),
		heimdall.WithOnlyActiveAdvice(),
	)
	if err != nil {
		t.Fatalf("ListAdvice() failed: %v", err)
	}

	if diff := cmp.Diff([]pageCall{
		{PageToken: "", Limit: 42, OnlyActive: true},
		{PageToken: "next", Limit: 42, OnlyActive: true},
	}, server.listAdviceCalls); diff != "" {
		t.Fatalf("ListAdvice calls mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff([]*pb.Advice{firstAdvice, secondAdvice}, advice, protocmp.Transform()); diff != "" {
		t.Fatalf("ListAdvice() mismatch (-want +got):\n%s", diff)
	}
}

func TestGRPCClientUpdatesAdviceReconciliationStatus(t *testing.T) {
	ctx := context.Background()
	server := &recordingHeimdallServer{}
	client, cleanup := newBufconnClient(t, server)
	defer cleanup()
	statusUpdate := &pb.AdviceReconciliationStatus{
		ObservedGeneration: 3,
		Disposition:        pb.AdviceReconciliationDisposition_ADVICE_RECONCILIATION_DISPOSITION_CONVERGED,
		Reason:             pb.AdviceReconciliationReason_ADVICE_RECONCILIATION_REASON_DESIRED_STATE_OBSERVED,
	}

	got, err := client.UpdateAdviceReconciliationStatus(ctx, "advice-a", statusUpdate)
	if err != nil {
		t.Fatalf("UpdateAdviceReconciliationStatus() failed: %v", err)
	}

	if server.statusAdviceID != "advice-a" {
		t.Fatalf("status advice ID = %q, want advice-a", server.statusAdviceID)
	}
	if diff := cmp.Diff(statusUpdate, server.statusUpdate, protocmp.Transform()); diff != "" {
		t.Fatalf("status request mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(statusUpdate, got, protocmp.Transform()); diff != "" {
		t.Fatalf("status response mismatch (-want +got):\n%s", diff)
	}
}

func TestGRPCClientListAdviceReturnsLaterPageError(t *testing.T) {
	ctx := context.Background()
	nextPage := "next"
	pageErr := errors.New("backend unavailable")
	server := &recordingHeimdallServer{
		listAdviceResponses: map[string]*pb.ListAdviceResponse{
			"": {NextPageToken: &nextPage},
		},
		listAdviceErrByPage: map[string]error{"next": pageErr},
	}
	client, cleanup := newBufconnClient(t, server)
	defer cleanup()

	_, err := client.ListAdvice(ctx, heimdall.WithPageLimit(42))
	if status.Code(err) != codes.Unknown {
		t.Fatalf("ListAdvice() error code = %s, want %s: %v", status.Code(err), codes.Unknown, err)
	}
	if !strings.Contains(err.Error(), `list advice page "next"`) {
		t.Fatalf("ListAdvice() error = %q, want page context", err)
	}
	if !strings.Contains(err.Error(), pageErr.Error()) {
		t.Fatalf("ListAdvice() error = %q, want backend error", err)
	}
}

func TestGRPCClientGetJobsPaginates(t *testing.T) {
	ctx := context.Background()
	nextPage := "next"
	firstJob := &pb.Job{HeimdallJobId: "heimdall-a", JobId: "job-a"}
	secondJob := &pb.Job{HeimdallJobId: "heimdall-b", JobId: "job-b"}
	server := &recordingHeimdallServer{
		getJobsResponses: map[string]*pb.GetJobsResponse{
			"":     {Jobs: []*pb.Job{firstJob}, NextPageToken: &nextPage},
			"next": {Jobs: []*pb.Job{secondJob}},
		},
	}
	client, cleanup := newBufconnClient(t, server)
	defer cleanup()

	jobs, err := client.GetJobs(ctx, heimdall.WithPageLimit(42))
	if err != nil {
		t.Fatalf("GetJobs() failed: %v", err)
	}

	completedJobs := false
	wantCalls := []pageCall{
		{PageToken: "", Limit: 42, CompletedJobs: &completedJobs},
		{PageToken: "next", Limit: 42, CompletedJobs: &completedJobs},
	}
	if diff := cmp.Diff(wantCalls, server.getJobsCalls); diff != "" {
		t.Fatalf("GetJobs calls mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff([]*pb.Job{firstJob, secondJob}, jobs, protocmp.Transform()); diff != "" {
		t.Fatalf("GetJobs() mismatch (-want +got):\n%s", diff)
	}
}

func TestGRPCClientGetJobsReturnsLaterPageError(t *testing.T) {
	ctx := context.Background()
	nextPage := "next"
	pageErr := errors.New("backend unavailable")
	server := &recordingHeimdallServer{
		getJobsResponses: map[string]*pb.GetJobsResponse{
			"": {NextPageToken: &nextPage},
		},
		getJobsErrByPage: map[string]error{"next": pageErr},
	}
	client, cleanup := newBufconnClient(t, server)
	defer cleanup()

	_, err := client.GetJobs(ctx, heimdall.WithPageLimit(42))
	if status.Code(err) != codes.Unknown {
		t.Fatalf("GetJobs() error code = %s, want %s: %v", status.Code(err), codes.Unknown, err)
	}
	if !strings.Contains(err.Error(), `get jobs page "next"`) {
		t.Fatalf("GetJobs() error = %q, want page context", err)
	}
	if !strings.Contains(err.Error(), pageErr.Error()) {
		t.Fatalf("GetJobs() error = %q, want backend error", err)
	}
}

// The cutoff must only constrain the failure read, remain fixed across pages,
// and never let a failed second read commit a partial bootstrap.
func TestGRPCClientGetJobsIncludesRecentFailures(t *testing.T) {
	for _, failLastPage := range []bool{false, true} {
		name := "complete"
		if failLastPage {
			name = "failure-page-unavailable"
		}
		t.Run(name, func(t *testing.T) {
			cutoff := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
			first := &pb.Job{JobId: "long-running"}
			second := &pb.Job{JobId: "pending"}
			failed := &pb.Job{JobId: "recent-failure"}
			last := &pb.Job{JobId: "recent-timeout"}
			server := &recordingHeimdallServer{
				getJobsResponses: map[string]*pb.GetJobsResponse{
					"":     {Jobs: []*pb.Job{first}, NextPageToken: new("next")},
					"next": {Jobs: []*pb.Job{second}},
				},
				recentFailureResponses: map[string]*pb.GetJobsResponse{
					"":     {Jobs: []*pb.Job{failed}, NextPageToken: new("next")},
					"next": {Jobs: []*pb.Job{last}},
				},
			}
			if failLastPage {
				server.recentFailureErrors = map[string]error{"next": status.Error(codes.Unavailable, "unavailable")}
			}
			client, cleanup := newBufconnClient(t, server)
			defer cleanup()
			jobs, err := client.GetJobs(context.Background(), heimdall.WithPageLimit(1), heimdall.WithRecentFailuresSince(cutoff))
			if failLastPage {
				if status.Code(err) != codes.Unavailable || jobs != nil {
					t.Fatalf("GetJobs() = %v, %v; want no partial results and Unavailable", jobs, err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if diff := cmp.Diff([]*pb.Job{first, second, failed, last}, jobs, protocmp.Transform()); diff != "" {
					t.Fatal(diff)
				}
			}
			want := []pageCall{
				{PageToken: "", Limit: 1, CompletedJobs: new(false)},
				{PageToken: "next", Limit: 1, CompletedJobs: new(false)},
				{PageToken: "", Limit: 1, ReconciliationEligibleJobs: new(true), EndTimeFrom: &cutoff},
				{PageToken: "next", Limit: 1, ReconciliationEligibleJobs: new(true), EndTimeFrom: &cutoff},
			}
			if diff := cmp.Diff(want, server.getJobsCalls); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}

func newBufconnClient(t *testing.T, server pb.HeimdallServiceServer) (*heimdall.GRPCClient, func()) {
	t.Helper()

	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	pb.RegisterHeimdallServiceServer(grpcServer, server)
	go func() {
		_ = grpcServer.Serve(listener)
	}()

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("NewClient() failed: %v", err)
	}

	return heimdall.NewGRPCClientFromConn(conn), func() {
		_ = conn.Close()
		grpcServer.Stop()
		_ = listener.Close()
	}
}

type recordingHeimdallServer struct {
	pb.UnimplementedHeimdallServiceServer

	upsertJobID            string
	getJobsCalls           []pageCall
	getJobsErrByPage       map[string]error
	getJobsResponses       map[string]*pb.GetJobsResponse
	recentFailureResponses map[string]*pb.GetJobsResponse
	recentFailureErrors    map[string]error
	listAdviceCalls        []pageCall
	listAdviceErrByPage    map[string]error
	listAdviceResponses    map[string]*pb.ListAdviceResponse
	statusAdviceID         string
	statusUpdate           *pb.AdviceReconciliationStatus
}

type pageCall struct {
	PageToken                  string
	Limit                      int32
	ReconciliationEligibleJobs *bool
	CompletedJobs              *bool
	EndTimeFrom                *time.Time
	OnlyActive                 bool
}

func (s *recordingHeimdallServer) CreateOrUpdateJob(_ context.Context, req *pb.CreateOrUpdateJobRequest) (*pb.CreateOrUpdateJobResponse, error) {
	s.upsertJobID = req.GetJobId()
	return &pb.CreateOrUpdateJobResponse{}, nil
}

func (s *recordingHeimdallServer) GetJobs(_ context.Context, req *pb.GetJobsRequest) (*pb.GetJobsResponse, error) {
	var endTimeFrom *time.Time
	if req.EndTimeFrom != nil {
		endTimeFrom = new(req.EndTimeFrom.AsTime())
	}
	s.getJobsCalls = append(s.getJobsCalls, pageCall{
		PageToken:                  req.GetPageToken(),
		Limit:                      req.GetLimit(),
		ReconciliationEligibleJobs: req.ReconciliationEligibleJobs,
		CompletedJobs:              req.CompletedJobs,
		EndTimeFrom:                endTimeFrom,
	})
	if req.EndTimeFrom != nil {
		if err := s.recentFailureErrors[req.GetPageToken()]; err != nil {
			return nil, err
		}
		return s.recentFailureResponses[req.GetPageToken()], nil
	}
	if err := s.getJobsErrByPage[req.GetPageToken()]; err != nil {
		return nil, err
	}
	if resp := s.getJobsResponses[req.GetPageToken()]; resp != nil {
		return resp, nil
	}
	return &pb.GetJobsResponse{}, nil
}

func (s *recordingHeimdallServer) ListAdvice(_ context.Context, req *pb.ListAdviceRequest) (*pb.ListAdviceResponse, error) {
	s.listAdviceCalls = append(s.listAdviceCalls, pageCall{
		PageToken:  req.GetPage(),
		Limit:      req.GetLimit(),
		OnlyActive: req.GetOnlyActive(),
	})
	if err := s.listAdviceErrByPage[req.GetPage()]; err != nil {
		return nil, err
	}
	if resp := s.listAdviceResponses[req.GetPage()]; resp != nil {
		return resp, nil
	}
	return &pb.ListAdviceResponse{}, nil
}

func (s *recordingHeimdallServer) UpdateAdviceReconciliationStatus(
	_ context.Context,
	req *pb.UpdateAdviceReconciliationStatusRequest,
) (*pb.UpdateAdviceReconciliationStatusResponse, error) {
	s.statusAdviceID = req.GetHeimdallJobId()
	s.statusUpdate = req.GetStatus()

	return &pb.UpdateAdviceReconciliationStatusResponse{Status: req.GetStatus()}, nil
}

type acceptingAuditServer struct {
	pb.UnimplementedAuditServiceServer
}

func (*acceptingAuditServer) CreateAuditEvents(context.Context, *pb.CreateAuditEventsRequest) (*pb.CreateAuditEventsResponse, error) {
	return &pb.CreateAuditEventsResponse{}, nil
}
