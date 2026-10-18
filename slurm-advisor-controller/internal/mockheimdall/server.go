// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package mockheimdall

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"sync"

	"github.com/cockroachdb/errors"
	pb "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/go/heimdall/v1"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Server struct {
	pb.UnimplementedHeimdallServiceServer

	mu      sync.Mutex
	upserts []*pb.CreateOrUpdateJobRequest
	advice  []*pb.Advice
}

func NewServer(advice []*pb.Advice) *Server {
	return &Server{advice: append([]*pb.Advice(nil), advice...)}
}

func (s *Server) CreateOrUpdateJob(_ context.Context, req *pb.CreateOrUpdateJobRequest) (*pb.CreateOrUpdateJobResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.upserts = append(s.upserts, req)

	return &pb.CreateOrUpdateJobResponse{
		Job: &pb.Job{
			HeimdallJobId:     req.GetHeimdallJobId(),
			JobId:             req.GetJobId(),
			WorkflowId:        req.GetWorkflowId(),
			AttemptIndex:      req.GetAttemptIndex(),
			Nodes:             append([]string(nil), req.GetAllocatedNodes()...),
			NodeConstraints:   req.GetNodeConstraints(),
			State:             req.GetState(),
			SchedulerMetadata: req.GetSchedulerMetadata(),
			JobName:           req.JobName,
			RecoveryConfig:    req.GetRecoveryConfig(),
		},
	}, nil
}

func (s *Server) GetJobs(_ context.Context, req *pb.GetJobsRequest) (*pb.GetJobsResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	jobs := make([]*pb.Job, 0, len(s.upserts))
	for _, upsert := range s.upserts {
		jobs = append(jobs, jobFromUpsert(upsert))
	}

	offset, err := strconv.Atoi(req.GetPageToken())
	if err != nil || offset < 0 {
		offset = 0
	}
	limit := int(req.GetLimit())
	if limit <= 0 || limit > 100 {
		limit = 100
	}
	if offset >= len(jobs) {
		return &pb.GetJobsResponse{}, nil
	}

	end := min(offset+limit, len(jobs))
	resp := &pb.GetJobsResponse{
		Jobs:  append([]*pb.Job(nil), jobs[offset:end]...),
		Count: int32(end - offset),
	}
	if end < len(jobs) {
		next := strconv.Itoa(end)
		resp.NextPageToken = &next
	}

	return resp, nil
}

func (s *Server) ListAdvice(_ context.Context, req *pb.ListAdviceRequest) (*pb.ListAdviceResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	offset, err := strconv.Atoi(req.GetPage())
	if err != nil || offset < 0 {
		offset = 0
	}
	limit := int(req.GetLimit())
	if limit <= 0 || limit > 100 {
		limit = 100
	}
	if offset >= len(s.advice) {
		return &pb.ListAdviceResponse{}, nil
	}

	end := min(offset+limit, len(s.advice))
	resp := &pb.ListAdviceResponse{
		Advice: append([]*pb.Advice(nil), s.advice[offset:end]...),
		Count:  int32(end - offset),
	}
	if end < len(s.advice) {
		next := strconv.Itoa(end)
		resp.NextPageToken = &next
	}

	return resp, nil
}

func jobFromUpsert(req *pb.CreateOrUpdateJobRequest) *pb.Job {
	return &pb.Job{
		HeimdallJobId:     req.GetHeimdallJobId(),
		JobId:             req.GetJobId(),
		WorkflowId:        req.GetWorkflowId(),
		AttemptIndex:      req.GetAttemptIndex(),
		Nodes:             append([]string(nil), req.GetAllocatedNodes()...),
		NodeConstraints:   req.GetNodeConstraints(),
		State:             req.GetState(),
		SchedulerMetadata: req.GetSchedulerMetadata(),
		JobName:           req.JobName,
		RecoveryConfig:    req.GetRecoveryConfig(),
	}
}

func (s *Server) Upserts() []*pb.CreateOrUpdateJobRequest {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]*pb.CreateOrUpdateJobRequest(nil), s.upserts...)
}

func (s *Server) SetAdvice(advice []*pb.Advice) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.advice = append([]*pb.Advice(nil), advice...)
}

func (s *Server) RegisterDebugHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/upserts", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(UpsertSummaries(s.Upserts())); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})
	mux.HandleFunc("/advice", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(AdviceSummaries(s.currentAdvice())); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
		case http.MethodPut:
			var fixtures []DebugAdvice
			if err := json.NewDecoder(r.Body).Decode(&fixtures); err != nil {
				http.Error(w, fmt.Sprintf("decode advice fixtures: %v", err), http.StatusBadRequest)
				return
			}
			advice, err := buildAdvice(fixtures)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			s.SetAdvice(advice)
			w.WriteHeader(http.StatusNoContent)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})
}

type UpsertSummary struct {
	JobID             string         `json:"job_id"`
	AttemptIndex      int32          `json:"attempt_index"`
	State             string         `json:"state"`
	WorkflowID        string         `json:"workflow_id"`
	SchedulerMetadata map[string]any `json:"scheduler_metadata,omitempty"`
}

func UpsertSummaries(requests []*pb.CreateOrUpdateJobRequest) []UpsertSummary {
	out := make([]UpsertSummary, 0, len(requests))
	for _, req := range requests {
		var schedulerMetadata map[string]any
		if req.GetSchedulerMetadata() != nil {
			schedulerMetadata = req.GetSchedulerMetadata().AsMap()
		}
		out = append(out, UpsertSummary{
			JobID:             req.GetJobId(),
			AttemptIndex:      req.GetAttemptIndex(),
			State:             req.GetState().String(),
			WorkflowID:        req.GetWorkflowId(),
			SchedulerMetadata: schedulerMetadata,
		})
	}

	return out
}

type AdviceSummary struct {
	AdviceID     string `json:"advice_id"`
	JobID        string `json:"job_id"`
	AttemptIndex int32  `json:"attempt_index"`
	State        string `json:"state"`
}

func AdviceSummaries(advice []*pb.Advice) []AdviceSummary {
	out := make([]AdviceSummary, 0, len(advice))
	for _, item := range advice {
		out = append(out, AdviceSummary{
			AdviceID:     item.GetHeimdallJobId(),
			JobID:        item.GetJobId(),
			AttemptIndex: item.GetAttemptIndex(),
			State:        item.GetAdvice().GetState().String(),
		})
	}

	return out
}

type DebugAdvice struct {
	AdviceID          string         `json:"advice_id"`
	JobID             string         `json:"job_id"`
	AttemptIndex      int32          `json:"attempt_index"`
	State             string         `json:"state"`
	Inclusions        []string       `json:"inclusions"`
	Exclusions        []string       `json:"exclusions"`
	SchedulerMetadata map[string]any `json:"scheduler_metadata"`
}

func (s *Server) currentAdvice() []*pb.Advice {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]*pb.Advice(nil), s.advice...)
}

func buildAdvice(fixtures []DebugAdvice) ([]*pb.Advice, error) {
	advice := make([]*pb.Advice, 0, len(fixtures))
	for _, fixture := range fixtures {
		item, err := buildAdviceItem(fixture)
		if err != nil {
			return nil, err
		}
		advice = append(advice, item)
	}

	return advice, nil
}

func buildAdviceItem(fixture DebugAdvice) (*pb.Advice, error) {
	if fixture.AdviceID == "" {
		return nil, errors.New("advice_id is required")
	}
	if fixture.JobID == "" {
		return nil, errors.New("job_id is required")
	}

	var state *pb.AdviceState
	if fixture.State != "" {
		value, ok := pb.AdviceState_value[fixture.State]
		if !ok {
			return nil, errors.Newf("unsupported advice state %q", fixture.State)
		}
		parsed := pb.AdviceState(value)
		state = &parsed
	}

	var constraints *pb.NodeConstraints
	if len(fixture.Inclusions) > 0 || len(fixture.Exclusions) > 0 {
		constraints = &pb.NodeConstraints{
			Inclusions: append([]string(nil), fixture.Inclusions...),
			Exclusions: append([]string(nil), fixture.Exclusions...),
		}
	}

	metadata, err := structpb.NewStruct(fixture.SchedulerMetadata)
	if err != nil {
		return nil, errors.Wrapf(err, "build scheduler_metadata for advice %q", fixture.AdviceID)
	}

	return &pb.Advice{
		HeimdallJobId:     fixture.AdviceID,
		JobId:             fixture.JobID,
		AttemptIndex:      fixture.AttemptIndex,
		SchedulerMetadata: metadata,
		CreatedAt:         timestamppb.Now(),
		Advice: &pb.AdviceRecommendation{
			State:           state,
			NodeConstraints: constraints,
		},
	}, nil
}
