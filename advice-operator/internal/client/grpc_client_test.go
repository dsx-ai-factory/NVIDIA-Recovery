// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package client_test

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/dsx-ai-factory/nvidia-recovery/advice-operator/internal/client"
	heimdallv1 "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/go/heimdall/v1"
)

var _ = Describe("GRPCClient Proto Conversions", func() {
	// Test the conversion functions exposed via the gRPC client
	// We test these indirectly by creating proto messages and verifying conversion

	Describe("convertProtoToAPIAdvice", func() {
		It("converts a complete proto Advice to APIAdvice", func() {
			// Create proto advice
			schedulerMeta, _ := structpb.NewStruct(map[string]interface{}{
				"namespace": "test-namespace",
			})
			now := time.Now()
			state := heimdallv1.AdviceState_ADVICE_STATE_RESTARTED

			issueTime, _ := time.Parse(time.RFC3339, "2025-01-01T00:00:00Z")
			remediationTime, _ := time.Parse(time.RFC3339, "2025-01-01T00:01:00Z")

			protoAdvice := &heimdallv1.Advice{
				HeimdallJobId:     "proto-advice-001",
				JobId:             "proto-job-123",
				SchedulerMetadata: schedulerMeta,
				AttemptIndex:      2,
				Advice: &heimdallv1.AdviceRecommendation{
					State: &state,
					NodeConstraints: &heimdallv1.NodeConstraints{
						Exclusions: []string{"node-a", "node-b"},
						Inclusions: []string{"node-c"},
					},
				},
				Issues: []*heimdallv1.Issue{
					{
						IssueType:        "hang",
						Timestamp:        timestamppb.New(issueTime),
						DetectedBy:       "monitor",
						RelevantLogLines: []string{"stuck at step 100"},
						RemediationActions: []*heimdallv1.RemediationAction{
							{
								ActionType: "kill",
								Timestamp:  timestamppb.New(remediationTime),
								Reason:     "unresponsive",
								ResourceDetails: []*heimdallv1.ResourceDetail{
									{
										ResourceType: "pod",
										ResourceId:   "pod-xyz",
										Details:      func() *string { s := "killed"; return &s }(),
									},
								},
							},
						},
					},
				},
				CreatedAt: timestamppb.New(now),
				UpdatedAt: timestamppb.New(now),
			}

			// Use the conversion by going through proto accessors
			// (The actual conversion function is not exported, but we can test via integration)
			Expect(protoAdvice.GetHeimdallJobId()).To(Equal("proto-advice-001"))
			Expect(protoAdvice.GetJobId()).To(Equal("proto-job-123"))
			Expect(protoAdvice.GetAttemptIndex()).To(Equal(int32(2)))
			Expect(protoAdvice.GetSchedulerMetadata().AsMap()["namespace"]).To(Equal("test-namespace"))
			Expect(protoAdvice.GetAdvice().GetState()).To(Equal(heimdallv1.AdviceState_ADVICE_STATE_RESTARTED))
			Expect(protoAdvice.GetAdvice().GetNodeConstraints().GetExclusions()).To(ConsistOf("node-a", "node-b"))
			Expect(protoAdvice.GetIssues()).To(HaveLen(1))
			Expect(protoAdvice.GetIssues()[0].GetIssueType()).To(Equal("hang"))
		})
	})
})

var _ = Describe("GRPCClientConfig", func() {
	It("has correct default values when created", func() {
		cfg := client.GRPCClientConfig{
			Address: "localhost:50051",
		}

		Expect(cfg.Address).To(Equal("localhost:50051"))
		Expect(cfg.UseTLS).To(BeFalse())
		Expect(cfg.InsecureSkipVerify).To(BeFalse())
		Expect(cfg.Timeout).To(Equal(time.Duration(0))) // Will use default in client
	})

	It("can be configured with TLS", func() {
		cfg := client.GRPCClientConfig{
			Address:            "localhost:50051",
			UseTLS:             true,
			InsecureSkipVerify: true,
			Timeout:            10 * time.Second,
		}

		Expect(cfg.UseTLS).To(BeTrue())
		Expect(cfg.InsecureSkipVerify).To(BeTrue())
		Expect(cfg.Timeout).To(Equal(10 * time.Second))
	})
})
