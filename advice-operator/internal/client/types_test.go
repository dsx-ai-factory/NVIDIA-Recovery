// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package client_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/types"

	heimdallv1alpha1 "github.com/dsx-ai-factory/nvidia-recovery/advice-operator/api/v1alpha1"
	"github.com/dsx-ai-factory/nvidia-recovery/advice-operator/internal/client"
)

var _ = Describe("APIAdvice", func() {
	Describe("GetNamespace", func() {
		It("returns namespace from scheduler metadata", func() {
			advice := &client.APIAdvice{
				SchedulerMetadata: map[string]interface{}{
					"namespace": "runai-team-a",
				},
			}

			Expect(advice.GetNamespace()).To(Equal("runai-team-a"))
		})

		It("returns empty string when scheduler metadata is nil", func() {
			advice := &client.APIAdvice{
				SchedulerMetadata: nil,
			}

			Expect(advice.GetNamespace()).To(BeEmpty())
		})

		It("returns empty string when namespace is not in metadata", func() {
			advice := &client.APIAdvice{
				SchedulerMetadata: map[string]interface{}{
					"other": "value",
				},
			}

			Expect(advice.GetNamespace()).To(BeEmpty())
		})

		It("returns empty string when namespace is not a string", func() {
			advice := &client.APIAdvice{
				SchedulerMetadata: map[string]interface{}{
					"namespace": 12345, // Not a string
				},
			}

			Expect(advice.GetNamespace()).To(BeEmpty())
		})
	})

	Describe("GetJobKind", func() {
		It("returns job_kind from scheduler metadata", func() {
			advice := &client.APIAdvice{
				SchedulerMetadata: map[string]interface{}{
					"job_kind": "PyTorchJob",
				},
			}

			Expect(advice.GetJobKind()).To(Equal("PyTorchJob"))
		})

		It("returns empty string when scheduler metadata is nil", func() {
			advice := &client.APIAdvice{
				SchedulerMetadata: nil,
			}

			Expect(advice.GetJobKind()).To(BeEmpty())
		})

		It("returns empty string when job_kind is not in metadata", func() {
			advice := &client.APIAdvice{
				SchedulerMetadata: map[string]interface{}{
					"other": "value",
				},
			}

			Expect(advice.GetJobKind()).To(BeEmpty())
		})
	})

	Describe("GetJobName", func() {
		It("returns job_name from scheduler metadata", func() {
			advice := &client.APIAdvice{
				SchedulerMetadata: map[string]interface{}{
					"job_name": "training-job-abc",
				},
			}

			Expect(advice.GetJobName()).To(Equal("training-job-abc"))
		})

		It("returns empty string when scheduler metadata is nil", func() {
			advice := &client.APIAdvice{SchedulerMetadata: nil}

			Expect(advice.GetJobName()).To(BeEmpty())
		})

		It("returns empty string when job_name is not in metadata", func() {
			advice := &client.APIAdvice{
				SchedulerMetadata: map[string]interface{}{
					"other": "value",
				},
			}

			Expect(advice.GetJobName()).To(BeEmpty())
		})

		It("returns empty string when job_name is not a string", func() {
			advice := &client.APIAdvice{
				SchedulerMetadata: map[string]interface{}{
					"job_name": 12345,
				},
			}

			Expect(advice.GetJobName()).To(BeEmpty())
		})
	})

	Describe("GetOwnerReference", func() {
		It("returns owner reference from scheduler metadata", func() {
			advice := &client.APIAdvice{
				SchedulerMetadata: map[string]interface{}{
					"owner_api_version": "run.ai/v2alpha1",
					"owner_kind":        "DistributedWorkload",
					"owner_name":        "training-job-abc",
					"owner_uid":         "bb17fcd2-11ef-4fe8-9ab4-ad5139cd8ee3",
				},
			}

			ownerRef := advice.GetOwnerReference()
			Expect(ownerRef).NotTo(BeNil())
			Expect(ownerRef.APIVersion).To(Equal("run.ai/v2alpha1"))
			Expect(ownerRef.Kind).To(Equal("DistributedWorkload"))
			Expect(ownerRef.Name).To(Equal("training-job-abc"))
			Expect(ownerRef.UID).To(Equal(types.UID("bb17fcd2-11ef-4fe8-9ab4-ad5139cd8ee3")))
		})

		It("returns nil when owner metadata is incomplete", func() {
			advice := &client.APIAdvice{
				SchedulerMetadata: map[string]interface{}{
					"owner_api_version": "run.ai/v2alpha1",
					"owner_kind":        "DistributedWorkload",
				},
			}

			Expect(advice.GetOwnerReference()).To(BeNil())
		})

		It("returns nil when owner metadata has wrong types", func() {
			advice := &client.APIAdvice{
				SchedulerMetadata: map[string]interface{}{
					"owner_api_version": "run.ai/v2alpha1",
					"owner_kind":        "DistributedWorkload",
					"owner_name":        12345,
					"owner_uid":         "bb17fcd2-11ef-4fe8-9ab4-ad5139cd8ee3",
				},
			}

			Expect(advice.GetOwnerReference()).To(BeNil())
		})
	})

	Describe("ToAdviceCRD", func() {
		It("converts a complete APIAdvice to Advice CRD", func() {
			state := "Restarted"
			advice := &client.APIAdvice{
				HeimdallID: "advice-123",
				JobID:      "job-456",
				SchedulerMetadata: map[string]interface{}{
					"namespace":         "test-namespace",
					"job_name":          "training-job-abc",
					"job_kind":          "PyTorchJob",
					"job_api_version":   "kubeflow.org/v1",
					"owner_api_version": "run.ai/v2alpha1",
					"owner_kind":        "DistributedWorkload",
					"owner_name":        "training-job-abc",
					"owner_uid":         "bb17fcd2-11ef-4fe8-9ab4-ad5139cd8ee3",
				},
				AttemptIndex: 3,
				Advice: client.APIAdviceRecommendation{
					State: &state,
					NodeConstraints: &client.APINodeConstraints{
						Exclusions: []string{"node-1", "node-2"},
						Inclusions: []string{"node-3"},
					},
				},
				Issues: []client.APIIssue{
					{
						IssueType:        "crash",
						Timestamp:        "2025-01-01T00:00:00Z",
						DetectedBy:       "analyzer",
						RelevantLogLines: []string{"error line 1", "error line 2"},
						RemediationActions: []client.APIRemediationAction{
							{
								ActionType: "restart",
								Timestamp:  "2025-01-01T00:01:00Z",
								Reason:     "recovery attempt",
								ResourceDetails: []client.APIResourceDetail{
									{
										ResourceType: "pod",
										ResourceID:   "pod-123",
										Details:      "restarted",
									},
								},
							},
						},
					},
				},
			}

			crd, err := advice.ToAdviceCRD()
			Expect(err).NotTo(HaveOccurred())

			// Verify basic fields
			Expect(crd.Name).To(Equal("advice-123"))
			Expect(crd.Namespace).To(Equal("test-namespace"))

			// Verify annotations and labels
			Expect(crd.Annotations).To(HaveKeyWithValue("heimdall.nvidia.com/heimdall-id", "advice-123"))
			Expect(crd.Annotations).To(HaveKeyWithValue("heimdall.nvidia.com/job-kind", "PyTorchJob"))
			Expect(crd.Labels).To(HaveKeyWithValue("heimdall.nvidia.com/job-id", "job-456"))
			Expect(crd.Labels).To(HaveKeyWithValue("heimdall.nvidia.com/attempt-index", "3"))
			Expect(crd.OwnerReferences).To(HaveLen(1))
			Expect(crd.OwnerReferences[0].APIVersion).To(Equal("run.ai/v2alpha1"))
			Expect(crd.OwnerReferences[0].Kind).To(Equal("DistributedWorkload"))
			Expect(crd.OwnerReferences[0].Name).To(Equal("training-job-abc"))
			Expect(crd.OwnerReferences[0].UID).To(Equal(types.UID("bb17fcd2-11ef-4fe8-9ab4-ad5139cd8ee3")))

			// Verify job reference
			Expect(crd.Spec.Job.HeimdallID).To(Equal("advice-123"))
			Expect(crd.Spec.Job.JobID).To(Equal("job-456"))
			Expect(crd.Spec.Job.Name).To(Equal("training-job-abc"))
			Expect(crd.Spec.Job.AttemptIndex).To(Equal(int32(3)))
			Expect(crd.Spec.Job.Kind).To(Equal("PyTorchJob"))
			Expect(crd.Spec.Job.APIVersion).To(Equal("kubeflow.org/v1"))

			// Verify advice recommendation
			Expect(crd.Spec.Advice.State).NotTo(BeNil())
			Expect(*crd.Spec.Advice.State).To(Equal(heimdallv1alpha1.JobState("Restarted")))
			Expect(crd.Spec.Advice.NodeConstraints).NotTo(BeNil())
			Expect(crd.Spec.Advice.NodeConstraints.Exclusions).To(ConsistOf("node-1", "node-2"))
			Expect(crd.Spec.Advice.NodeConstraints.Inclusions).To(ConsistOf("node-3"))

			// Verify issues
			Expect(crd.Spec.Issues).To(HaveLen(1))
			Expect(crd.Spec.Issues[0].IssueType).To(Equal("crash"))
			Expect(crd.Spec.Issues[0].DetectedBy).To(Equal("analyzer"))
			Expect(crd.Spec.Issues[0].RelevantLogLines).To(ConsistOf("error line 1", "error line 2"))

			// Verify remediation actions
			Expect(crd.Spec.Issues[0].RemediationActions).To(HaveLen(1))
			Expect(crd.Spec.Issues[0].RemediationActions[0].ActionType).To(Equal("restart"))
			Expect(crd.Spec.Issues[0].RemediationActions[0].ResourceDetails).To(HaveLen(1))
			Expect(crd.Spec.Issues[0].RemediationActions[0].ResourceDetails[0].ResourceId).To(Equal("pod-123"))
		})

		It("uses 'default' namespace when not specified", func() {
			advice := &client.APIAdvice{
				HeimdallID:        "advice-123",
				JobID:             "job-456",
				SchedulerMetadata: map[string]interface{}{},
			}

			crd, err := advice.ToAdviceCRD()
			Expect(err).NotTo(HaveOccurred())

			Expect(crd.Namespace).To(Equal("default"))
		})

		It("handles nil advice state", func() {
			advice := &client.APIAdvice{
				HeimdallID: "advice-123",
				JobID:      "job-456",
				Advice: client.APIAdviceRecommendation{
					State: nil,
				},
			}

			crd, err := advice.ToAdviceCRD()
			Expect(err).NotTo(HaveOccurred())

			Expect(crd.Spec.Advice.State).To(BeNil())
		})

		It("handles nil node constraints", func() {
			advice := &client.APIAdvice{
				HeimdallID: "advice-123",
				JobID:      "job-456",
				Advice: client.APIAdviceRecommendation{
					NodeConstraints: nil,
				},
			}

			crd, err := advice.ToAdviceCRD()
			Expect(err).NotTo(HaveOccurred())

			Expect(crd.Spec.Advice.NodeConstraints).To(BeNil())
		})

		It("handles empty issues", func() {
			advice := &client.APIAdvice{
				HeimdallID: "advice-123",
				JobID:      "job-456",
				Issues:     []client.APIIssue{},
			}

			crd, err := advice.ToAdviceCRD()
			Expect(err).NotTo(HaveOccurred())

			Expect(crd.Spec.Issues).To(BeEmpty())
		})

		It("defaults empty detectedBy to a non-empty value", func() {
			advice := &client.APIAdvice{
				HeimdallID: "advice-123",
				JobID:      "job-456",
				Issues: []client.APIIssue{
					{
						IssueType:  "crash",
						Timestamp:  "2025-01-01T00:00:00Z",
						DetectedBy: "   ",
					},
				},
			}

			crd, err := advice.ToAdviceCRD()
			Expect(err).NotTo(HaveOccurred())

			Expect(crd.Spec.Issues).To(HaveLen(1))
			Expect(crd.Spec.Issues[0].DetectedBy).To(Equal("Unknown"))
		})

		It("returns error when HeimdallID is empty", func() {
			advice := &client.APIAdvice{
				HeimdallID: "",
				JobID:      "job-456",
			}

			crd, err := advice.ToAdviceCRD()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("HeimdallID is required"))
			Expect(crd).To(BeNil())
		})

		It("returns error when JobID is empty", func() {
			advice := &client.APIAdvice{
				HeimdallID: "advice-123",
				JobID:      "",
			}

			crd, err := advice.ToAdviceCRD()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("JobID is required"))
			Expect(crd).To(BeNil())
		})
	})
})
