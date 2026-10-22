// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package syncer_test

import (
	"context"
	"errors"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/mock/gomock"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"

	heimdallv1alpha1 "github.com/dsx-ai-factory/nvidia-recovery/advice-operator/api/v1alpha1"
	"github.com/dsx-ai-factory/nvidia-recovery/advice-operator/internal/cache"
	heimdallclient "github.com/dsx-ai-factory/nvidia-recovery/advice-operator/internal/client"
	"github.com/dsx-ai-factory/nvidia-recovery/advice-operator/internal/syncer"
)

func TestAdviceSyncer(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "AdviceSyncer Suite")
}

var _ = Describe("AdviceSyncer", func() {
	var (
		k8sClient    client.Client
		mockCtrl     *gomock.Controller
		mockHeimdall *heimdallclient.MockAdviceSource
		adviceSyncer *syncer.AdviceSyncer
		adviceCache  *cache.AdviceCache
		eventChannel chan event.GenericEvent
		ctx          context.Context
	)

	BeforeEach(func() {
		ctx = context.Background()

		// Add our scheme to the default scheme
		Expect(heimdallv1alpha1.AddToScheme(scheme.Scheme)).To(Succeed())

		// Create a fake Kubernetes client
		k8sClient = fake.NewClientBuilder().
			WithScheme(scheme.Scheme).
			Build()

		// Create gomock controller using GinkgoT()
		mockCtrl = gomock.NewController(GinkgoT())
		mockHeimdall = heimdallclient.NewMockAdviceSource(mockCtrl)

		// Create the cache and event channel
		adviceCache = cache.NewAdviceCache()
		eventChannel = make(chan event.GenericEvent, 100)

		// Create the syncer with the mock
		adviceSyncer = &syncer.AdviceSyncer{
			K8sClient:      k8sClient,
			HeimdallClient: mockHeimdall,
			PollInterval:   time.Second,
			Namespaces:     []string{},
			Cache:          adviceCache,
			Events:         eventChannel,
		}
	})

	Describe("syncAdvice", func() {
		Context("when the API returns advice", func() {
			It("updates the cache and emits events", func() {
				// Arrange: Set up mock to return advice
				apiAdvice := createSampleAPIAdvice()
				mockHeimdall.EXPECT().
					ListAdvice(gomock.Any()).
					Return(apiAdvice, nil).
					Times(1)

				// Act: Trigger sync by starting and immediately stopping
				syncCtx, cancel := context.WithCancel(ctx)
				go func() {
					time.Sleep(50 * time.Millisecond)
					cancel()
				}()
				err := adviceSyncer.Start(syncCtx)

				// Assert: No error
				Expect(err).NotTo(HaveOccurred())

				// Cache should be populated
				Expect(adviceCache.AllIDs()).To(HaveLen(2))
				Expect(adviceCache.Get("advice-001")).NotTo(BeNil())
				Expect(adviceCache.Get("advice-002")).NotTo(BeNil())

				// Events should be emitted
				close(eventChannel)
				eventCount := 0
				for range eventChannel {
					eventCount++
				}
				Expect(eventCount).To(BeNumerically(">=", 2))
			})

			It("filters advice to configured namespaces", func() {
				apiAdvice := createSampleAPIAdvice()
				adviceSyncer.Namespaces = []string{"runai-team-a"}
				mockHeimdall.EXPECT().
					ListAdvice(gomock.Any()).
					Return(apiAdvice, nil).
					Times(1)

				syncCtx, cancel := context.WithCancel(ctx)
				go func() {
					time.Sleep(50 * time.Millisecond)
					cancel()
				}()
				err := adviceSyncer.Start(syncCtx)

				Expect(err).NotTo(HaveOccurred())
				Expect(adviceCache.AllIDs()).To(ConsistOf("advice-001"))
				Expect(adviceCache.Get("advice-001")).NotTo(BeNil())
				Expect(adviceCache.Get("advice-002")).To(BeNil())

				close(eventChannel)
				eventCount := 0
				for evt := range eventChannel {
					Expect(evt.Object.GetNamespace()).To(Equal("runai-team-a"))
					eventCount++
				}
				Expect(eventCount).To(BeNumerically(">=", 1))
			})
		})

		Context("when the API returns an error", func() {
			It("continues running without crashing", func() {
				// Arrange: Mock returns an error
				mockHeimdall.EXPECT().
					ListAdvice(gomock.Any()).
					Return(nil, errors.New("connection refused")).
					Times(1)

				// Act: Start syncer briefly
				syncCtx, cancel := context.WithCancel(ctx)
				go func() {
					time.Sleep(50 * time.Millisecond)
					cancel()
				}()
				err := adviceSyncer.Start(syncCtx)

				// Assert: Syncer exits gracefully (no panic, nil error on context cancel)
				Expect(err).NotTo(HaveOccurred())

				// Cache should be empty
				Expect(adviceCache.AllIDs()).To(BeEmpty())
			})
		})

		Context("when the API returns empty advice", func() {
			It("clears the cache", func() {
				// First populate the cache with data
				initialAdvice := []heimdallclient.APIAdvice{{
					HeimdallID:        "advice-initial",
					JobID:             "job-initial",
					SchedulerMetadata: map[string]interface{}{"namespace": "default"},
				}}
				adviceCache.Update(initialAdvice)
				Expect(adviceCache.AllIDs()).To(HaveLen(1))

				// Now mock returns empty
				mockHeimdall.EXPECT().
					ListAdvice(gomock.Any()).
					Return([]heimdallclient.APIAdvice{}, nil).
					Times(1)

				// Act
				syncCtx, cancel := context.WithCancel(ctx)
				go func() {
					time.Sleep(50 * time.Millisecond)
					cancel()
				}()
				err := adviceSyncer.Start(syncCtx)

				// Assert
				Expect(err).NotTo(HaveOccurred())
				Expect(adviceCache.AllIDs()).To(BeEmpty())
			})
		})

		Context("when advice is removed from API", func() {
			It("tracks removed items for deletion", func() {
				// First call returns two items
				initialAdvice := createSampleAPIAdvice()
				mockHeimdall.EXPECT().
					ListAdvice(gomock.Any()).
					Return(initialAdvice, nil).
					Times(1)

				// Start syncer to populate cache
				syncCtx, cancel := context.WithCancel(ctx)
				go func() {
					time.Sleep(50 * time.Millisecond)
					cancel()
				}()
				_ = adviceSyncer.Start(syncCtx)

				// Verify initial state
				Expect(adviceCache.AllIDs()).To(HaveLen(2))

				// Now mock returns only one item
				reducedAdvice := []heimdallclient.APIAdvice{initialAdvice[0]}
				mockHeimdall.EXPECT().
					ListAdvice(gomock.Any()).
					Return(reducedAdvice, nil).
					Times(1)

				// Start syncer again
				eventChannel = make(chan event.GenericEvent, 100)
				adviceSyncer.Events = eventChannel
				syncCtx2, cancel2 := context.WithCancel(ctx)
				go func() {
					time.Sleep(50 * time.Millisecond)
					cancel2()
				}()
				_ = adviceSyncer.Start(syncCtx2)

				// Cache should have only one item now
				Expect(adviceCache.AllIDs()).To(HaveLen(1))
				Expect(adviceCache.Get("advice-001")).NotTo(BeNil())
				Expect(adviceCache.Get("advice-002")).To(BeNil())
			})
		})
	})

	Describe("ToAdviceCRD conversion", func() {
		It("correctly converts APIAdvice to Advice CRD", func() {
			restartState := "Restarted"
			apiAdvice := &heimdallclient.APIAdvice{
				HeimdallID: "test-advice",
				JobID:      "test-job",
				SchedulerMetadata: map[string]interface{}{
					"namespace":       "test-namespace",
					"job_name":        "training-job-abc",
					"job_kind":        "PyTorchJob",
					"job_api_version": "kubeflow.org/v1",
				},
				AttemptIndex: 2,
				Advice: heimdallclient.APIAdviceRecommendation{
					State: &restartState,
					NodeConstraints: &heimdallclient.APINodeConstraints{
						Exclusions: []string{"node-1", "node-2"},
						Inclusions: []string{"node-3"},
					},
				},
			}

			crd, err := apiAdvice.ToAdviceCRD()
			Expect(err).NotTo(HaveOccurred())

			Expect(crd.Name).To(Equal("test-advice"))
			Expect(crd.Namespace).To(Equal("test-namespace"))
			Expect(crd.Annotations).To(HaveKeyWithValue("heimdall.nvidia.com/heimdall-id", "test-advice"))
			Expect(crd.Annotations).To(HaveKeyWithValue("heimdall.nvidia.com/job-kind", "PyTorchJob"))
			Expect(crd.Labels).To(HaveKeyWithValue("heimdall.nvidia.com/job-id", "test-job"))
			Expect(crd.Labels).To(HaveKeyWithValue("heimdall.nvidia.com/attempt-index", "2"))
			Expect(crd.Spec.Job.HeimdallID).To(Equal("test-advice"))
			Expect(crd.Spec.Job.JobID).To(Equal("test-job"))
			Expect(crd.Spec.Job.Name).To(Equal("training-job-abc"))
			Expect(crd.Spec.Job.AttemptIndex).To(Equal(int32(2)))
			Expect(crd.Spec.Job.Kind).To(Equal("PyTorchJob"))
			Expect(crd.Spec.Job.APIVersion).To(Equal("kubeflow.org/v1"))
			Expect(crd.Spec.Advice.State).NotTo(BeNil())
			Expect(*crd.Spec.Advice.State).To(Equal(heimdallv1alpha1.JobState("Restarted")))
			Expect(crd.Spec.Advice.NodeConstraints).NotTo(BeNil())
			Expect(crd.Spec.Advice.NodeConstraints.Exclusions).To(ConsistOf("node-1", "node-2"))
			Expect(crd.Spec.Advice.NodeConstraints.Inclusions).To(ConsistOf("node-3"))
		})

		It("uses 'default' namespace when not specified in metadata", func() {
			apiAdvice := &heimdallclient.APIAdvice{
				HeimdallID:        "test-advice",
				JobID:             "test-job",
				SchedulerMetadata: map[string]interface{}{},
			}

			crd, err := apiAdvice.ToAdviceCRD()
			Expect(err).NotTo(HaveOccurred())

			Expect(crd.Namespace).To(Equal("default"))
		})
	})

	Describe("Cache operations", func() {
		It("provides lookup by job ID", func() {
			apiAdvice := []heimdallclient.APIAdvice{
				{HeimdallID: "advice-1", JobID: "job-A"},
				{HeimdallID: "advice-2", JobID: "job-A"},
				{HeimdallID: "advice-3", JobID: "job-B"},
			}
			adviceCache.Update(apiAdvice)

			// Get all advice for job-A
			results := adviceCache.GetByJobID("job-A")
			Expect(results).To(HaveLen(2))

			// Get all advice for job-B
			results = adviceCache.GetByJobID("job-B")
			Expect(results).To(HaveLen(1))

			// Get advice for non-existent job
			results = adviceCache.GetByJobID("job-C")
			Expect(results).To(BeEmpty())
		})

		It("lists all advice", func() {
			apiAdvice := []heimdallclient.APIAdvice{
				{HeimdallID: "advice-1", JobID: "job-1"},
				{HeimdallID: "advice-2", JobID: "job-2"},
			}
			adviceCache.Update(apiAdvice)

			all := adviceCache.List()
			Expect(all).To(HaveLen(2))
		})

		It("checks existence", func() {
			apiAdvice := []heimdallclient.APIAdvice{
				{HeimdallID: "advice-1", JobID: "job-1"},
			}
			adviceCache.Update(apiAdvice)

			Expect(adviceCache.Exists("advice-1")).To(BeTrue())
			Expect(adviceCache.Exists("advice-2")).To(BeFalse())
		})
	})
})

// Helper functions to create test data

func createSampleAPIAdvice() []heimdallclient.APIAdvice {
	restartState := "Restarted"
	terminatedState := "Terminated"

	return []heimdallclient.APIAdvice{
		{
			HeimdallID: "advice-001",
			JobID:      "job-12345",
			SchedulerMetadata: map[string]interface{}{
				"namespace": "runai-team-a",
			},
			AttemptIndex: 1,
			Advice: heimdallclient.APIAdviceRecommendation{
				State: &restartState,
				NodeConstraints: &heimdallclient.APINodeConstraints{
					Exclusions: []string{"node-gpu-01", "node-gpu-02"},
				},
			},
			Issues: []heimdallclient.APIIssue{
				{
					IssueType:  "crash",
					DetectedBy: "heimdall-log-analyzer",
				},
			},
		},
		{
			HeimdallID: "advice-002",
			JobID:      "job-67890",
			SchedulerMetadata: map[string]interface{}{
				"namespace": "runai-team-b",
			},
			AttemptIndex: 0,
			Advice: heimdallclient.APIAdviceRecommendation{
				State: &terminatedState,
				NodeConstraints: &heimdallclient.APINodeConstraints{
					Inclusions: []string{"node-gpu-03", "node-gpu-04", "node-gpu-05"},
				},
			},
		},
	}
}
