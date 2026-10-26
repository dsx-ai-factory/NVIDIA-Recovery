// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	heimdallv1alpha1 "github.com/dsx-ai-factory/nvidia-recovery/advice-operator/api/v1alpha1"
	"github.com/dsx-ai-factory/nvidia-recovery/advice-operator/internal/cache"
	heimdallclient "github.com/dsx-ai-factory/nvidia-recovery/advice-operator/internal/client"
)

// testCounter is used to generate unique resource names across tests
var testCounter int

var _ = Describe("AdviceReconciler", func() {
	const (
		adviceNamespace = "default"
		timeout         = time.Second * 10
		interval        = time.Millisecond * 250
		restartState    = "ADVICE_STATE_RESTARTED"
	)

	var (
		reconciler         *AdviceReconciler
		adviceCache        *cache.AdviceCache
		typeNamespacedName types.NamespacedName
		// Unique names per test
		adviceName string
		jobID      string
	)

	BeforeEach(func() {
		// Generate unique names for this test
		testCounter++
		suffix := fmt.Sprintf("%d", testCounter)
		adviceName = "test-advice-" + suffix
		jobID = "test-job-" + suffix

		adviceCache = cache.NewAdviceCache()
		reconciler = &AdviceReconciler{
			Client:    k8sClient,
			Scheme:    k8sClient.Scheme(),
			APIReader: mgr.GetAPIReader(),
			Cache:     adviceCache,
		}

		typeNamespacedName = types.NamespacedName{
			Name:      adviceName,
			Namespace: adviceNamespace,
		}
	})

	Describe("Reconcile with Cache", func() {
		Context("when advice is in cache", func() {
			BeforeEach(func() {
				// Populate the cache with advice
				state := restartState
				apiAdvice := []heimdallclient.APIAdvice{{
					HeimdallID:   adviceName,
					JobID:        jobID,
					AttemptIndex: 1,
					SchedulerMetadata: map[string]interface{}{
						"namespace":       adviceNamespace,
						"job_name":        "job-ctrl-123",
						"job_kind":        "PyTorchJob",
						"job_api_version": "kubeflow.org/v1",
					},
					Advice: heimdallclient.APIAdviceRecommendation{
						State: &state,
						NodeConstraints: &heimdallclient.APINodeConstraints{
							Exclusions: []string{"node-1"},
						},
					},
				}}
				adviceCache.Update(apiAdvice)
			})

			AfterEach(func() {
				// Cleanup any remaining Advice
				advice := &heimdallv1alpha1.Advice{}
				if err := k8sClient.Get(ctx, typeNamespacedName, advice); err == nil {
					Expect(k8sClient.Delete(ctx, advice)).To(Succeed())
				}
			})

			It("creates Advice CRD from cache", func() {
				result, err := reconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: typeNamespacedName,
				})

				Expect(err).NotTo(HaveOccurred())
				Expect(result).To(Equal(reconcile.Result{}))

				// Verify Advice was created
				var advice heimdallv1alpha1.Advice
				Eventually(func() error {
					return k8sClient.Get(ctx, typeNamespacedName, &advice)
				}, timeout, interval).Should(Succeed())

				// Check spec
				Expect(advice.Spec.Job.JobID).To(Equal(jobID))
				Expect(advice.Spec.Job.Name).To(Equal("job-ctrl-123"))
				Expect(advice.Spec.Job.AttemptIndex).To(Equal(int32(1)))
				Expect(advice.Spec.Job.Kind).To(Equal("PyTorchJob"))
				Expect(advice.Spec.Job.APIVersion).To(Equal("kubeflow.org/v1"))
				Expect(advice.Spec.Advice.State).NotTo(BeNil())
				Expect(string(*advice.Spec.Advice.State)).To(Equal("ADVICE_STATE_RESTARTED"))
				Expect(advice.Labels).To(HaveKeyWithValue("heimdall.nvidia.com/job-id", jobID))
				Expect(advice.Labels).To(HaveKeyWithValue("heimdall.nvidia.com/attempt-index", "1"))
			})

			It("updates existing Advice CRD when cache changes", func() {
				// First reconcile to create
				_, err := reconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: typeNamespacedName,
				})
				Expect(err).NotTo(HaveOccurred())

				// Wait for the Advice to be visible in the cached client
				Eventually(func() error {
					return k8sClient.Get(ctx, typeNamespacedName, &heimdallv1alpha1.Advice{})
				}, timeout, interval).Should(Succeed())

				// Update cache with new data
				terminatedState := "ADVICE_STATE_TERMINATED"
				apiAdvice := []heimdallclient.APIAdvice{{
					HeimdallID:   adviceName,
					JobID:        jobID,
					AttemptIndex: 2, // Changed
					SchedulerMetadata: map[string]interface{}{
						"namespace":       adviceNamespace,
						"job_name":        "job-ctrl-123",
						"job_kind":        "PyTorchJob",
						"job_api_version": "kubeflow.org/v1",
					},
					Advice: heimdallclient.APIAdviceRecommendation{
						State: &terminatedState, // Changed
						NodeConstraints: &heimdallclient.APINodeConstraints{
							Exclusions: []string{"node-1", "node-2"}, // Added
						},
					},
				}}
				adviceCache.Update(apiAdvice)

				// Reconcile again
				_, err = reconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: typeNamespacedName,
				})
				Expect(err).NotTo(HaveOccurred())

				// Verify updates (use Eventually since cached client may have stale data)
				Eventually(func() int32 {
					var advice heimdallv1alpha1.Advice
					if err := k8sClient.Get(ctx, typeNamespacedName, &advice); err != nil {
						return 0
					}
					return advice.Spec.Job.AttemptIndex
				}, timeout, interval).Should(Equal(int32(2)))

				var advice heimdallv1alpha1.Advice
				Expect(k8sClient.Get(ctx, typeNamespacedName, &advice)).To(Succeed())
				Expect(advice.Spec.Job.Name).To(Equal("job-ctrl-123"))
				Expect(advice.Spec.Job.Kind).To(Equal("PyTorchJob"))
				Expect(advice.Spec.Job.APIVersion).To(Equal("kubeflow.org/v1"))
				Expect(string(*advice.Spec.Advice.State)).To(Equal("ADVICE_STATE_TERMINATED"))
				Expect(advice.Spec.Advice.NodeConstraints.Exclusions).To(ContainElements("node-1", "node-2"))
				Expect(advice.Labels).To(HaveKeyWithValue("heimdall.nvidia.com/job-id", jobID))
				Expect(advice.Labels).To(HaveKeyWithValue("heimdall.nvidia.com/attempt-index", "2"))
			})
		})

		Context("when advice is removed from cache", func() {
			BeforeEach(func() {
				// Populate cache and reconcile to create Advice
				state := restartState
				apiAdvice := []heimdallclient.APIAdvice{{
					HeimdallID:   adviceName,
					JobID:        jobID,
					AttemptIndex: 1,
					SchedulerMetadata: map[string]interface{}{
						"namespace":       adviceNamespace,
						"job_name":        "job-ctrl-123",
						"job_kind":        "PyTorchJob",
						"job_api_version": "kubeflow.org/v1",
					},
					Advice: heimdallclient.APIAdviceRecommendation{
						State: &state,
					},
				}}
				adviceCache.Update(apiAdvice)

				_, err := reconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: typeNamespacedName,
				})
				Expect(err).NotTo(HaveOccurred())

				// Verify Advice exists
				Eventually(func() error {
					return k8sClient.Get(ctx, typeNamespacedName, &heimdallv1alpha1.Advice{})
				}, timeout, interval).Should(Succeed())
			})

			It("deletes Advice CRD when removed from cache", func() {
				// Clear the cache (simulate API returning empty list)
				adviceCache.Update([]heimdallclient.APIAdvice{})

				// Reconcile should delete
				_, err := reconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: typeNamespacedName,
				})
				Expect(err).NotTo(HaveOccurred())

				// Verify Advice was deleted
				Eventually(func() bool {
					err := k8sClient.Get(ctx, typeNamespacedName, &heimdallv1alpha1.Advice{})
					return apierrors.IsNotFound(err)
				}, timeout, interval).Should(BeTrue())
			})
		})

		Context("when advice is not in cache", func() {
			It("returns without error for non-existent advice", func() {
				// Don't populate cache - advice doesn't exist anywhere
				result, err := reconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: typeNamespacedName,
				})

				// Should complete successfully with nothing to do
				Expect(err).NotTo(HaveOccurred())
				Expect(result).To(Equal(reconcile.Result{}))
			})
		})

		Context("when advice has an owner reference", func() {
			var ownerName string

			BeforeEach(func() {
				ownerName = "owner-" + adviceName
			})

			AfterEach(func() {
				owner := &corev1.ConfigMap{}
				ownerKey := types.NamespacedName{Name: ownerName, Namespace: adviceNamespace}
				if err := k8sClient.Get(ctx, ownerKey, owner); err == nil {
					Expect(k8sClient.Delete(ctx, owner)).To(Succeed())
				}

				advice := &heimdallv1alpha1.Advice{}
				if err := k8sClient.Get(ctx, typeNamespacedName, advice); err == nil {
					Expect(k8sClient.Delete(ctx, advice)).To(Succeed())
				}
			})

			It("does not create Advice CRD when the referenced owner is missing", func() {
				state := restartState
				apiAdvice := []heimdallclient.APIAdvice{{
					HeimdallID:   adviceName,
					JobID:        jobID,
					AttemptIndex: 1,
					SchedulerMetadata: map[string]interface{}{
						"namespace":         adviceNamespace,
						"owner_api_version": "v1",
						"owner_kind":        "ConfigMap",
						"owner_name":        ownerName,
						"owner_uid":         "11111111-1111-1111-1111-111111111111",
					},
					Advice: heimdallclient.APIAdviceRecommendation{
						State: &state,
					},
				}}
				adviceCache.Update(apiAdvice)

				result, err := reconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: typeNamespacedName,
				})

				Expect(err).NotTo(HaveOccurred())
				Expect(result).To(Equal(reconcile.Result{}))
				Consistently(func() bool {
					err := k8sClient.Get(ctx, typeNamespacedName, &heimdallv1alpha1.Advice{})
					return apierrors.IsNotFound(err)
				}, timeout, interval).Should(BeTrue())
			})

			It("deletes Advice CRD and does not recreate it after the owner is removed", func() {
				owner := &corev1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{
						Name:      ownerName,
						Namespace: adviceNamespace,
						UID:       types.UID("22222222-2222-2222-2222-222222222222"),
					},
				}
				Expect(k8sClient.Create(ctx, owner)).To(Succeed())

				state := restartState
				apiAdvice := []heimdallclient.APIAdvice{{
					HeimdallID:   adviceName,
					JobID:        jobID,
					AttemptIndex: 1,
					SchedulerMetadata: map[string]interface{}{
						"namespace":         adviceNamespace,
						"owner_api_version": "v1",
						"owner_kind":        "ConfigMap",
						"owner_name":        ownerName,
						"owner_uid":         string(owner.UID),
					},
					Advice: heimdallclient.APIAdviceRecommendation{
						State: &state,
					},
				}}
				adviceCache.Update(apiAdvice)

				_, err := reconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: typeNamespacedName,
				})
				Expect(err).NotTo(HaveOccurred())

				Eventually(func() error {
					return k8sClient.Get(ctx, typeNamespacedName, &heimdallv1alpha1.Advice{})
				}, timeout, interval).Should(Succeed())

				Expect(k8sClient.Delete(ctx, owner)).To(Succeed())

				_, err = reconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: typeNamespacedName,
				})
				Expect(err).NotTo(HaveOccurred())

				Eventually(func() bool {
					err := k8sClient.Get(ctx, typeNamespacedName, &heimdallv1alpha1.Advice{})
					return apierrors.IsNotFound(err)
				}, timeout, interval).Should(BeTrue())

				_, err = reconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: typeNamespacedName,
				})
				Expect(err).NotTo(HaveOccurred())

				Consistently(func() bool {
					err := k8sClient.Get(ctx, typeNamespacedName, &heimdallv1alpha1.Advice{})
					return apierrors.IsNotFound(err)
				}, timeout, interval).Should(BeTrue())
			})
		})
	})

	Describe("Cache operations", func() {
		It("tracks added, updated, and removed items", func() {
			// Initial populate
			apiAdvice := []heimdallclient.APIAdvice{
				{HeimdallID: "advice-1", JobID: "job-1"},
				{HeimdallID: "advice-2", JobID: "job-2"},
			}
			added, updated, removed := adviceCache.Update(apiAdvice)
			Expect(added).To(ConsistOf("advice-1", "advice-2"))
			Expect(updated).To(BeEmpty())
			Expect(removed).To(BeEmpty())

			// Update with changes
			apiAdvice = []heimdallclient.APIAdvice{
				{HeimdallID: "advice-1", JobID: "job-1", AttemptIndex: 1}, // Updated
				{HeimdallID: "advice-3", JobID: "job-3"},                  // Added
			}
			added, updated, removed = adviceCache.Update(apiAdvice)
			Expect(added).To(ConsistOf("advice-3"))
			Expect(updated).To(ConsistOf("advice-1"))
			// removed is now []RemovedAdvice, extract IDs for comparison
			removedIDs := make([]string, len(removed))
			for i, r := range removed {
				removedIDs[i] = r.ID
			}
			Expect(removedIDs).To(ConsistOf("advice-2"))
		})

		It("tracks scheduler metadata changes as updates", func() {
			initial := []heimdallclient.APIAdvice{
				{
					HeimdallID: "advice-1",
					JobID:      "job-1",
					SchedulerMetadata: map[string]interface{}{
						"namespace":       "default",
						"job_name":        "job-1-name",
						"job_kind":        "PyTorchJob",
						"job_api_version": "kubeflow.org/v1",
					},
				},
			}
			adviceCache.Update(initial)

			updatedAdvice := []heimdallclient.APIAdvice{
				{
					HeimdallID: "advice-1",
					JobID:      "job-1",
					SchedulerMetadata: map[string]interface{}{
						"namespace":       "default",
						"job_name":        "job-1-name-updated",
						"job_kind":        "PyTorchJob",
						"job_api_version": "kubeflow.org/v2",
					},
				},
			}

			added, updated, removed := adviceCache.Update(updatedAdvice)
			Expect(added).To(BeEmpty())
			Expect(updated).To(ConsistOf("advice-1"))
			Expect(removed).To(BeEmpty())
		})

		It("tracks issue payload changes as updates", func() {
			initial := []heimdallclient.APIAdvice{
				{
					HeimdallID: "advice-1",
					JobID:      "job-1",
					Issues: []heimdallclient.APIIssue{{
						IssueType:        "Hang",
						Timestamp:        "2026-01-01T00:00:00Z",
						DetectedBy:       "Throughput Monitor",
						RelevantLogLines: []string{"rank stalled"},
					}},
				},
			}
			adviceCache.Update(initial)

			updatedAdvice := []heimdallclient.APIAdvice{
				{
					HeimdallID: "advice-1",
					JobID:      "job-1",
					Issues: []heimdallclient.APIIssue{{
						IssueType:        "Hang",
						Timestamp:        "2026-01-01T00:01:00Z",
						DetectedBy:       "Scheduler State",
						RelevantLogLines: []string{"rank stalled", "heartbeat missing"},
						RemediationActions: []heimdallclient.APIRemediationAction{{
							ActionType: "Restart",
							Timestamp:  "2026-01-01T00:02:00Z",
							Reason:     "Recovered from hang",
						}},
					}},
				},
			}

			added, updated, removed := adviceCache.Update(updatedAdvice)
			Expect(added).To(BeEmpty())
			Expect(updated).To(ConsistOf("advice-1"))
			Expect(removed).To(BeEmpty())
		})

		It("tracks namespace moves as an update plus old-namespace removal", func() {
			initial := []heimdallclient.APIAdvice{
				{
					HeimdallID: "advice-1",
					JobID:      "job-1",
					SchedulerMetadata: map[string]interface{}{
						"namespace": "runai-team-a",
					},
				},
			}
			adviceCache.Update(initial)

			updatedAdvice := []heimdallclient.APIAdvice{
				{
					HeimdallID: "advice-1",
					JobID:      "job-1",
					SchedulerMetadata: map[string]interface{}{
						"namespace": "runai-team-b",
					},
				},
			}

			added, updated, removed := adviceCache.Update(updatedAdvice)
			Expect(added).To(BeEmpty())
			Expect(updated).To(ConsistOf("advice-1"))
			Expect(removed).To(ConsistOf(cache.RemovedAdvice{ID: "advice-1", Namespace: "runai-team-a"}))
		})

		It("provides lookup by ID", func() {
			apiAdvice := []heimdallclient.APIAdvice{
				{HeimdallID: "advice-1", JobID: "job-1"},
			}
			adviceCache.Update(apiAdvice)

			result := adviceCache.Get("advice-1")
			Expect(result).NotTo(BeNil())
			Expect(result.JobID).To(Equal("job-1"))

			result = adviceCache.Get("non-existent")
			Expect(result).To(BeNil())
		})
	})
})
