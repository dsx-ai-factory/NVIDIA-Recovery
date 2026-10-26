//go:build integration
// +build integration

// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package integration

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	heimdallv1alpha1 "github.com/dsx-ai-factory/nvidia-recovery/advice-operator/api/v1alpha1"
	"github.com/dsx-ai-factory/nvidia-recovery/advice-operator/internal/cache"
	"github.com/dsx-ai-factory/nvidia-recovery/advice-operator/internal/client"
	"github.com/dsx-ai-factory/nvidia-recovery/advice-operator/internal/controller"
)

var _ = Describe("AdviceController", Label("controller"), func() {
	Describe("controller reconciliation logic", Ordered, func() {
		const (
			testNamespace = "default"
			ownerName     = "workload-123"
			advice1ID     = "advice-ctrl-001"
			advice2ID     = "advice-ctrl-002"
			advice3ID     = "advice-ctrl-003"
			advice4ID     = "advice-ctrl-004"
			jobID1        = "job-ctrl-123"
			jobID2        = "job-ctrl-456"
			jobID3        = "job-ctrl-789"
			jobID4        = "job-ctrl-abc"
		)

		var (
			reconciler            *controller.AdviceReconciler
			adviceCache           *cache.AdviceCache
			ownerUID              types.UID
			initialAdvice         []client.APIAdvice
			updatedNodeExclusions []client.APIAdvice
			updatedNodeInclusions []client.APIAdvice
			withAdvice3           []client.APIAdvice
			updatedState          []client.APIAdvice
			withAdvice4           []client.APIAdvice
			updatedMultipleFields []client.APIAdvice
			reducedAdvice         []client.APIAdvice
		)

		BeforeAll(func() {
			owner := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      ownerName,
					Namespace: testNamespace,
				},
			}
			Expect(k8sClient.Create(ctx, owner)).To(Succeed())
			ownerUID = owner.UID

			adviceCache = cache.NewAdviceCache()
			reconciler = &controller.AdviceReconciler{
				Client:    k8sClient,
				Scheme:    k8sClient.Scheme(),
				APIReader: k8sClient,
				Cache:     adviceCache,
			}

			// Define all test advice datasets upfront
			state1 := "ADVICE_STATE_RESTARTED"
			state2 := "ADVICE_STATE_TERMINATED"
			state3 := "ADVICE_STATE_RESTARTED"
			state3Updated := "ADVICE_STATE_TERMINATED"
			state4 := "ADVICE_STATE_RESTARTED"

			// Initial: advice1 (exclude node-1), advice2 (include node-3)
			initialAdvice = []client.APIAdvice{
				{
					HeimdallID:   advice1ID,
					JobID:        jobID1,
					AttemptIndex: 0,
					SchedulerMetadata: map[string]interface{}{
						"namespace":         testNamespace,
						"owner_api_version": "v1",
						"owner_kind":        "ConfigMap",
						"owner_name":        ownerName,
						"owner_uid":         string(ownerUID),
					},
					Advice: client.APIAdviceRecommendation{
						State: &state1,
						NodeConstraints: &client.APINodeConstraints{
							Exclusions: []string{"node-1"},
						},
					},
				},
				{
					HeimdallID:   advice2ID,
					JobID:        jobID2,
					AttemptIndex: 0,
					SchedulerMetadata: map[string]interface{}{
						"namespace": testNamespace,
					},
					Advice: client.APIAdviceRecommendation{
						State: &state2,
						NodeConstraints: &client.APINodeConstraints{
							Inclusions: []string{"node-3"},
						},
					},
				},
			}

			// Updated node exclusions: advice1 now excludes node-1 AND node-2
			updatedNodeExclusions = []client.APIAdvice{
				{
					HeimdallID:   advice1ID,
					JobID:        jobID1,
					AttemptIndex: 0,
					SchedulerMetadata: map[string]interface{}{
						"namespace": testNamespace,
					},
					Advice: client.APIAdviceRecommendation{
						State: &state1,
						NodeConstraints: &client.APINodeConstraints{
							Exclusions: []string{"node-1", "node-2"}, // Added node-2
						},
					},
				},
				{
					HeimdallID:   advice2ID,
					JobID:        jobID2,
					AttemptIndex: 0,
					SchedulerMetadata: map[string]interface{}{
						"namespace": testNamespace,
					},
					Advice: client.APIAdviceRecommendation{
						State: &state2,
						NodeConstraints: &client.APINodeConstraints{
							Inclusions: []string{"node-3"},
						},
					},
				},
			}

			// Updated node inclusions: advice2 now includes node-3 AND node-4
			updatedNodeInclusions = []client.APIAdvice{
				{
					HeimdallID:   advice1ID,
					JobID:        jobID1,
					AttemptIndex: 0,
					SchedulerMetadata: map[string]interface{}{
						"namespace": testNamespace,
					},
					Advice: client.APIAdviceRecommendation{
						State: &state1,
						NodeConstraints: &client.APINodeConstraints{
							Exclusions: []string{"node-1", "node-2"},
						},
					},
				},
				{
					HeimdallID:   advice2ID,
					JobID:        jobID2,
					AttemptIndex: 0,
					SchedulerMetadata: map[string]interface{}{
						"namespace": testNamespace,
					},
					Advice: client.APIAdviceRecommendation{
						State: &state2,
						NodeConstraints: &client.APINodeConstraints{
							Inclusions: []string{"node-3", "node-4"}, // Added node-4
						},
					},
				},
			}

			// Add advice3 with initial state
			withAdvice3 = []client.APIAdvice{
				{
					HeimdallID:   advice1ID,
					JobID:        jobID1,
					AttemptIndex: 0,
					SchedulerMetadata: map[string]interface{}{
						"namespace": testNamespace,
					},
					Advice: client.APIAdviceRecommendation{
						State: &state1,
						NodeConstraints: &client.APINodeConstraints{
							Exclusions: []string{"node-1", "node-2"},
						},
					},
				},
				{
					HeimdallID:   advice2ID,
					JobID:        jobID2,
					AttemptIndex: 0,
					SchedulerMetadata: map[string]interface{}{
						"namespace": testNamespace,
					},
					Advice: client.APIAdviceRecommendation{
						State: &state2,
						NodeConstraints: &client.APINodeConstraints{
							Inclusions: []string{"node-3", "node-4"},
						},
					},
				},
				{
					HeimdallID:   advice3ID,
					JobID:        jobID3,
					AttemptIndex: 0,
					SchedulerMetadata: map[string]interface{}{
						"namespace": testNamespace,
					},
					Advice: client.APIAdviceRecommendation{
						State: &state3,
					},
				},
			}

			// Updated state: advice3 state changed from Restarted to Terminated
			updatedState = []client.APIAdvice{
				{
					HeimdallID:   advice1ID,
					JobID:        jobID1,
					AttemptIndex: 0,
					SchedulerMetadata: map[string]interface{}{
						"namespace": testNamespace,
					},
					Advice: client.APIAdviceRecommendation{
						State: &state1,
						NodeConstraints: &client.APINodeConstraints{
							Exclusions: []string{"node-1", "node-2"},
						},
					},
				},
				{
					HeimdallID:   advice2ID,
					JobID:        jobID2,
					AttemptIndex: 0,
					SchedulerMetadata: map[string]interface{}{
						"namespace": testNamespace,
					},
					Advice: client.APIAdviceRecommendation{
						State: &state2,
						NodeConstraints: &client.APINodeConstraints{
							Inclusions: []string{"node-3", "node-4"},
						},
					},
				},
				{
					HeimdallID:   advice3ID,
					JobID:        jobID3,
					AttemptIndex: 0,
					SchedulerMetadata: map[string]interface{}{
						"namespace": testNamespace,
					},
					Advice: client.APIAdviceRecommendation{
						State: &state3Updated, // Changed from Restarted to Terminated
					},
				},
			}

			// Add advice4 with initial state
			withAdvice4 = []client.APIAdvice{
				{
					HeimdallID:   advice1ID,
					JobID:        jobID1,
					AttemptIndex: 0,
					SchedulerMetadata: map[string]interface{}{
						"namespace": testNamespace,
					},
					Advice: client.APIAdviceRecommendation{
						State: &state1,
						NodeConstraints: &client.APINodeConstraints{
							Exclusions: []string{"node-1", "node-2"},
						},
					},
				},
				{
					HeimdallID:   advice2ID,
					JobID:        jobID2,
					AttemptIndex: 0,
					SchedulerMetadata: map[string]interface{}{
						"namespace": testNamespace,
					},
					Advice: client.APIAdviceRecommendation{
						State: &state2,
						NodeConstraints: &client.APINodeConstraints{
							Inclusions: []string{"node-3", "node-4"},
						},
					},
				},
				{
					HeimdallID:   advice3ID,
					JobID:        jobID3,
					AttemptIndex: 0,
					SchedulerMetadata: map[string]interface{}{
						"namespace": testNamespace,
					},
					Advice: client.APIAdviceRecommendation{
						State: &state3Updated,
					},
				},
				{
					HeimdallID:   advice4ID,
					JobID:        jobID4,
					AttemptIndex: 0,
					SchedulerMetadata: map[string]interface{}{
						"namespace": testNamespace,
					},
					Advice: client.APIAdviceRecommendation{
						State: &state4,
						NodeConstraints: &client.APINodeConstraints{
							Exclusions: []string{"node-5"},
						},
					},
				},
			}

			// Updated multiple fields: advice4 attempt index + exclusions changed
			updatedMultipleFields = []client.APIAdvice{
				{
					HeimdallID:   advice1ID,
					JobID:        jobID1,
					AttemptIndex: 0,
					SchedulerMetadata: map[string]interface{}{
						"namespace": testNamespace,
					},
					Advice: client.APIAdviceRecommendation{
						State: &state1,
						NodeConstraints: &client.APINodeConstraints{
							Exclusions: []string{"node-1", "node-2"},
						},
					},
				},
				{
					HeimdallID:   advice2ID,
					JobID:        jobID2,
					AttemptIndex: 0,
					SchedulerMetadata: map[string]interface{}{
						"namespace": testNamespace,
					},
					Advice: client.APIAdviceRecommendation{
						State: &state2,
						NodeConstraints: &client.APINodeConstraints{
							Inclusions: []string{"node-3", "node-4"},
						},
					},
				},
				{
					HeimdallID:   advice3ID,
					JobID:        jobID3,
					AttemptIndex: 0,
					SchedulerMetadata: map[string]interface{}{
						"namespace": testNamespace,
					},
					Advice: client.APIAdviceRecommendation{
						State: &state3Updated,
					},
				},
				{
					HeimdallID:   advice4ID,
					JobID:        jobID4,
					AttemptIndex: 1, // Changed
					SchedulerMetadata: map[string]interface{}{
						"namespace": testNamespace,
					},
					Advice: client.APIAdviceRecommendation{
						State: &state4,
						NodeConstraints: &client.APINodeConstraints{
							Exclusions: []string{"node-5", "node-6", "node-7"}, // Changed
						},
					},
				},
			}

			// Reduced: only advice1 remains
			reducedAdvice = []client.APIAdvice{
				{
					HeimdallID:   advice1ID,
					JobID:        jobID1,
					AttemptIndex: 0,
					SchedulerMetadata: map[string]interface{}{
						"namespace": testNamespace,
					},
					Advice: client.APIAdviceRecommendation{
						State: &state1,
						NodeConstraints: &client.APINodeConstraints{
							Exclusions: []string{"node-1", "node-2"},
						},
					},
				},
			}
		})

		AfterAll(func() {
			// Clean up any Advice CRDs
			adviceList := &heimdallv1alpha1.AdviceList{}
			_ = k8sClient.List(ctx, adviceList)
			for _, advice := range adviceList.Items {
				_ = k8sClient.Delete(ctx, &advice)
			}

			owner := &corev1.ConfigMap{}
			ownerKey := types.NamespacedName{Name: ownerName, Namespace: testNamespace}
			if err := k8sClient.Get(ctx, ownerKey, owner); err == nil {
				_ = k8sClient.Delete(ctx, owner)
			}
		})

		Context("When cache has advice data", func() {
			It("should create CRD when cache has advice but no CRD exists", func() {
				// Populate cache with initial advice data
				adviceCache.Update(initialAdvice)

				// Reconcile advice1
				result, err := reconciler.Reconcile(ctx, ctrl.Request{
					NamespacedName: types.NamespacedName{
						Name:      advice1ID,
						Namespace: testNamespace,
					},
				})
				Expect(err).NotTo(HaveOccurred())
				Expect(result.Requeue).To(BeFalse())

				// Verify CRD was created with correct data
				advice1 := &heimdallv1alpha1.Advice{}
				Expect(k8sClient.Get(ctx, types.NamespacedName{
					Name:      advice1ID,
					Namespace: testNamespace,
				}, advice1)).To(Succeed())

				Expect(advice1.OwnerReferences).To(Equal([]metav1.OwnerReference{{
					APIVersion: "v1",
					Kind:       "ConfigMap",
					Name:       ownerName,
					UID:        ownerUID,
				}}))
				Expect(advice1.Spec.Job.JobID).To(Equal(jobID1))
				Expect(advice1.Spec.Job.AttemptIndex).To(Equal(int32(0)))
				Expect(advice1.Spec.Advice.State).NotTo(BeNil())
				Expect(*advice1.Spec.Advice.State).To(Equal(heimdallv1alpha1.JobState("ADVICE_STATE_RESTARTED")))
				Expect(advice1.Spec.Advice.NodeConstraints).NotTo(BeNil())
				Expect(advice1.Spec.Advice.NodeConstraints.Exclusions).To(Equal([]string{"node-1"}))

				// Reconcile advice2
				result, err = reconciler.Reconcile(ctx, ctrl.Request{
					NamespacedName: types.NamespacedName{
						Name:      advice2ID,
						Namespace: testNamespace,
					},
				})
				Expect(err).NotTo(HaveOccurred())
				Expect(result.Requeue).To(BeFalse())

				// Verify advice2 was created
				advice2 := &heimdallv1alpha1.Advice{}
				Expect(k8sClient.Get(ctx, types.NamespacedName{
					Name:      advice2ID,
					Namespace: testNamespace,
				}, advice2)).To(Succeed())
				Expect(advice2.Spec.Advice.NodeConstraints.Inclusions).To(Equal([]string{"node-3"}))
			})

			It("should not modify CRD when cache and CRD already match", func() {
				// Get current resource version
				advice1 := &heimdallv1alpha1.Advice{}
				Expect(k8sClient.Get(ctx, types.NamespacedName{
					Name:      advice1ID,
					Namespace: testNamespace,
				}, advice1)).To(Succeed())
				initialResourceVersion := advice1.ResourceVersion

				// Reconcile again without changing cache
				result, err := reconciler.Reconcile(ctx, ctrl.Request{
					NamespacedName: types.NamespacedName{
						Name:      advice1ID,
						Namespace: testNamespace,
					},
				})
				Expect(err).NotTo(HaveOccurred())
				Expect(result.Requeue).To(BeFalse())

				// Verify CRD was not modified (resource version unchanged)
				Expect(k8sClient.Get(ctx, types.NamespacedName{
					Name:      advice1ID,
					Namespace: testNamespace,
				}, advice1)).To(Succeed())
				Expect(advice1.ResourceVersion).To(Equal(initialResourceVersion))
			})

			Context("When updating CRD fields", func() {
				It("should update node exclusions", func() {
					// Update cache with new exclusions for advice1
					adviceCache.Update(updatedNodeExclusions)

					// Reconcile advice1
					result, err := reconciler.Reconcile(ctx, ctrl.Request{
						NamespacedName: types.NamespacedName{
							Name:      advice1ID,
							Namespace: testNamespace,
						},
					})
					Expect(err).NotTo(HaveOccurred())
					Expect(result.Requeue).To(BeFalse())

					// Verify exclusions were updated
					advice1 := &heimdallv1alpha1.Advice{}
					Expect(k8sClient.Get(ctx, types.NamespacedName{
						Name:      advice1ID,
						Namespace: testNamespace,
					}, advice1)).To(Succeed())
					Expect(advice1.Spec.Advice.NodeConstraints.Exclusions).To(Equal([]string{"node-1", "node-2"}))
				})

				It("should update node inclusions", func() {
					// Update cache with new inclusions for advice2
					adviceCache.Update(updatedNodeInclusions)

					// Reconcile advice2
					result, err := reconciler.Reconcile(ctx, ctrl.Request{
						NamespacedName: types.NamespacedName{
							Name:      advice2ID,
							Namespace: testNamespace,
						},
					})
					Expect(err).NotTo(HaveOccurred())
					Expect(result.Requeue).To(BeFalse())

					// Verify inclusions were updated
					advice2 := &heimdallv1alpha1.Advice{}
					Expect(k8sClient.Get(ctx, types.NamespacedName{
						Name:      advice2ID,
						Namespace: testNamespace,
					}, advice2)).To(Succeed())
					Expect(advice2.Spec.Advice.NodeConstraints.Inclusions).To(Equal([]string{"node-3", "node-4"}))
				})

				It("should update state", func() {
					// Create advice3 with initial state
					adviceCache.Update(withAdvice3)

					// Reconcile advice3 to create it
					result, err := reconciler.Reconcile(ctx, ctrl.Request{
						NamespacedName: types.NamespacedName{
							Name:      advice3ID,
							Namespace: testNamespace,
						},
					})
					Expect(err).NotTo(HaveOccurred())
					Expect(result.Requeue).To(BeFalse())

					// Now update state in cache
					adviceCache.Update(updatedState)

					// Reconcile advice3
					result, err = reconciler.Reconcile(ctx, ctrl.Request{
						NamespacedName: types.NamespacedName{
							Name:      advice3ID,
							Namespace: testNamespace,
						},
					})
					Expect(err).NotTo(HaveOccurred())
					Expect(result.Requeue).To(BeFalse())

					// Verify state was updated
					advice3 := &heimdallv1alpha1.Advice{}
					Expect(k8sClient.Get(ctx, types.NamespacedName{
						Name:      advice3ID,
						Namespace: testNamespace,
					}, advice3)).To(Succeed())
					Expect(*advice3.Spec.Advice.State).To(Equal(heimdallv1alpha1.JobState("ADVICE_STATE_TERMINATED")))
				})

				It("should update multiple fields simultaneously", func() {
					// Create advice4 with initial state
					adviceCache.Update(withAdvice4)

					// Reconcile advice4 to create it
					result, err := reconciler.Reconcile(ctx, ctrl.Request{
						NamespacedName: types.NamespacedName{
							Name:      advice4ID,
							Namespace: testNamespace,
						},
					})
					Expect(err).NotTo(HaveOccurred())
					Expect(result.Requeue).To(BeFalse())

					// Now update multiple fields in cache
					adviceCache.Update(updatedMultipleFields)

					// Reconcile advice4
					result, err = reconciler.Reconcile(ctx, ctrl.Request{
						NamespacedName: types.NamespacedName{
							Name:      advice4ID,
							Namespace: testNamespace,
						},
					})
					Expect(err).NotTo(HaveOccurred())
					Expect(result.Requeue).To(BeFalse())

					// Verify both fields were updated
					advice4 := &heimdallv1alpha1.Advice{}
					Expect(k8sClient.Get(ctx, types.NamespacedName{
						Name:      advice4ID,
						Namespace: testNamespace,
					}, advice4)).To(Succeed())
					Expect(advice4.Spec.Job.AttemptIndex).To(Equal(int32(1)))
					Expect(advice4.Spec.Advice.NodeConstraints.Exclusions).To(Equal([]string{"node-5", "node-6", "node-7"}))
				})
			})
		})

		Context("When cache does not have advice data", func() {
			It("should delete CRD if it exists in cluster", func() {
				// advice2, advice3, advice4 exist in cluster, now remove them from cache
				adviceCache.Update(reducedAdvice)

				// Reconcile advice2 (not in cache)
				result, err := reconciler.Reconcile(ctx, ctrl.Request{
					NamespacedName: types.NamespacedName{
						Name:      advice2ID,
						Namespace: testNamespace,
					},
				})
				Expect(err).NotTo(HaveOccurred())
				Expect(result.Requeue).To(BeFalse())

				// Verify advice2 was deleted
				advice2 := &heimdallv1alpha1.Advice{}
				err = k8sClient.Get(ctx, types.NamespacedName{
					Name:      advice2ID,
					Namespace: testNamespace,
				}, advice2)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("not found"))

				// Reconcile advice3 and advice4
				_, _ = reconciler.Reconcile(ctx, ctrl.Request{
					NamespacedName: types.NamespacedName{Name: advice3ID, Namespace: testNamespace},
				})
				_, _ = reconciler.Reconcile(ctx, ctrl.Request{
					NamespacedName: types.NamespacedName{Name: advice4ID, Namespace: testNamespace},
				})

				// Verify advice1 still exists (in cache)
				advice1 := &heimdallv1alpha1.Advice{}
				Expect(k8sClient.Get(ctx, types.NamespacedName{
					Name:      advice1ID,
					Namespace: testNamespace,
				}, advice1)).To(Succeed())
			})
		})
	})
})
