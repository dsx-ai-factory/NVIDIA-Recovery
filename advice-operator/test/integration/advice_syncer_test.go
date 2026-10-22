//go:build integration
// +build integration

// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package integration

import (
	"context"
	"errors"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/mock/gomock"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/event"

	heimdallv1alpha1 "github.com/dsx-ai-factory/nvidia-recovery/advice-operator/api/v1alpha1"
	"github.com/dsx-ai-factory/nvidia-recovery/advice-operator/internal/cache"
	"github.com/dsx-ai-factory/nvidia-recovery/advice-operator/internal/client"
	"github.com/dsx-ai-factory/nvidia-recovery/advice-operator/internal/controller"
	"github.com/dsx-ai-factory/nvidia-recovery/advice-operator/internal/syncer"
)

var _ = Describe("AdviceSyncer", Label("syncer"), func() {
	Describe("full sync lifecycle", Ordered, func() {
		const (
			testNamespace = "default"
			ownerName     = "workload-123"
			advice1ID     = "advice-001"
			advice2ID     = "advice-002"
			advice3ID     = "advice-003"
			advice4ID     = "advice-004"
			jobID1        = "job-123"
			jobID2        = "job-456"
			jobID3        = "job-789"
			jobID4        = "job-abc"
		)

		var (
			mockCtrl              *gomock.Controller
			mockClient            *client.MockAdviceSource
			adviceSyncer          *syncer.AdviceSyncer
			adviceCache           *cache.AdviceCache
			reconciler            *controller.AdviceReconciler
			ownerUID              types.UID
			syncCtx               context.Context
			syncCancel            context.CancelFunc
			currentMockData       []client.APIAdvice
			mockShouldError       bool
			mockDataMutex         sync.RWMutex
			initialAdvice         []client.APIAdvice
			updatedNodeExclusions []client.APIAdvice
			updatedNodeInclusions []client.APIAdvice
			updatedState          []client.APIAdvice
			updatedSimultaneous   []client.APIAdvice
			expandedAdvice        []client.APIAdvice
		)

		BeforeAll(func() {
			syncCtx, syncCancel = context.WithCancel(ctx)
			mockCtrl = gomock.NewController(GinkgoT())
			mockClient = client.NewMockAdviceSource(mockCtrl)

			owner := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      ownerName,
					Namespace: testNamespace,
				},
			}
			Expect(k8sClient.Create(ctx, owner)).To(Succeed())
			ownerUID = owner.UID

			// Create shared cache
			adviceCache = cache.NewAdviceCache()

			// Create a buffered channel to prevent blocking
			eventChan := make(chan event.GenericEvent, 100)

			// Set up the syncer
			adviceSyncer = &syncer.AdviceSyncer{
				K8sClient:      k8sClient,
				HeimdallClient: mockClient,
				PollInterval:   200 * time.Millisecond, // Fast polling for tests
				Namespaces:     []string{},             // All namespaces
				Cache:          adviceCache,
				Events:         eventChan,
			}

			// Set up the reconciler
			reconciler = &controller.AdviceReconciler{
				Client:    k8sClient,
				Scheme:    runtime.NewScheme(),
				APIReader: k8sClient,
				Cache:     adviceCache,
			}
			// Add Advice type to scheme
			_ = heimdallv1alpha1.AddToScheme(reconciler.Scheme)
			_ = scheme.AddToScheme(reconciler.Scheme)

			// Start a goroutine to process events from the channel
			go func() {
				for {
					select {
					case <-syncCtx.Done():
						return
					case evt := <-eventChan:
						// Process the event by calling Reconcile
						nsName := types.NamespacedName{
							Name:      evt.Object.GetName(),
							Namespace: evt.Object.GetNamespace(),
						}
						_, _ = reconciler.Reconcile(syncCtx, ctrl.Request{NamespacedName: nsName})
					}
				}
			}()

			// Initial advice setup: 4 advice objects to test different update scenarios
			state1 := "ADVICE_STATE_RESTARTED"
			state2 := "ADVICE_STATE_TERMINATED"
			state3 := "ADVICE_STATE_RESTARTED"
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
				{
					HeimdallID:   advice4ID,
					JobID:        jobID4,
					AttemptIndex: 0,
					SchedulerMetadata: map[string]interface{}{
						"namespace": testNamespace,
					},
					Advice: client.APIAdviceRecommendation{
						State: &state1,
						NodeConstraints: &client.APINodeConstraints{
							Exclusions: []string{"node-5"},
						},
					},
				},
			}

			// Update 1: Node exclusions changed for advice1
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
				{
					HeimdallID:   advice4ID,
					JobID:        jobID4,
					AttemptIndex: 0,
					SchedulerMetadata: map[string]interface{}{
						"namespace": testNamespace,
					},
					Advice: client.APIAdviceRecommendation{
						State: &state1,
						NodeConstraints: &client.APINodeConstraints{
							Exclusions: []string{"node-5"},
						},
					},
				},
			}

			// Update 2: Node inclusions changed for advice2
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
				{
					HeimdallID:   advice4ID,
					JobID:        jobID4,
					AttemptIndex: 0,
					SchedulerMetadata: map[string]interface{}{
						"namespace": testNamespace,
					},
					Advice: client.APIAdviceRecommendation{
						State: &state1,
						NodeConstraints: &client.APINodeConstraints{
							Exclusions: []string{"node-5"},
						},
					},
				},
			}

			// Update 3: State changed for advice3
			stateChanged := "ADVICE_STATE_TERMINATED"
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
						State: &stateChanged, // Changed from Restarted to Terminated
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
						State: &state1,
						NodeConstraints: &client.APINodeConstraints{
							Exclusions: []string{"node-5"},
						},
					},
				},
			}

			// Update 4: Attempt index + node exclusions for advice4 (simultaneous)
			updatedSimultaneous = []client.APIAdvice{
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
						State: &stateChanged,
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
						State: &state1,
						NodeConstraints: &client.APINodeConstraints{
							Exclusions: []string{"node-5", "node-6", "node-7"}, // Changed
						},
					},
				},
			}

			// Update 5: Add new advice
			newAdviceState := "ADVICE_STATE_RESTARTED"
			expandedAdvice = append(updatedSimultaneous, client.APIAdvice{
				HeimdallID:   "advice-new",
				JobID:        "job-new",
				AttemptIndex: 0,
				SchedulerMetadata: map[string]interface{}{
					"namespace": testNamespace,
				},
				Advice: client.APIAdviceRecommendation{
					State: &newAdviceState,
					NodeConstraints: &client.APINodeConstraints{
						Inclusions: []string{"special-node"},
					},
				},
			})

			// Initialize with the initial advice data
			currentMockData = initialAdvice
			mockShouldError = false

			// Set up mock to always return the current mutable data
			mockClient.EXPECT().
				ListAdvice(gomock.Any()).
				DoAndReturn(func(ctx context.Context) ([]client.APIAdvice, error) {
					mockDataMutex.RLock()
					defer mockDataMutex.RUnlock()

					if mockShouldError {
						return nil, errors.New("mock API error")
					}

					// Return a copy to avoid race conditions
					result := make([]client.APIAdvice, len(currentMockData))
					copy(result, currentMockData)
					return result, nil
				}).
				AnyTimes()

			// Start the syncer in background
			go func() {
				_ = adviceSyncer.Start(syncCtx)
			}()
		})

		AfterAll(func() {
			mockCtrl.Finish()
			syncCancel()

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

		It("should fetch advice from API and create CRDs with correct spec fields", func() {
			// Wait for all 4 advice CRDs to be created with correct initial state
			// Check them all together in one Eventually to avoid race condition with fast syncer polling
			Eventually(func(g Gomega) {
				advice1 := &heimdallv1alpha1.Advice{}
				g.Expect(k8sClient.Get(syncCtx, types.NamespacedName{
					Name:      advice1ID,
					Namespace: testNamespace,
				}, advice1)).To(Succeed())
				g.Expect(advice1.OwnerReferences).To(Equal([]metav1.OwnerReference{{
					APIVersion: "v1",
					Kind:       "ConfigMap",
					Name:       ownerName,
					UID:        ownerUID,
				}}))
				g.Expect(advice1.Spec.Job.JobID).To(Equal(jobID1))
				g.Expect(advice1.Spec.Job.AttemptIndex).To(Equal(int32(0)))
				g.Expect(advice1.Spec.Advice.State).NotTo(BeNil())
				g.Expect(*advice1.Spec.Advice.State).To(Equal(heimdallv1alpha1.JobState("ADVICE_STATE_RESTARTED")))
				g.Expect(advice1.Spec.Advice.NodeConstraints).NotTo(BeNil())
				g.Expect(advice1.Spec.Advice.NodeConstraints.Exclusions).To(Equal([]string{"node-1"}))

				advice2 := &heimdallv1alpha1.Advice{}
				g.Expect(k8sClient.Get(syncCtx, types.NamespacedName{
					Name:      advice2ID,
					Namespace: testNamespace,
				}, advice2)).To(Succeed())
				g.Expect(advice2.Spec.Advice.NodeConstraints).NotTo(BeNil())
				g.Expect(advice2.Spec.Advice.NodeConstraints.Inclusions).To(Equal([]string{"node-3"}))

				advice3 := &heimdallv1alpha1.Advice{}
				g.Expect(k8sClient.Get(syncCtx, types.NamespacedName{
					Name:      advice3ID,
					Namespace: testNamespace,
				}, advice3)).To(Succeed())

				advice4 := &heimdallv1alpha1.Advice{}
				g.Expect(k8sClient.Get(syncCtx, types.NamespacedName{
					Name:      advice4ID,
					Namespace: testNamespace,
				}, advice4)).To(Succeed())
			}).WithTimeout(3 * time.Second).WithPolling(50 * time.Millisecond).Should(Succeed())
		})

		Context("When API data changes", func() {
			It("should update node exclusions when they change", func() {
				// Update mock data to return updatedNodeExclusions
				mockDataMutex.Lock()
				currentMockData = updatedNodeExclusions
				mockDataMutex.Unlock()

				// Syncer will fetch updated advice on next poll, verify advice1 exclusions updated
				Eventually(func(g Gomega) {
					advice1 := &heimdallv1alpha1.Advice{}
					g.Expect(k8sClient.Get(syncCtx, types.NamespacedName{
						Name:      advice1ID,
						Namespace: testNamespace,
					}, advice1)).To(Succeed())

					g.Expect(advice1.Spec.Advice.NodeConstraints).NotTo(BeNil())
					g.Expect(advice1.Spec.Advice.NodeConstraints.Exclusions).To(Equal([]string{"node-1", "node-2"}))
				}).WithTimeout(3 * time.Second).WithPolling(50 * time.Millisecond).Should(Succeed())
			})

			It("should update node inclusions when they change", func() {
				// Update mock data to return updatedNodeInclusions
				mockDataMutex.Lock()
				currentMockData = updatedNodeInclusions
				mockDataMutex.Unlock()

				// Syncer will fetch updated advice on next poll, verify advice2 inclusions updated
				Eventually(func(g Gomega) {
					advice2 := &heimdallv1alpha1.Advice{}
					g.Expect(k8sClient.Get(syncCtx, types.NamespacedName{
						Name:      advice2ID,
						Namespace: testNamespace,
					}, advice2)).To(Succeed())

					g.Expect(advice2.Spec.Advice.NodeConstraints).NotTo(BeNil())
					g.Expect(advice2.Spec.Advice.NodeConstraints.Inclusions).To(Equal([]string{"node-3", "node-4"}))
				}).WithTimeout(3 * time.Second).WithPolling(50 * time.Millisecond).Should(Succeed())
			})

			It("should update state when it changes", func() {
				// Update mock data to return updatedState
				mockDataMutex.Lock()
				currentMockData = updatedState
				mockDataMutex.Unlock()

				// Syncer will fetch updated advice on next poll, verify advice3 state changed
				Eventually(func(g Gomega) {
					advice3 := &heimdallv1alpha1.Advice{}
					g.Expect(k8sClient.Get(syncCtx, types.NamespacedName{
						Name:      advice3ID,
						Namespace: testNamespace,
					}, advice3)).To(Succeed())

					g.Expect(advice3.Spec.Advice.State).NotTo(BeNil())
					g.Expect(*advice3.Spec.Advice.State).To(Equal(heimdallv1alpha1.JobState("ADVICE_STATE_TERMINATED")))
				}).WithTimeout(3 * time.Second).WithPolling(50 * time.Millisecond).Should(Succeed())
			})

			It("should update attempt index + node exclusions simultaneously", func() {
				// Update mock data to return updatedSimultaneous
				mockDataMutex.Lock()
				currentMockData = updatedSimultaneous
				mockDataMutex.Unlock()

				// Syncer will fetch updated advice on next poll, verify advice4 has both changes
				Eventually(func(g Gomega) {
					advice4 := &heimdallv1alpha1.Advice{}
					g.Expect(k8sClient.Get(syncCtx, types.NamespacedName{
						Name:      advice4ID,
						Namespace: testNamespace,
					}, advice4)).To(Succeed())

					g.Expect(advice4.Spec.Job.AttemptIndex).To(Equal(int32(1)))
					g.Expect(advice4.Spec.Advice.NodeConstraints).NotTo(BeNil())
					g.Expect(advice4.Spec.Advice.NodeConstraints.Exclusions).To(Equal([]string{"node-5", "node-6", "node-7"}))
				}).WithTimeout(3 * time.Second).WithPolling(50 * time.Millisecond).Should(Succeed())
			})
		})

		It("should create new CRDs for new advice", func() {
			// Update mock data to return expandedAdvice (includes advice-new)
			mockDataMutex.Lock()
			currentMockData = expandedAdvice
			mockDataMutex.Unlock()

			// Syncer will fetch expanded advice on next poll, verify new advice was created
			Eventually(func(g Gomega) {
				adviceNew := &heimdallv1alpha1.Advice{}
				g.Expect(k8sClient.Get(syncCtx, types.NamespacedName{
					Name:      "advice-new",
					Namespace: testNamespace,
				}, adviceNew)).To(Succeed())

				g.Expect(adviceNew.Spec.Job.JobID).To(Equal("job-new"))
				g.Expect(adviceNew.Spec.Advice.NodeConstraints).NotTo(BeNil())
				g.Expect(adviceNew.Spec.Advice.NodeConstraints.Inclusions).To(Equal([]string{"special-node"}))
			}).WithTimeout(3 * time.Second).WithPolling(50 * time.Millisecond).Should(Succeed())
		})

		It("should delete CRDs not present in API response", func() {
			// Update mock data to return only advice-001
			mockDataMutex.Lock()
			reducedAdvice := []client.APIAdvice{initialAdvice[0]}
			currentMockData = reducedAdvice
			mockDataMutex.Unlock()

			// Syncer will fetch reduced advice list on next poll, verify deletions
			advice2 := &heimdallv1alpha1.Advice{}
			Eventually(func() bool {
				err := k8sClient.Get(syncCtx, types.NamespacedName{
					Name:      advice2ID,
					Namespace: testNamespace,
				}, advice2)
				return err != nil
			}).WithTimeout(3 * time.Second).WithPolling(100 * time.Millisecond).Should(BeTrue())

			advice3 := &heimdallv1alpha1.Advice{}
			Eventually(func() bool {
				err := k8sClient.Get(syncCtx, types.NamespacedName{
					Name:      advice3ID,
					Namespace: testNamespace,
				}, advice3)
				return err != nil
			}).WithTimeout(3 * time.Second).WithPolling(100 * time.Millisecond).Should(BeTrue())

			advice4 := &heimdallv1alpha1.Advice{}
			Eventually(func() bool {
				err := k8sClient.Get(syncCtx, types.NamespacedName{
					Name:      advice4ID,
					Namespace: testNamespace,
				}, advice4)
				return err != nil
			}).WithTimeout(3 * time.Second).WithPolling(100 * time.Millisecond).Should(BeTrue())

			adviceNew := &heimdallv1alpha1.Advice{}
			Eventually(func() bool {
				err := k8sClient.Get(syncCtx, types.NamespacedName{
					Name:      "advice-new",
					Namespace: testNamespace,
				}, adviceNew)
				return err != nil
			}).WithTimeout(3 * time.Second).WithPolling(100 * time.Millisecond).Should(BeTrue())

			// Verify advice1 still exists
			advice1 := &heimdallv1alpha1.Advice{}
			Expect(k8sClient.Get(syncCtx, types.NamespacedName{
				Name:      advice1ID,
				Namespace: testNamespace,
			}, advice1)).To(Succeed())
		})

		It("should handle API errors gracefully", func() {
			// Update mock to return errors
			mockDataMutex.Lock()
			mockShouldError = true
			mockDataMutex.Unlock()

			// Syncer will encounter error on next poll
			// Wait a bit to ensure syncer has attempted to sync
			time.Sleep(500 * time.Millisecond)

			// Verify advice1 still exists (syncer didn't crash, no changes made)
			advice1 := &heimdallv1alpha1.Advice{}
			Expect(k8sClient.Get(syncCtx, types.NamespacedName{
				Name:      advice1ID,
				Namespace: testNamespace,
			}, advice1)).To(Succeed())
		})
	})
})
