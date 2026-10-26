// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	advicev1alpha1 "github.com/dsx-ai-factory/nvidia-recovery/advice-operator/api/v1alpha1"
	heimdallv1 "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/go/heimdall/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

//go:generate go run go.uber.org/mock/mockgen@v0.6.0 -destination=mock_advice_source.go -package=client . AdviceSource

// AdviceSource defines the interface for fetching advice from any backend.
// Implementations may include HTTP clients, gRPC clients, or mock clients for testing.
type AdviceSource interface {
	// ListAdvice fetches all available advice from the backend.
	ListAdvice(ctx context.Context) ([]APIAdvice, error)
}

const defaultDetectedBy = "Unknown"

// APIAdvice represents the advice structure from the Heimdall API.
type APIAdvice struct {
	HeimdallID        string                  `json:"heimdallId"`
	JobID             string                  `json:"jobId"`
	SchedulerMetadata map[string]interface{}  `json:"schedulerMetadata"`
	AttemptIndex      int32                   `json:"attemptIndex"`
	Advice            APIAdviceRecommendation `json:"advice"`
	Issues            []APIIssue              `json:"issues,omitempty"`
	CreatedAt         string                  `json:"createdAt,omitempty"`
	UpdatedAt         string                  `json:"updatedAt,omitempty"`
}

// GetNamespace extracts the namespace from scheduler metadata.
func (a *APIAdvice) GetNamespace() string {
	if a.SchedulerMetadata == nil {
		return ""
	}
	if ns, ok := a.SchedulerMetadata["namespace"].(string); ok {
		return ns
	}
	return ""
}

// GetJobKind extracts the job kind from scheduler metadata.
func (a *APIAdvice) GetJobKind() string {
	if a.SchedulerMetadata == nil {
		return ""
	}
	if jobKind, ok := a.SchedulerMetadata["job_kind"].(string); ok {
		return jobKind
	}
	return ""
}

// GetJobAPIVersion extracts the job apiVersion from scheduler metadata.
func (a *APIAdvice) GetJobAPIVersion() string {
	if a.SchedulerMetadata == nil {
		return ""
	}
	if apiVersion, ok := a.SchedulerMetadata["job_api_version"].(string); ok {
		return apiVersion
	}
	return ""
}

// GetJobName extracts the job name from scheduler metadata.
func (a *APIAdvice) GetJobName() string {
	if a.SchedulerMetadata == nil {
		return ""
	}
	if name, ok := a.SchedulerMetadata["job_name"].(string); ok {
		return name
	}
	return ""
}

// GetOwnerReference extracts a single owner reference from scheduler metadata.
func (a *APIAdvice) GetOwnerReference() *metav1.OwnerReference {
	if a.SchedulerMetadata == nil {
		return nil
	}

	apiVersion, ok := a.SchedulerMetadata["owner_api_version"].(string)
	if !ok || apiVersion == "" {
		return nil
	}
	kind, ok := a.SchedulerMetadata["owner_kind"].(string)
	if !ok || kind == "" {
		return nil
	}
	name, ok := a.SchedulerMetadata["owner_name"].(string)
	if !ok || name == "" {
		return nil
	}
	uid, ok := a.SchedulerMetadata["owner_uid"].(string)
	if !ok || uid == "" {
		return nil
	}

	return &metav1.OwnerReference{
		APIVersion: apiVersion,
		Kind:       kind,
		Name:       name,
		UID:        types.UID(uid),
	}
}

// ToAdviceCRD converts an APIAdvice to an Advice CRD.
// Returns an error if required fields (HeimdallID, JobID) are missing.
func (a *APIAdvice) ToAdviceCRD() (*advicev1alpha1.Advice, error) {
	// Validate required fields - HeimdallID is used as Kubernetes resource name
	if a.HeimdallID == "" {
		return nil, errors.New("cannot convert APIAdvice to CRD: HeimdallID is required")
	}
	if a.JobID == "" {
		return nil, errors.New("cannot convert APIAdvice to CRD: JobID is required")
	}

	namespace := a.GetNamespace()
	if namespace == "" {
		namespace = "default"
	}

	// Build labels for correlation with scheduler metadata
	labels := map[string]string{
		"heimdall.nvidia.com/job-id":        a.JobID,
		"heimdall.nvidia.com/attempt-index": strconv.Itoa(int(a.AttemptIndex)),
	}
	// Build annotations for operator-owned metadata
	annotations := map[string]string{
		"heimdall.nvidia.com/heimdall-id": a.HeimdallID,
	}
	// Add job-kind if available from scheduler metadata
	if jobKind := a.GetJobKind(); jobKind != "" {
		annotations["heimdall.nvidia.com/job-kind"] = jobKind
	}

	advice := &advicev1alpha1.Advice{
		ObjectMeta: metav1.ObjectMeta{
			Name:        a.HeimdallID,
			Namespace:   namespace,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: advicev1alpha1.AdviceSpec{
			Job: advicev1alpha1.AdviceJobReference{
				HeimdallID:   a.HeimdallID,
				APIVersion:   a.GetJobAPIVersion(),
				Kind:         a.GetJobKind(),
				Name:         a.GetJobName(),
				JobID:        a.JobID,
				AttemptIndex: a.AttemptIndex,
			},
			Advice: advicev1alpha1.AdviceRecommendation{},
		},
	}
	if ownerRef := a.GetOwnerReference(); ownerRef != nil {
		advice.OwnerReferences = []metav1.OwnerReference{*ownerRef}
	}

	// Convert advice recommendation
	if a.Advice.State != nil && *a.Advice.State != heimdallv1.AdviceState_ADVICE_STATE_UNSPECIFIED.String() {
		state := advicev1alpha1.JobState(*a.Advice.State)
		advice.Spec.Advice.State = &state
	}

	if a.Advice.NodeConstraints != nil {
		advice.Spec.Advice.NodeConstraints = &advicev1alpha1.NodeConstraints{
			Exclusions: a.Advice.NodeConstraints.Exclusions,
			Inclusions: a.Advice.NodeConstraints.Inclusions,
		}
	}

	// Convert issues
	if len(a.Issues) > 0 {
		advice.Spec.Issues = make([]advicev1alpha1.Issue, len(a.Issues))
		for i, apiIssue := range a.Issues {
			issue := advicev1alpha1.Issue{
				IssueType:        apiIssue.IssueType,
				Timestamp:        apiIssue.Timestamp,
				DetectedBy:       sanitizeDetectedBy(apiIssue.DetectedBy),
				RelevantLogLines: apiIssue.RelevantLogLines,
			}

			// Convert remediation actions
			if len(apiIssue.RemediationActions) > 0 {
				issue.RemediationActions = make([]advicev1alpha1.RemediationAction, len(apiIssue.RemediationActions))
				for j, apiAction := range apiIssue.RemediationActions {
					action := advicev1alpha1.RemediationAction{
						ActionType: apiAction.ActionType,
						Timestamp:  apiAction.Timestamp,
						Reason:     apiAction.Reason,
					}

					// Convert resource details
					if len(apiAction.ResourceDetails) > 0 {
						action.ResourceDetails = make([]advicev1alpha1.ResourceDetail, len(apiAction.ResourceDetails))
						for k, apiDetail := range apiAction.ResourceDetails {
							action.ResourceDetails[k] = advicev1alpha1.ResourceDetail{
								ResourceType: apiDetail.ResourceType,
								ResourceId:   apiDetail.ResourceID,
								Details:      apiDetail.Details,
							}
						}
					}

					issue.RemediationActions[j] = action
				}
			}

			advice.Spec.Issues[i] = issue
		}
	}

	return advice, nil
}

func sanitizeDetectedBy(detectedBy string) string {
	trimmed := strings.TrimSpace(detectedBy)
	if trimmed == "" {
		return defaultDetectedBy
	}
	return trimmed
}

// APIAdviceRecommendation represents the advice recommendation from the API.
type APIAdviceRecommendation struct {
	State           *string             `json:"state,omitempty"`
	NodeConstraints *APINodeConstraints `json:"nodeConstraints,omitempty"`
}

// APINodeConstraints represents node constraints from the API.
type APINodeConstraints struct {
	Exclusions []string `json:"exclusions,omitempty"`
	Inclusions []string `json:"inclusions,omitempty"`
}

// APIIssue represents an issue from the API.
type APIIssue struct {
	IssueType          string                 `json:"issueType"`
	Timestamp          string                 `json:"timestamp"`
	DetectedBy         string                 `json:"detectedBy"`
	RelevantLogLines   []string               `json:"relevantLogLines,omitempty"`
	RemediationActions []APIRemediationAction `json:"remediationActions,omitempty"`
}

// APIRemediationAction represents a remediation action from the API.
type APIRemediationAction struct {
	ActionType      string              `json:"actionType"`
	Timestamp       string              `json:"timestamp"`
	Reason          string              `json:"reason"`
	ResourceDetails []APIResourceDetail `json:"resourceDetails,omitempty"`
}

// APIResourceDetail represents resource details from the API.
type APIResourceDetail struct {
	ResourceType string `json:"resourceType"`
	ResourceID   string `json:"resourceId"`
	Details      string `json:"details,omitempty"`
}
