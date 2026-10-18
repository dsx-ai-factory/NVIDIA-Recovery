// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package auth

import (
	"context"
	"net/http"

	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	authenticationv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// KubernetesTokenReviewAuthConfig configures projected ServiceAccount token validation.
type KubernetesTokenReviewAuthConfig struct {
	Audience         string
	ExpectedUsername string
}

func (c KubernetesTokenReviewAuthConfig) Validate() error {
	if c.Audience == "" {
		return errors.New("Kubernetes token review audience is required")
	}
	if c.ExpectedUsername == "" {
		return errors.New("Kubernetes token review expected username is required")
	}
	return nil
}

// KubernetesTokenReviewResult is the subset of TokenReview status needed by the gateway.
type KubernetesTokenReviewResult struct {
	Authenticated bool
	Username      string
	Audiences     []string
}

// KubernetesTokenReviewer validates a presented token with the Kubernetes API server.
type KubernetesTokenReviewer interface {
	ReviewToken(ctx context.Context, token string, audiences []string) (KubernetesTokenReviewResult, error)
}

// KubernetesTokenReviewAuth authenticates projected ServiceAccount tokens using TokenReview.
type KubernetesTokenReviewAuth struct {
	config KubernetesTokenReviewAuthConfig
	client KubernetesTokenReviewer
}

func NewKubernetesTokenReviewAuth(client KubernetesTokenReviewer, config KubernetesTokenReviewAuthConfig) (*KubernetesTokenReviewAuth, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	if client == nil {
		return nil, errors.New("Kubernetes token reviewer is required")
	}
	return &KubernetesTokenReviewAuth{config: config, client: client}, nil
}

func (a *KubernetesTokenReviewAuth) CheckGrpc(ctx context.Context, _ interface{}, _ *grpc.UnaryServerInfo) (bool, context.Context, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return false, nil, nil
	}
	authorization := md.Get("authorization")
	if len(authorization) == 0 {
		return false, nil, nil
	}
	token, ok := bearerToken(authorization[0])
	if !ok {
		return false, nil, nil
	}
	return a.checkToken(ctx, token)
}

func (a *KubernetesTokenReviewAuth) CheckHttp(ctx context.Context, req *http.Request) (bool, context.Context, error) {
	token, ok := bearerToken(req.Header.Get("Authorization"))
	if !ok {
		return false, nil, nil
	}
	return a.checkToken(ctx, token)
}

func (a *KubernetesTokenReviewAuth) checkToken(ctx context.Context, token string) (bool, context.Context, error) {
	review, err := a.client.ReviewToken(ctx, token, []string{a.config.Audience})
	if err != nil || !review.Authenticated || review.Username != a.config.ExpectedUsername || !containsAudience(review.Audiences, a.config.Audience) {
		return false, nil, nil
	}

	return true, ContextWithIdentity(ctx, &Identity{
		Mechanism: AuthMechanismKubernetes,
		UserID:    review.Username,
	}), nil
}

func containsAudience(audiences []string, audience string) bool {
	for _, candidate := range audiences {
		if candidate == audience {
			return true
		}
	}
	return false
}

// NewInClusterKubernetesTokenReviewer creates the production TokenReview client
// using the Pod's ServiceAccount credentials.
func NewInClusterKubernetesTokenReviewer() (KubernetesTokenReviewer, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, errors.Wrap(err, "load in-cluster Kubernetes configuration")
	}
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, errors.Wrap(err, "create Kubernetes client")
	}
	return &kubernetesTokenReviewer{client: client}, nil
}

type kubernetesTokenReviewer struct {
	client kubernetes.Interface
}

func (c *kubernetesTokenReviewer) ReviewToken(ctx context.Context, token string, audiences []string) (KubernetesTokenReviewResult, error) {
	review, err := c.client.AuthenticationV1().TokenReviews().Create(ctx, &authenticationv1.TokenReview{
		Spec: authenticationv1.TokenReviewSpec{
			Token:     token,
			Audiences: audiences,
		},
	}, metav1.CreateOptions{})
	if err != nil {
		return KubernetesTokenReviewResult{}, errors.Wrap(err, "create Kubernetes TokenReview")
	}
	return KubernetesTokenReviewResult{
		Authenticated: review.Status.Authenticated,
		Username:      review.Status.User.Username,
		Audiences:     review.Status.Audiences,
	}, nil
}
