// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package auth

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const (
	testKubernetesAudience = "heimdall-api-gateway"
	testServiceAccountUser = "system:serviceaccount:slurm:slurm-advisor-controller"
)

func TestKubernetesTokenReviewAuthAcceptsConfiguredServiceAccount(t *testing.T) {
	client := &fakeKubernetesTokenReviewer{
		result: KubernetesTokenReviewResult{
			Authenticated: true,
			Username:      testServiceAccountUser,
			Audiences:     []string{testKubernetesAudience},
		},
	}
	authn, err := NewKubernetesTokenReviewAuth(client, KubernetesTokenReviewAuthConfig{
		Audience:         testKubernetesAudience,
		ExpectedUsername: testServiceAccountUser,
	})
	if err != nil {
		t.Fatalf("NewKubernetesTokenReviewAuth() error = %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/jobs", nil)
	req.Header.Set("Authorization", "Bearer projected-token")
	ok, ctx, err := authn.CheckHttp(context.Background(), req)
	if err != nil {
		t.Fatalf("CheckHttp() error = %v", err)
	}
	if !ok {
		t.Fatal("CheckHttp() accepted = false, want true")
	}
	if client.token != "projected-token" {
		t.Fatalf("TokenReview token = %q, want projected token", client.token)
	}
	if !reflect.DeepEqual(client.audiences, []string{testKubernetesAudience}) {
		t.Fatalf("TokenReview audiences = %v, want [%s]", client.audiences, testKubernetesAudience)
	}
	identity := IdentityFromContext(ctx)
	if identity == nil {
		t.Fatal("identity missing from authenticated context")
	}
	if identity.Mechanism != AuthMechanismKubernetes {
		t.Fatalf("identity mechanism = %q, want %q", identity.Mechanism, AuthMechanismKubernetes)
	}
	if identity.UserID != testServiceAccountUser {
		t.Fatalf("identity user ID = %q, want %q", identity.UserID, testServiceAccountUser)
	}
}

func TestKubernetesTokenReviewAuthAuthenticatesGRPCBearerToken(t *testing.T) {
	client := &fakeKubernetesTokenReviewer{
		result: KubernetesTokenReviewResult{
			Authenticated: true,
			Username:      testServiceAccountUser,
			Audiences:     []string{testKubernetesAudience},
		},
	}
	authn, err := NewKubernetesTokenReviewAuth(client, KubernetesTokenReviewAuthConfig{
		Audience:         testKubernetesAudience,
		ExpectedUsername: testServiceAccountUser,
	})
	if err != nil {
		t.Fatalf("NewKubernetesTokenReviewAuth() error = %v", err)
	}

	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer projected-token"))
	ok, authenticatedCtx, err := authn.CheckGrpc(ctx, nil, &grpc.UnaryServerInfo{})
	if err != nil {
		t.Fatalf("CheckGrpc() error = %v", err)
	}
	if !ok {
		t.Fatal("CheckGrpc() accepted = false, want true")
	}
	if identity := IdentityFromContext(authenticatedCtx); identity == nil || identity.UserID != testServiceAccountUser {
		t.Fatalf("identity = %#v, want service account identity", identity)
	}
}

func TestKubernetesTokenReviewAuthRejectsUnauthenticatedWrongUserAndFailedReview(t *testing.T) {
	tests := []struct {
		name   string
		result KubernetesTokenReviewResult
		err    error
	}{
		{name: "unauthenticated", result: KubernetesTokenReviewResult{}},
		{name: "wrong username", result: KubernetesTokenReviewResult{Authenticated: true, Username: "system:serviceaccount:slurm:other"}},
		{name: "review error", err: errors.New("kubernetes unavailable")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &fakeKubernetesTokenReviewer{result: tt.result, err: tt.err}
			authn, err := NewKubernetesTokenReviewAuth(client, KubernetesTokenReviewAuthConfig{
				Audience:         testKubernetesAudience,
				ExpectedUsername: testServiceAccountUser,
			})
			if err != nil {
				t.Fatalf("NewKubernetesTokenReviewAuth() error = %v", err)
			}

			req := httptest.NewRequest(http.MethodGet, "/v1/jobs", nil)
			req.Header.Set("Authorization", "Bearer projected-token")
			ok, _, checkErr := authn.CheckHttp(context.Background(), req)
			if checkErr != nil {
				t.Fatalf("CheckHttp() error = %v", checkErr)
			}
			if ok {
				t.Fatal("CheckHttp() accepted = true, want false")
			}
		})
	}
}

func TestKubernetesTokenReviewAuthRejectsMissingOrMismatchedReviewedAudience(t *testing.T) {
	for _, result := range []KubernetesTokenReviewResult{
		{Authenticated: true, Username: testServiceAccountUser},
		{Authenticated: true, Username: testServiceAccountUser, Audiences: []string{"other-service"}},
	} {
		client := &fakeKubernetesTokenReviewer{result: result}
		authn, err := NewKubernetesTokenReviewAuth(client, KubernetesTokenReviewAuthConfig{
			Audience:         testKubernetesAudience,
			ExpectedUsername: testServiceAccountUser,
		})
		if err != nil {
			t.Fatalf("NewKubernetesTokenReviewAuth() error = %v", err)
		}

		req := httptest.NewRequest(http.MethodGet, "/v1/jobs", nil)
		req.Header.Set("Authorization", "Bearer projected-token")
		ok, _, err := authn.CheckHttp(context.Background(), req)
		if err != nil {
			t.Fatalf("CheckHttp() error = %v", err)
		}
		if ok {
			t.Fatalf("CheckHttp() accepted = true for reviewed audiences %v, want false", result.Audiences)
		}
	}
}

func TestKubernetesTokenReviewAuthRejectsMissingBearerTokenWithoutCallingKubernetes(t *testing.T) {
	client := &fakeKubernetesTokenReviewer{}
	authn, err := NewKubernetesTokenReviewAuth(client, KubernetesTokenReviewAuthConfig{
		Audience:         testKubernetesAudience,
		ExpectedUsername: testServiceAccountUser,
	})
	if err != nil {
		t.Fatalf("NewKubernetesTokenReviewAuth() error = %v", err)
	}

	ok, _, err := authn.CheckHttp(context.Background(), httptest.NewRequest(http.MethodGet, "/v1/jobs", nil))
	if err != nil {
		t.Fatalf("CheckHttp() error = %v", err)
	}
	if ok {
		t.Fatal("CheckHttp() accepted = true, want false")
	}
	if client.calls != 0 {
		t.Fatalf("TokenReview calls = %d, want 0", client.calls)
	}
}

func TestKubernetesTokenReviewAuthRejectsIncompleteConfiguration(t *testing.T) {
	for _, config := range []KubernetesTokenReviewAuthConfig{
		{ExpectedUsername: testServiceAccountUser},
		{Audience: testKubernetesAudience},
	} {
		if _, err := NewKubernetesTokenReviewAuth(&fakeKubernetesTokenReviewer{}, config); err == nil {
			t.Fatalf("NewKubernetesTokenReviewAuth(%+v) error = nil, want configuration error", config)
		}
	}
}

type fakeKubernetesTokenReviewer struct {
	result    KubernetesTokenReviewResult
	err       error
	calls     int
	token     string
	audiences []string
}

func (f *fakeKubernetesTokenReviewer) ReviewToken(_ context.Context, token string, audiences []string) (KubernetesTokenReviewResult, error) {
	f.calls++
	f.token = token
	f.audiences = append([]string(nil), audiences...)
	return f.result, f.err
}
