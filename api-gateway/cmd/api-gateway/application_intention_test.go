// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	pb "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/go/heimdall/v1"
	"github.com/dsx-ai-factory/nvidia-recovery/api-gateway/internal/server"
	"github.com/dsx-ai-factory/nvidia-recovery/api-gateway/internal/server/auth"
	"github.com/dsx-ai-factory/nvidia-recovery/api-gateway/internal/server/job"
	"github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// Exercise signed-token identity propagation and backend authorization errors
// through the production HTTP/gRPC registrations. The fixture models only the
// downstream HTTP contract; real-MySQL tests establish the ownership decisions.
func TestApplicationIntentionPublicTransports(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	jwks := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(jose.JSONWebKeySet{Keys: []jose.JSONWebKey{{Key: &key.PublicKey, KeyID: "test", Algorithm: "RS256", Use: "sig"}}}); err != nil {
			t.Error(err)
		}
	}))
	t.Cleanup(jwks.Close)
	verifier, err := auth.NewJwtAuth(context.Background(), auth.JwtAuthConfig{
		Issuer: "https://issuer.example.com", Audiences: []string{"recovery"}, JWKSURL: jwks.URL, SubjectClaim: "preferred_username",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	authentication := auth.NewAuth()
	authentication.AddAuthenticator(verifier)
	signer, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.RS256, Key: key}, (&jose.SignerOptions{}).WithType("JWT"))
	if err != nil {
		t.Fatal(err)
	}
	tokens := map[string]string{}
	for _, user := range []string{"alice", "bob"} {
		tokens[user], err = jwt.Signed(signer).Claims(jwt.Claims{
			Issuer: "https://issuer.example.com", Subject: "opaque-" + user, Audience: jwt.Audience{"recovery"}, Expiry: jwt.NewNumericDate(time.Now().Add(time.Hour)),
		}).Claims(map[string]any{"preferred_username": user}).Serialize()
		if err != nil {
			t.Fatal(err)
		}
	}
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		requester := r.URL.Query().Get("requested_by")
		if r.Method == http.MethodPost {
			var body struct {
				RequestedBy string `json:"requested_by"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Error(err)
				w.WriteHeader(400)
				return
			}
			requester = body.RequestedBy
		}
		if requester != "alice" {
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"detail":"owner mismatch"}`))
			return
		}
		_, _ = w.Write([]byte(`{"id":7,"job_id":"42","desired_state":"CANCELLED","authorization_status":"AUTHORIZED","created_at":"2026-11-02T12:00:00Z"}`))
	}))
	t.Cleanup(backend.Close)
	listener := bufconn.Listen(1 << 20)
	rpcServer := grpc.NewServer(grpc.UnaryInterceptor(authentication.GrpcAuthInterceptorfunc))
	pb.RegisterHeimdallServiceServer(rpcServer, server.NewHeimdallServer(job.NewJobClientSource(backend.URL)))
	go func() { _ = rpcServer.Serve(listener) }()
	t.Cleanup(func() { rpcServer.Stop(); _ = listener.Close() })
	conn, err := grpc.NewClient("passthrough:///intentions", grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	client := pb.NewHeimdallServiceClient(conn)
	mux := newGatewayMux()
	if err := registerGatewayHandlers(context.Background(), mux, conn); err != nil {
		t.Fatal(err)
	}
	handler := authentication.HttpAuthMiddleware(mux)
	for _, test := range []struct {
		user       string
		httpStatus int
		code       codes.Code
	}{
		{"", http.StatusUnauthorized, codes.Unauthenticated},
		{"bob", http.StatusForbidden, codes.PermissionDenied},
		{"alice", http.StatusOK, codes.OK},
	} {
		t.Run(test.user, func(t *testing.T) {
			ctx := context.Background()
			if token := tokens[test.user]; token != "" {
				ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", "Bearer "+token))
			}
			result, err := client.CreateApplicationIntention(ctx, &pb.CreateApplicationIntentionRequest{
				JobId: "42", DesiredState: pb.ApplicationIntentionDesiredState_APPLICATION_INTENTION_DESIRED_STATE_CANCELLED,
			})
			if status.Code(err) != test.code {
				t.Fatalf("gRPC: %v", err)
			}
			if err == nil && result.ApplicationIntention.GetAuthorizationStatus() != "AUTHORIZED" {
				t.Fatal(result)
			}
			for _, method := range []string{http.MethodPost, http.MethodGet} {
				path := "/v1/application-intentions"
				if method == http.MethodGet {
					path += "/42?requested_by=bob"
				}
				req := httptest.NewRequest(method, path, strings.NewReader(`{"jobId":"42","desiredState":"APPLICATION_INTENTION_DESIRED_STATE_CANCELLED"}`))
				req.Header.Set("Content-Type", "application/json")
				req.Header.Set("X-Requested-By", "alice")
				if token := tokens[test.user]; token != "" {
					req.Header.Set("Authorization", "Bearer "+token)
				}
				response := httptest.NewRecorder()
				handler.ServeHTTP(response, req)
				if response.Code != test.httpStatus {
					t.Fatalf("HTTP %s: %d %s", method, response.Code, response.Body.String())
				}
			}
		})
	}
}
