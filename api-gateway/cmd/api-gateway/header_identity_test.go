// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	pb "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/go/heimdall/v1"
	"github.com/dsx-ai-factory/nvidia-recovery/api-gateway/internal/server/auth"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// TestHeaderIdentityConfiguration prevents exposure through either public API listener.
func TestHeaderIdentityConfiguration(t *testing.T) {
	valid := Config{Auth: AuthConfig{TrustHeaderBasedIdentity: true}, Server: ServerConfig{
		EnableExternalEndpoints: true, HTTPServerEndpoint: "127.0.0.1:8080", GRPCServerEndpoint: "[::1]:9090",
		InternalHTTPServerEndpoint: "0.0.0.0:8081", InternalGRPCServerEndpoint: "0.0.0.0:9091",
	}}
	if err := valid.validateHeaderIdentity(); err != nil {
		t.Fatal(err)
	}
	for _, address := range []string{"0.0.0.0:8080", ":8080", "[::]:8080", "10.0.0.1:8080", "localhost:8080", "invalid"} {
		for _, listener := range []string{"HTTP", "GRPC"} {
			t.Run(listener+"/"+address, func(t *testing.T) {
				c := valid
				switch listener {
				case "HTTP":
					c.Server.HTTPServerEndpoint = address
				case "GRPC":
					c.Server.GRPCServerEndpoint = address
				}
				if err := c.validateHeaderIdentity(); err == nil {
					t.Fatal("non-loopback API listener accepted")
				}
				c.Auth.TrustHeaderBasedIdentity = false
				if err := c.validateHeaderIdentity(); err != nil {
					t.Fatal("ordinary mode changed", err)
				}
			})
		}
	}
	valid.Auth.Insecure = true
	if err := valid.validateHeaderIdentity(); err == nil {
		t.Fatal("insecure trust mode accepted")
	}
}

// headerIdentityService observes identity after actual HTTP-to-gRPC conversion.
type headerIdentityService struct {
	pb.UnimplementedHeimdallServiceServer
}

func (headerIdentityService) GetJobs(ctx context.Context, _ *pb.GetJobsRequest) (*pb.GetJobsResponse, error) {
	identity := auth.IdentityFromContext(ctx)
	if identity == nil || identity.ClientID != "client-1" {
		return nil, status.Error(codes.Internal, "identity lost")
	}
	return &pb.GetJobsResponse{}, nil
}

// TestHeaderIdentityTransports proves identity crosses the generated adapter, not just HTTP middleware.
func TestHeaderIdentityTransports(t *testing.T) {
	chain := auth.NewAuth()
	chain.AddAuthenticator(auth.HeaderBasedIdentityAuth{})
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer(grpc.ChainUnaryInterceptor(auth.HeaderIdentityUnaryInterceptor(true), chain.GrpcAuthInterceptorfunc))
	pb.RegisterHeimdallServiceServer(server, headerIdentityService{})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close() })
	conn, err := grpc.NewClient("passthrough:///identity", grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	mux := newGatewayMux(runtime.WithIncomingHeaderMatcher(gatewayHeaderMatcher(true)))
	if err := registerGatewayHandlers(t.Context(), mux, conn); err != nil {
		t.Fatal(err)
	}
	handler := auth.HeaderIdentityBoundary(true, chain.HttpAuthMiddleware(mux))
	for _, tc := range []struct {
		name    string
		headers http.Header
		want    int
	}{
		{"valid", http.Header{"X-Nvr-Auth-Mechanism": {"service"}, "X-Nvr-Client-Id": {"client-1"}}, 200},
		{"missing", http.Header{}, 401},
		{"partial", http.Header{"X-Nvr-Client-Id": {"client-1"}}, 401},
		{"alias only", http.Header{"Grpc-Metadata-X-Nvr-Auth-Mechanism": {"service"}, "Grpc-Metadata-X-Nvr-Client-Id": {"client-1"}}, 401},
		{"spoofed alias", http.Header{"X-Nvr-Auth-Mechanism": {"service"}, "X-Nvr-Client-Id": {"client-1"}, "Grpc-Metadata-X-Nvr-Client-Id": {"spoofed"}}, 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/v1/jobs", nil)
			r.Header = tc.headers
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, r)
			if w.Code != tc.want {
				t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
			}
		})
	}
	ctx := metadata.NewOutgoingContext(t.Context(), metadata.Pairs("x-nvr-auth-mechanism", "service", "x-nvr-client-id", "client-1"))
	if _, err := pb.NewHeimdallServiceClient(conn).GetJobs(ctx, &pb.GetJobsRequest{}); err != nil {
		t.Fatal(err)
	}
}

// TestHeaderIdentityInternalListener preserves anonymous internal calls without trusting assertions.
func TestHeaderIdentityInternalListener(t *testing.T) {
	chain := auth.NewAuth()
	chain.AddAuthenticator(auth.HeaderBasedIdentityAuth{})
	config := gatewayConfig{
		publicEndpoint:   endpointConfig{trustHeaderBasedIdentity: true},
		internalEndpoint: endpointConfig{insecure: true},
		auth:             chain,
	}
	endpoint := &config.internalEndpoint
	if endpointAuthenticator(&config, endpoint) != nil {
		t.Fatal("public header trust enabled internal authentication")
	}
	handler := auth.HeaderIdentityBoundary(endpoint.trustHeaderBasedIdentity, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get(auth.HeaderClientID) != "" || r.Header.Get(auth.HeaderAuthMechanism) != "" {
			t.Error("internal HTTP retained asserted identity")
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	r := httptest.NewRequest(http.MethodGet, "/v1/jobs", nil)
	r.Header.Set(auth.HeaderClientID, "spoofed")
	r.Header.Set(auth.HeaderAuthMechanism, "service")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	if w.Code != http.StatusNoContent {
		t.Fatalf("internal HTTP status = %d", w.Code)
	}
	interceptor := auth.HeaderIdentityUnaryInterceptor(endpoint.trustHeaderBasedIdentity)
	ctx := metadata.NewIncomingContext(t.Context(), metadata.Pairs("x-nvr-client-id", "spoofed", "x-nvr-auth-mechanism", "service"))
	_, err := interceptor(ctx, nil, &grpc.UnaryServerInfo{}, func(ctx context.Context, _ any) (any, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		if len(md.Get("x-nvr-client-id")) != 0 || len(md.Get("x-nvr-auth-mechanism")) != 0 {
			t.Error("internal gRPC retained asserted identity")
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	config.internalAuth = auth.NewAuth()
	endpoint.authenticate = true
	if endpointAuthenticator(&config, endpoint) != config.internalAuth {
		t.Fatal("existing internal authenticator was replaced")
	}
}
