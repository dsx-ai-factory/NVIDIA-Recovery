// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/cockroachdb/errors"
	pb "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/go/heimdall/v1"
	"github.com/dsx-ai-factory/nvidia-recovery/api-gateway/internal/server"
	"github.com/dsx-ai-factory/nvidia-recovery/api-gateway/internal/server/audit"
	"github.com/dsx-ai-factory/nvidia-recovery/api-gateway/internal/server/auth"
	"github.com/dsx-ai-factory/nvidia-recovery/api-gateway/internal/server/job"
	"github.com/dsx-ai-factory/nvidia-recovery/api-gateway/internal/server/observability"
	"github.com/dsx-ai-factory/nvidia-recovery/api-gateway/internal/util"
	"golang.org/x/sync/errgroup"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	typeid "go.jetify.com/typeid/v2"
	"go.opentelemetry.io/contrib/bridges/otelzap"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	health "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"

	slogzap "github.com/samber/slog-zap/v2"
)

// contextKey is a custom type for context keys to avoid collisions
type contextKey string

const (
	// loggerKey is the context key for the zap logger
	loggerKey contextKey = "zapLogger"

	auditRequestTimeout = 10 * time.Second
)

// LoggerFromContext retrieves the logger from context or returns the global logger
func LoggerFromContext(ctx context.Context) *zap.Logger {
	if logger, ok := ctx.Value(loggerKey).(*zap.Logger); ok {
		return logger
	}
	return zap.L()
}

// ContextWithLogger adds a logger to the context
func ContextWithLogger(ctx context.Context, logger *zap.Logger) context.Context {
	return context.WithValue(ctx, loggerKey, logger)
}

// requestIDInterceptor extracts or generates a request ID from the gRPC metadata (headers) and adds it to the request logging context.
//
// With this interceptor all logs will have a field called "id" with the request ID.
func requestIDInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	// Extract or generate request ID from gRPC metadata
	var requestID string
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		values := md.Get(strings.ToLower(util.RequestIDHeader))
		if len(values) > 0 {
			requestID = values[0]
		}
	}
	if requestID == "" {
		requestID = typeid.MustGenerate("req").String()
	}

	logger := zap.L().With(zap.String("id", requestID))
	ctx = ContextWithLogger(ctx, logger)

	return handler(ctx, req)
}

// grpcLogInterceptor logs the gRPC request to zap
func grpcLogInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	logger := LoggerFromContext(ctx)

	if !strings.HasPrefix(info.FullMethod, "/grpc.health.v1.Health") {
		logger.Info("gRPC Request", zap.String("method", info.FullMethod))
	}

	resp, err := handler(ctx, req)
	if err != nil {
		logger.Error("gRPC Request failed", zap.String("method", info.FullMethod), zap.Error(err))
	} else if !strings.HasPrefix(info.FullMethod, "/grpc.health.v1.Health") {
		logger.Info("gRPC Response", zap.String("method", info.FullMethod))
	}

	return resp, err
}

func registerGrpcServices(
	grpcServer *grpc.Server,
	heimdallServer pb.HeimdallServiceServer,
	apiKeysServer pb.ApiKeysServiceServer,
	auditServer pb.AuditServiceServer,
) {
	pb.RegisterHeimdallServiceServer(grpcServer, heimdallServer)
	pb.RegisterApiKeysServiceServer(grpcServer, apiKeysServer)
	pb.RegisterAuditServiceServer(grpcServer, auditServer)
}

func registerGatewayHandlers(ctx context.Context, mux *runtime.ServeMux, conn *grpc.ClientConn) error {
	if err := pb.RegisterHeimdallServiceHandler(ctx, mux, conn); err != nil {
		return fmt.Errorf("failed to register Heimdall gRPC-Gateway handler: %w", err)
	}
	if err := pb.RegisterApiKeysServiceHandler(ctx, mux, conn); err != nil {
		return fmt.Errorf("failed to register API keys gRPC-Gateway handler: %w", err)
	}
	if err := pb.RegisterAuditServiceHandler(ctx, mux, conn); err != nil {
		return errors.Wrap(err, "register Audit Trail gRPC-Gateway handler")
	}
	return nil
}

// runGrpcServer starts a gRPC server which accepts requests from the GRPC Gateway (HTTP server)
// reverse proxy. This is the entry point for all gRPC requests.
func runGrpcServer(ctx context.Context, heimdallServer pb.HeimdallServiceServer, auditServer pb.AuditServiceServer, config *gatewayConfig, endpoint *endpointConfig) error {
	// 1. Start the gRPC server
	lis, err := net.Listen("tcp", endpoint.grpcServerEndpoint)
	if err != nil {
		return fmt.Errorf("failed to listen on gRPC endpoint %s: %w", endpoint.grpcServerEndpoint, err)
	}
	defer lis.Close() //nolint:errcheck

	grpcInterceptors := []grpc.UnaryServerInterceptor{}
	grpcInterceptors = append(grpcInterceptors, requestIDInterceptor)
	grpcInterceptors = append(grpcInterceptors, grpcLogInterceptor)
	if authenticator := endpointAuthenticator(config, endpoint); authenticator != nil {
		grpcInterceptors = append(grpcInterceptors, authenticator.GrpcAuthInterceptorfunc)
	}

	grpcOpts := []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(grpcInterceptors...),
	}
	if config.otelProvider != nil && (config.otelProvider.IsTracingEnabled() || config.otelProvider.IsMetricsEnabled()) {
		grpcOpts = append(grpcOpts, observability.GrpcServerStatsHandler())
	}

	grpcServer := grpc.NewServer(grpcOpts...)

	// IMPORTANT: These next lines register the actual grpc service implementations
	// This is the entry point for all gRPC requests.
	registerGrpcServices(grpcServer, heimdallServer, server.NewDisabledApiKeysServer(), auditServer)

	if heimdallServerHealth, ok := heimdallServer.(health.HealthServer); ok {
		health.RegisterHealthServer(grpcServer, heimdallServerHealth)
	} else {
		zap.L().Error("heimdallServer does not implement health.HealthServer")
		return fmt.Errorf("heimdallServer does not implement health.HealthServer")
	}

	go func() {
		<-ctx.Done()
		zap.L().Info("Shutting down gRPC server", zap.String("endpoint", endpoint.grpcServerEndpoint))
		grpcServer.GracefulStop()
	}()

	zap.L().Info("gRPC server listening", zap.String("endpoint", endpoint.grpcServerEndpoint))
	if err := grpcServer.Serve(lis); err != nil {
		zap.L().Error("failed to serve gRPC", zap.Error(err))
		return fmt.Errorf("failed to serve gRPC: %w", err)
	}

	return nil
}

// runMetricsServer starts a dedicated HTTP server that serves Prometheus metrics.
func runMetricsServer(ctx context.Context, config *gatewayConfig) error {
	mux := http.NewServeMux()
	mux.Handle("/metrics", config.otelProvider.MetricsHandler())

	srv := &http.Server{
		Addr:    config.metricsEndpoint,
		Handler: mux,
	}

	go func() {
		<-ctx.Done()
		zap.L().Info("Shutting down metrics server", zap.String("endpoint", config.metricsEndpoint))
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			zap.L().Error("failed to shutdown metrics server", zap.Error(err))
		}
	}()

	zap.L().Info("Metrics server listening", zap.String("endpoint", config.metricsEndpoint))
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("failed to serve metrics: %w", err)
	}
	return nil
}

// runGatewayServer starts a HTTP server that proxies requests to the gRPC server
func runGatewayServer(ctx context.Context, config *gatewayConfig, endpoint *endpointConfig) error {
	zap.L().Info("Configuring HTTP gateway server", zap.String("endpoint", endpoint.httpServerEndpoint))
	// 2. Start the gRPC Gateway (HTTP server)
	// Create a client connection to the gRPC server.
	// gRPC-Gateway will proxy HTTP requests to this connection.
	conn, err := grpc.NewClient(endpoint.grpcServerEndpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to dial gRPC server at %s: %w", endpoint.grpcServerEndpoint, err)
	}
	defer conn.Close() //nolint:errcheck

	// Create the gRPC-Gateway mux with header forwarding for request IDs.
	gwmux := newGatewayMux(
		runtime.WithIncomingHeaderMatcher(func(key string) (string, bool) {
			// Match all case variations of util.RequestIDHeader
			if strings.EqualFold(key, util.RequestIDHeader) {
				return util.RequestIDHeader, true
			}
			return runtime.DefaultHeaderMatcher(key)
		}),
		runtime.WithHealthzEndpoint(health.NewHealthClient(conn)),
	)
	if err := registerGatewayHandlers(ctx, gwmux, conn); err != nil {
		return err
	}

	// requestIDMiddleware generates or extracts request IDs and adds them to context
	requestIDMiddleware := func(handler http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Extract request ID from header or generate a new one
			requestID := r.Header.Get(util.RequestIDHeader)
			if requestID == "" {
				requestID = typeid.MustGenerate("req").String()
				r.Header.Set(util.RequestIDHeader, requestID)
			}

			// Add request ID to response header
			w.Header().Set(util.RequestIDHeader, requestID)

			// Add request ID to context
			ctx := context.WithValue(r.Context(), util.RequestIDKey, requestID)
			r = r.WithContext(ctx)

			handler.ServeHTTP(w, r)
		})
	}

	// TODO: This is a temporary solution to allow requests from any domain. Remove this eventually.
	// corsMiddleware adds CORS headers to allow requests from any domain
	corsMiddleware := func(handler http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Access-Control-Allow-Origin", *config.corsAllowOrigin)
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Request-ID")

			if r.Method == "OPTIONS" {
				w.WriteHeader(http.StatusOK)
				return
			}

			handler.ServeHTTP(w, r)
		})
	}

	// stripUrlPrefixMiddleware removes the given prefix from the request URL's path.
	stripUrlPrefixMiddleware := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if endpoint.urlPrefix != "" && strings.HasPrefix(r.URL.Path, endpoint.urlPrefix) {
				newPath := strings.TrimPrefix(r.URL.Path, endpoint.urlPrefix)
				if newPath == "" {
					r.URL.Path = "/"
				} else {
					if newPath[0] != '/' {
						r.URL.Path = "/" + newPath
					} else {
						r.URL.Path = newPath
					}
				}
				// Note: r.URL.RawPath is not explicitly handled here for simplicity.
				// Most muxes, including gRPC-Gateway's, primarily use r.URL.Path.
				// If RawPath handling becomes an issue, this middleware might need
				// to update r.URL.RawPath carefully.
			}
			next.ServeHTTP(w, r)
		})
	}

	// Build authenticated API handler chain (only auth here, other middlewares wrap root)
	var apiHandler http.Handler = gwmux
	if authenticator := endpointAuthenticator(config, endpoint); authenticator != nil {
		apiHandler = authenticator.HttpAuthMiddleware(apiHandler)
	}

	// Build public handler chain for OIDC login/callback (no auth)
	// TODO: Rate limit login/callback endpoints to prevent abuse
	//       Especially important since these endpoints are public.
	loginMux := http.NewServeMux()

	if !endpoint.insecure {
		if config.oidcRP != nil {
			// Build session cookie manager for UI cookies
			zap.L().Info("registering OIDC login handlers")
			config.oidcRP.RegisterHandlers(loginMux)
		} else {
			zap.L().Info("no OIDC login handlers registered")
		}
	}

	// Root mux: route public endpoints and default to API handler
	rootMux := http.NewServeMux()
	registerGatewayHealthRoute(rootMux)

	if !endpoint.insecure {
		var publicHandler http.Handler = loginMux

		rootMux.Handle(auth.LoginPath, publicHandler)
		rootMux.Handle(auth.CallbackPath, publicHandler)
		rootMux.Handle(auth.RefreshPath, publicHandler)
		rootMux.Handle(auth.LogoutPath, publicHandler)
	}

	rootMux.Handle("/", apiHandler)

	// recoveryMiddleware handles panics and logs them
	recoveryMiddleware := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if err := recover(); err != nil {
					zap.L().Error("panic recovered", zap.Any("error", err), zap.String("path", r.URL.Path))
					http.Error(w, "Internal Server Error", http.StatusInternalServerError)
				}
			}()
			next.ServeHTTP(w, r)
		})
	}

	// loggingMiddleware logs HTTP requests (excluding /healthz)
	loggingMiddleware := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/healthz" {
				next.ServeHTTP(w, r)
				return
			}
			start := time.Now()
			next.ServeHTTP(w, r)
			logger := zap.L()
			if requestID := r.Context().Value(util.RequestIDKey); requestID != nil {
				logger = logger.With(zap.String("id", requestID.(string)))
			}
			logger.Info("HTTP Request",
				zap.String("method", r.Method),
				zap.String("path", r.URL.Path),
				zap.Duration("latency", time.Since(start)))
		})
	}

	// Wrap the entire mux with common middlewares in correct outer-to-inner order
	var finalHandler http.Handler = rootMux
	finalHandler = recoveryMiddleware(finalHandler)
	finalHandler = loggingMiddleware(finalHandler)
	if config.otelProvider != nil && (config.otelProvider.IsTracingEnabled() || config.otelProvider.IsMetricsEnabled()) {
		finalHandler = observability.HttpServerMiddleware(config.otelServiceName)(finalHandler)
	}
	finalHandler = requestIDMiddleware(finalHandler)
	finalHandler = stripUrlPrefixMiddleware(finalHandler)
	if config.corsAllowOrigin != nil {
		finalHandler = corsMiddleware(finalHandler)
	}

	gwServer := &http.Server{
		Addr:    endpoint.httpServerEndpoint,
		Handler: finalHandler,
	}

	go func() {
		<-ctx.Done()
		zap.L().Info("Shutting down HTTP Gateway server", zap.String("endpoint", endpoint.httpServerEndpoint))
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		err := gwServer.Shutdown(shutdownCtx)
		if err != nil {
			zap.L().Error("failed to shutdown HTTP Gateway server", zap.String("endpoint", endpoint.httpServerEndpoint), zap.Error(err))
		}
	}()

	zap.L().Info("HTTP Gateway server listening", zap.String("endpoint", endpoint.httpServerEndpoint))
	if err := gwServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("failed to serve HTTP gateway: %w", err)
	}

	return nil
}

type endpointConfig struct {
	insecure           bool
	authenticate       bool
	grpcServerEndpoint string
	httpServerEndpoint string
	urlPrefix          string
}

type gatewayConfig struct {
	enableExternalEndpoints bool
	publicEndpoint          endpointConfig
	internalEndpoint        endpointConfig
	loginRedirectURL        string
	corsAllowOrigin         *string
	auth                    *auth.Auth
	internalAuth            *auth.Auth
	oidcRP                  *auth.OIDCRelyingParty
	oidcConfig              auth.OidcAuthConfig
	jobSource               job.JobSource
	auditSource             audit.AuditSource
	otelProvider            *observability.OtelProvider
	otelServiceName         string
	metricsEndpoint         string
}

func endpointAuthenticator(config *gatewayConfig, endpoint *endpointConfig) *auth.Auth {
	if endpoint.insecure {
		if endpoint.authenticate {
			return config.internalAuth
		}
		return nil
	}
	return config.auth
}

func registerGatewayHealthRoute(rootMux *http.ServeMux) {
	rootMux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
}

func publicAuthenticatorConfigurationInvalid(externalEndpointsEnabled, insecure bool, authenticators *auth.Auth) bool {
	return externalEndpointsEnabled && !insecure && authenticators == nil
}

func run(config *gatewayConfig) error {
	ctx := ContextWithLogger(context.Background(), zap.L())
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Shutdown OTel provider on exit
	if config.otelProvider != nil {
		defer func() {
			shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer shutdownCancel()
			if err := config.otelProvider.Shutdown(shutdownCtx); err != nil {
				zap.L().Error("Failed to shutdown OTel provider", zap.Error(err))
			}
		}()
	}

	heimdallServer := server.NewHeimdallServer(config.jobSource)
	auditServer := server.NewAuditServer(config.auditSource)

	g, ctx := errgroup.WithContext(ctx)

	if config.enableExternalEndpoints {
		g.Go(func() error {
			zap.L().Info("Starting gRPC server", zap.String("endpoint", config.publicEndpoint.grpcServerEndpoint))
			err := runGrpcServer(ctx, heimdallServer, auditServer, config, &config.publicEndpoint)
			if err != nil {
				zap.L().Error("failed to run gRPC server", zap.Error(err))
			}
			return err
		})

		g.Go(func() error {
			zap.L().Info("Starting HTTP server", zap.String("endpoint", config.publicEndpoint.httpServerEndpoint), zap.String("url_prefix", config.publicEndpoint.urlPrefix))
			err := runGatewayServer(ctx, config, &config.publicEndpoint)
			if err != nil {
				zap.L().Error("failed to run HTTP server", zap.Error(err))
			}
			return err
		})
	}

	if config.otelProvider != nil && config.otelProvider.IsMetricsEnabled() && config.metricsEndpoint != "" {
		g.Go(func() error {
			err := runMetricsServer(ctx, config)
			if err != nil {
				zap.L().Error("failed to run metrics server", zap.Error(err))
			}
			return err
		})
	}

	g.Go(func() error {
		zap.L().Info("Starting internal gRPC server", zap.String("endpoint", config.internalEndpoint.grpcServerEndpoint))
		err := runGrpcServer(ctx, heimdallServer, auditServer, config, &config.internalEndpoint)
		if err != nil {
			zap.L().Error("failed to run gRPC server", zap.Error(err))
		}
		return err
	})

	g.Go(func() error {
		zap.L().Info("Starting internal HTTP server", zap.String("endpoint", config.internalEndpoint.httpServerEndpoint), zap.String("url_prefix", config.internalEndpoint.urlPrefix))
		err := runGatewayServer(ctx, config, &config.internalEndpoint)
		if err != nil {
			zap.L().Error("failed to run HTTP server", zap.Error(err))
		}
		return err
	})

	zap.L().Info("Heimdall API server started")

	go func() {
		// Handle SIGINT and SIGTERM
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, syscall.SIGTERM, syscall.SIGINT)
		<-ch

		go func() {
			<-ch
			panic("Additional SIGTERM received. Aborting graceful shutdown!")
		}()

		zap.L().Info("Gracefully shutting down Heimdall API server...")
		// Once we stop receiving requests start draining the inflight ones via a graceful shutdown
		cancel()
	}()

	return g.Wait()
}

func main() {
	cfg, err := loadConfig()
	if err != nil {
		// Use basic logger before configured
		logger, _ := zap.NewProduction()
		logger.Error("Failed to load config", zap.Error(err))
		os.Exit(1)
	}

	var logger *zap.Logger
	var zapConfig zap.Config

	switch cfg.LogFormat {
	case "text":
		zapConfig = zap.NewDevelopmentConfig()
		zapConfig.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
		zapConfig.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	case "json":
		zapConfig = zap.NewProductionConfig()
	default:
		// Use basic logger for error
		logger, _ = zap.NewProduction()
		logger.Error("Invalid log format", zap.String("format", cfg.LogFormat))
		os.Exit(1)
	}

	logger, err = zapConfig.Build()
	if err != nil {
		panic(fmt.Sprintf("Failed to initialize logger: %v", err))
	}
	zap.ReplaceGlobals(logger)
	defer logger.Sync() //nolint:errcheck

	var jobSource job.JobSource

	switch cfg.Job.Source {
	case "notimplemented":
		zap.L().Info("Using not implemented job source")
		jobSource = job.NewNotImplementedJobSource()
	case "jobmanager":
		zap.L().Info("Using jobmanager job source", zap.String("endpoint", cfg.Job.Endpoint))
		jobSource = job.NewJobClientSource(cfg.Job.Endpoint)
	case "synthetic":
		zap.L().Info("Using synthetic job source")
		jobSource = job.NewJobsSyntheticSource()
	}

	// Initialize OTel provider
	otelProvider, err := observability.NewOtelProvider(
		observability.OtelConfig{
			Enabled:        cfg.Otel.Enabled,
			ServiceName:    cfg.Otel.ServiceName,
			ServiceVersion: cfg.Otel.ServiceVersion,
			Insecure:       cfg.Otel.Insecure,
			SampleRate:     cfg.Otel.SampleRate,
			Protocol:       cfg.Otel.Protocol,
			Endpoint:       cfg.Otel.Endpoint,
			Token:          cfg.Otel.Token,
		},
		cfg.MetricsTelemetry.Enabled,
	)
	if err != nil {
		zap.L().Error("Failed to initialize OTel provider", zap.Error(err))
		os.Exit(1)
	}

	// Bridge zap logs to OTel if logging is enabled.
	// Uses go.opentelemetry.io/contrib/bridges/otelzap which supports trace
	// correlation when callers pass context via zap.Any("ctx", ctx).
	if otelProvider.IsLoggingEnabled() {
		lp := otelProvider.LoggingProvider()
		otelCore := otelzap.NewCore(cfg.Otel.ServiceName,
			otelzap.WithLoggerProvider(lp.Provider()),
		)
		teeCore := zapcore.NewTee(logger.Core(), otelCore)
		logger = zap.New(teeCore, zap.AddCaller(), zap.AddStacktrace(zapcore.ErrorLevel))
		zap.ReplaceGlobals(logger)
	}

	// Convert slog logs from dependencies to the final zap logger, including the
	// OTel-bridged logger when logging export is enabled.
	setDefaultSlogLogger(logger)

	auditSource, err := audit.NewAuditClientSource(
		cfg.Audit.Endpoint,
		&http.Client{Timeout: auditRequestTimeout},
	)
	if err != nil {
		logger.Error("Failed to configure audit source", zap.Error(err))
		os.Exit(1)
	}

	var authenticators *auth.Auth
	var internalAuthenticators *auth.Auth
	var oidcConfig auth.OidcAuthConfig
	var oidcRP *auth.OIDCRelyingParty

	if !cfg.Server.EnableExternalEndpoints || cfg.Auth.Insecure {
		if cfg.Auth.Insecure {
			zap.L().Info("Insecure mode enabled. Authentication is disabled.")
		}
		authenticators = nil
	} else {
		authenticators = auth.NewAuth()

		if cfg.Auth.OidcEnabled {
			oidcConfig = auth.OidcAuthConfig{
				ProviderURL:   cfg.Auth.OidcConfig.ProviderURL,
				ClientID:      cfg.Auth.OidcConfig.ClientID,
				ClientSecret:  cfg.Auth.OidcConfig.ClientSecret,
				Scopes:        cfg.Auth.OidcConfig.Scopes,
				CookieKey:     cfg.Auth.OidcConfig.CookieKey,
				SecureCookie:  cfg.Auth.OidcConfig.SecureCookie,
				CookieDomain:  cfg.Auth.OidcConfig.CookieDomain,
				SubjectClaim:  cfg.Auth.OidcConfig.SubjectClaim,
				ClientIDClaim: cfg.Auth.OidcConfig.ClientIDClaim,
				ScopesClaim:   cfg.Auth.OidcConfig.ScopesClaim,
			}

			oidcRP, err = auth.NewOIDCRelyingParty(context.Background(), oidcConfig, cfg.Auth.LoginRedirectURL)
			if err != nil {
				zap.L().Error("Failed to initialize OIDC relying party", zap.Error(err))
				os.Exit(1)
			}

			authenticators.SetSessionMiddleware(oidcRP.SessionMiddleware)

			oidcAuth := oidcRP.GetAuthenticator()
			authenticators.AddAuthenticator(oidcAuth)
			zap.L().Info("OIDC authentication enabled", zap.String("provider", cfg.Auth.OidcConfig.ProviderURL))
		}

		if cfg.Auth.JwtEnabled {
			jwtAuth, err := auth.NewJwtAuth(context.Background(), auth.JwtAuthConfig{
				Issuer:          cfg.Auth.JwtConfig.Issuer,
				Audiences:       cfg.Auth.JwtConfig.Audiences,
				JWKSURL:         cfg.Auth.JwtConfig.JWKSURL,
				SubjectClaim:    cfg.Auth.JwtConfig.SubjectClaim,
				AllowedSubjects: cfg.Auth.JwtConfig.AllowedSubjects,
				ClientIDClaim:   cfg.Auth.JwtConfig.ClientIDClaim,
				ScopesClaim:     cfg.Auth.JwtConfig.ScopesClaim,
			}, nil)
			if err != nil {
				zap.L().Error("Failed to initialize JWT authentication", zap.Error(err))
				os.Exit(1)
			}
			authenticators.AddAuthenticator(jwtAuth)
			zap.L().Info("JWT authentication enabled", zap.String("issuer", cfg.Auth.JwtConfig.Issuer))
		}

		if authenticators.IsEmpty() {
			zap.L().Error("No authentication provider configured but insecure mode is disabled. Please configure authentication or enable insecure mode.")
			os.Exit(1)
		}
	}

	if cfg.Auth.KubernetesConfig.Enabled {
		tokenReviewer, err := auth.NewInClusterKubernetesTokenReviewer()
		if err != nil {
			zap.L().Error("Failed to initialize Kubernetes TokenReview client", zap.Error(err))
			os.Exit(1)
		}
		kubernetesAuth, err := auth.NewKubernetesTokenReviewAuth(tokenReviewer, auth.KubernetesTokenReviewAuthConfig{
			Audience:         cfg.Auth.KubernetesConfig.Audience,
			ExpectedUsername: cfg.Auth.KubernetesConfig.ExpectedServiceAccount,
		})
		if err != nil {
			zap.L().Error("Failed to initialize Kubernetes TokenReview authentication", zap.Error(err))
			os.Exit(1)
		}
		internalAuthenticators = auth.NewAuth()
		internalAuthenticators.AddAuthenticator(kubernetesAuth)
		zap.L().Info("Kubernetes TokenReview authentication enabled for internal endpoints", zap.String("audience", cfg.Auth.KubernetesConfig.Audience), zap.String("expected_username", cfg.Auth.KubernetesConfig.ExpectedServiceAccount))
	}

	if publicAuthenticatorConfigurationInvalid(cfg.Server.EnableExternalEndpoints, cfg.Auth.Insecure, authenticators) {
		zap.L().Error("Authentication is not configured but external endpoints are enabled. Please configure authentication or enable insecure mode.")
		os.Exit(1)
	}
	zap.L().Info("Starting Heimdall API server...")

	if err := run(&gatewayConfig{
		enableExternalEndpoints: cfg.Server.EnableExternalEndpoints,
		publicEndpoint: endpointConfig{
			insecure:           cfg.Auth.Insecure,
			grpcServerEndpoint: cfg.Server.GRPCServerEndpoint,
			httpServerEndpoint: cfg.Server.HTTPServerEndpoint,
			urlPrefix:          cfg.Server.URLPrefix,
		},
		internalEndpoint: endpointConfig{
			insecure:           true,
			authenticate:       cfg.Auth.KubernetesConfig.Enabled,
			grpcServerEndpoint: cfg.Server.InternalGRPCServerEndpoint,
			httpServerEndpoint: cfg.Server.InternalHTTPServerEndpoint,
			urlPrefix:          cfg.Server.InternalURLPrefix,
		},
		auth:             authenticators,
		internalAuth:     internalAuthenticators,
		oidcRP:           oidcRP,
		loginRedirectURL: cfg.Auth.LoginRedirectURL,
		oidcConfig:       oidcConfig,
		jobSource:        jobSource,
		auditSource:      auditSource,
		corsAllowOrigin:  &cfg.Server.CorsAllowOrigin,
		otelProvider:     otelProvider,
		otelServiceName:  cfg.Otel.ServiceName,
		metricsEndpoint:  cfg.MetricsTelemetry.ServerEndpoint,
	}); err != nil {
		zap.L().Error("Server failed", zap.Error(err))
		os.Exit(1)
	}
}

func setDefaultSlogLogger(logger *zap.Logger) {
	slog.SetDefault(slog.New(slogzap.Option{Level: slog.LevelWarn, Logger: logger}.NewZapHandler()))
}
