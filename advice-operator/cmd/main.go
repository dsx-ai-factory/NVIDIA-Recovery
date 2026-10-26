// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"crypto/tls"
	"flag"
	"os"
	"strings"

	"github.com/go-logr/zapr"
	"go.uber.org/zap"

	// Import all Kubernetes client auth plugins so out-of-cluster clients can use them.
	_ "k8s.io/client-go/plugin/pkg/client/auth"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	ctrlzap "sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/metrics/filters"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	heimdallv1alpha1 "github.com/dsx-ai-factory/nvidia-recovery/advice-operator/api/v1alpha1"
	"github.com/dsx-ai-factory/nvidia-recovery/advice-operator/internal/cache"
	heimdallclient "github.com/dsx-ai-factory/nvidia-recovery/advice-operator/internal/client"
	"github.com/dsx-ai-factory/nvidia-recovery/advice-operator/internal/config"
	"github.com/dsx-ai-factory/nvidia-recovery/advice-operator/internal/controller"
	"github.com/dsx-ai-factory/nvidia-recovery/advice-operator/internal/syncer"
	// +kubebuilder:scaffold:imports
)

const (
	// defaultEventChannelSize is the buffer size for the event channel between syncer and reconciler.
	// A larger buffer reduces the chance of blocking on event emission during sync.
	defaultEventChannelSize = 1024
)

var (
	scheme   = runtime.NewScheme()
	setupLog = zap.NewNop().Named("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))

	utilruntime.Must(heimdallv1alpha1.AddToScheme(scheme))
	// +kubebuilder:scaffold:scheme
}

// nolint:gocyclo
func main() {
	var metricsAddr string
	var metricsCertPath, metricsCertName, metricsCertKey string
	var webhookCertPath, webhookCertName, webhookCertKey string
	var enableLeaderElection bool
	var probeAddr string
	var secureMetrics bool
	var enableHTTP2 bool
	var adviceNamespaces string
	var tlsOpts []func(*tls.Config)
	flag.StringVar(&metricsAddr, "metrics-bind-address", "0", "The address the metrics endpoint binds to. "+
		"Use :8443 for HTTPS or :8080 for HTTP, or leave as 0 to disable the metrics service.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", false,
		"Enable leader election for controller manager. "+
			"Enabling this will ensure there is only one active controller manager.")
	flag.BoolVar(&secureMetrics, "metrics-secure", true,
		"If set, the metrics endpoint is served securely via HTTPS. Use --metrics-secure=false to use HTTP instead.")
	flag.StringVar(&webhookCertPath, "webhook-cert-path", "", "The directory that contains the webhook certificate.")
	flag.StringVar(&webhookCertName, "webhook-cert-name", "tls.crt", "The name of the webhook certificate file.")
	flag.StringVar(&webhookCertKey, "webhook-cert-key", "tls.key", "The name of the webhook key file.")
	flag.StringVar(&metricsCertPath, "metrics-cert-path", "",
		"The directory that contains the metrics server certificate.")
	flag.StringVar(&metricsCertName, "metrics-cert-name", "tls.crt", "The name of the metrics server certificate file.")
	flag.StringVar(&metricsCertKey, "metrics-cert-key", "tls.key", "The name of the metrics server key file.")
	flag.BoolVar(&enableHTTP2, "enable-http2", false,
		"If set, HTTP/2 will be enabled for the metrics and webhook servers")
	flag.StringVar(&adviceNamespaces, "advice-namespaces", "*", "Comma-separated namespaces to sync, or '*' for all")
	opts := ctrlzap.Options{
		Development: true,
	}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	logger := ctrlzap.NewRaw(ctrlzap.UseFlagOptions(&opts))
	ctrl.SetLogger(zapr.NewLogger(logger))
	setupLog = logger.Named("setup")

	cfg, err := config.Load()
	if err != nil {
		setupLog.Error("unable to load configuration", zap.Error(err))
		os.Exit(1)
	}

	// Parse namespaces: "*" means all (empty slice), otherwise split by comma
	var namespacesToWatch []string
	if adviceNamespaces != "*" && adviceNamespaces != "" {
		namespacesToWatch = strings.Split(adviceNamespaces, ",")
		// Trim whitespace from each namespace
		for i := range namespacesToWatch {
			namespacesToWatch[i] = strings.TrimSpace(namespacesToWatch[i])
		}
	}
	// Empty slice means watch all namespaces

	setupLog.Info("loaded configuration",
		zap.String("apiURL", cfg.HeimdallAPIURL),
		zap.String("apiProtocol", cfg.HeimdallAPIProtocol),
		zap.Duration("pollInterval", cfg.PollInterval),
		zap.String("adviceNamespaces", adviceNamespaces))

	// Create Heimdall API client
	ctx := ctrl.SetupSignalHandler()
	useTLS := strings.HasPrefix(cfg.HeimdallAPIURL, "https://")
	setupLog.Info("using gRPC client",
		zap.String("address", cfg.HeimdallAPIURL),
		zap.String("protocol", cfg.HeimdallAPIProtocol),
		zap.Bool("tls", useTLS),
		zap.Bool("insecureSkipVerify", cfg.HeimdallAPIInsecureSkipVerify))
	heimdallClient, err := heimdallclient.NewHeimdallGRPCClient(ctx, heimdallclient.GRPCClientConfig{
		Address:            cfg.HeimdallAPIURL,
		UseTLS:             useTLS,
		InsecureSkipVerify: cfg.HeimdallAPIInsecureSkipVerify,
		Logger:             logger,
	})
	if err != nil {
		setupLog.Error("unable to create gRPC client", zap.Error(err))
		os.Exit(1)
	}

	// if the enable-http2 flag is false (the default), http/2 should be disabled
	// due to its vulnerabilities. More specifically, disabling http/2 will
	// prevent from being vulnerable to the HTTP/2 Stream Cancellation and
	// Rapid Reset CVEs. For more information see:
	// - https://github.com/advisories/GHSA-qppj-fm5r-hxr3
	// - https://github.com/advisories/GHSA-4374-p667-p6c8
	disableHTTP2 := func(c *tls.Config) {
		setupLog.Info("disabling HTTP/2")
		c.NextProtos = []string{"http/1.1"}
	}

	if !enableHTTP2 {
		tlsOpts = append(tlsOpts, disableHTTP2)
	}

	// Initial webhook TLS options
	webhookTLSOpts := tlsOpts
	webhookServerOptions := webhook.Options{
		TLSOpts: webhookTLSOpts,
	}

	if len(webhookCertPath) > 0 {
		setupLog.Info("initializing webhook certificate watcher using provided certificates",
			zap.String("webhookCertPath", webhookCertPath),
			zap.String("webhookCertName", webhookCertName),
			zap.String("webhookCertKey", webhookCertKey))

		webhookServerOptions.CertDir = webhookCertPath
		webhookServerOptions.CertName = webhookCertName
		webhookServerOptions.KeyName = webhookCertKey
	}

	webhookServer := webhook.NewServer(webhookServerOptions)

	// Metrics endpoint is enabled in 'config/default/kustomization.yaml'. The Metrics options configure the server.
	// More info:
	// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.23.3/pkg/metrics/server
	// - https://book.kubebuilder.io/reference/metrics.html
	metricsServerOptions := metricsserver.Options{
		BindAddress:   metricsAddr,
		SecureServing: secureMetrics,
		TLSOpts:       tlsOpts,
	}

	if secureMetrics {
		// FilterProvider is used to protect the metrics endpoint with authn/authz.
		// These configurations ensure that only authorized users and service accounts
		// can access the metrics endpoint. The RBAC are configured in 'config/rbac/kustomization.yaml'. More info:
		// https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.23.3/pkg/metrics/filters#WithAuthenticationAndAuthorization
		metricsServerOptions.FilterProvider = filters.WithAuthenticationAndAuthorization
	}

	// If the certificate is not specified, controller-runtime will automatically
	// generate self-signed certificates for the metrics server. While convenient for development and testing,
	// this setup is not recommended for production.
	//
	// TODO(user): If you enable certManager, uncomment the following lines:
	// - [METRICS-WITH-CERTS] at config/default/kustomization.yaml to generate and use certificates
	// managed by cert-manager for the metrics server.
	// - [PROMETHEUS-WITH-CERTS] at config/prometheus/kustomization.yaml for TLS certification.
	if len(metricsCertPath) > 0 {
		setupLog.Info("initializing metrics certificate watcher using provided certificates",
			zap.String("metricsCertPath", metricsCertPath),
			zap.String("metricsCertName", metricsCertName),
			zap.String("metricsCertKey", metricsCertKey))

		metricsServerOptions.CertDir = metricsCertPath
		metricsServerOptions.CertName = metricsCertName
		metricsServerOptions.KeyName = metricsCertKey
	}

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsServerOptions,
		WebhookServer:          webhookServer,
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       "ed19b002.nvidia.com",
		// LeaderElectionReleaseOnCancel defines if the leader should step down voluntarily
		// when the Manager ends. This requires the binary to immediately end when the
		// Manager is stopped, otherwise, this setting is unsafe. Setting this significantly
		// speeds up voluntary leader transitions as the new leader don't have to wait
		// LeaseDuration time first.
		//
		// In the default scaffold provided, the program ends immediately after
		// the manager stops, so would be fine to enable this option. However,
		// if you are doing or is intended to do any operation such as perform cleanups
		// after the manager stops then its usage might be unsafe.
		// LeaderElectionReleaseOnCancel: true,
	})
	if err != nil {
		setupLog.Error("unable to start manager", zap.Error(err))
		os.Exit(1)
	}

	// Create shared components for syncer and reconciler
	adviceCache := cache.NewAdviceCache()
	eventChannel := make(chan event.GenericEvent, defaultEventChannelSize)

	// Set up the advice controller with the event channel
	adviceReconciler := &controller.AdviceReconciler{
		Client:    mgr.GetClient(),
		Scheme:    mgr.GetScheme(),
		APIReader: mgr.GetAPIReader(),
		Cache:     adviceCache,
		Logger:    logger,
	}
	if err := adviceReconciler.SetupWithManager(mgr, eventChannel); err != nil {
		setupLog.Error("unable to create controller", zap.String("controller", "Advice"), zap.Error(err))
		os.Exit(1)
	}

	// Add the advice syncer runnable that polls the API and emits events
	adviceSyncer := &syncer.AdviceSyncer{
		K8sClient:      mgr.GetClient(),
		HeimdallClient: heimdallClient,
		PollInterval:   cfg.PollInterval,
		Namespaces:     namespacesToWatch,
		Cache:          adviceCache,
		Events:         eventChannel,
		Logger:         logger,
	}
	if err := mgr.Add(adviceSyncer); err != nil {
		setupLog.Error("unable to add advice syncer", zap.Error(err))
		os.Exit(1)
	}

	setupLog.Info("configured advice synchronization",
		zap.Duration("pollInterval", cfg.PollInterval),
		zap.String("adviceNamespaces", adviceNamespaces),
		zap.String("apiURL", cfg.HeimdallAPIURL),
		zap.Int("eventChannelSize", defaultEventChannelSize))

	// +kubebuilder:scaffold:builder

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error("unable to set up health check", zap.Error(err))
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error("unable to set up ready check", zap.Error(err))
		os.Exit(1)
	}

	setupLog.Info("starting manager")
	if err := mgr.Start(ctx); err != nil {
		setupLog.Error("problem running manager", zap.Error(err))
		os.Exit(1)
	}
}
