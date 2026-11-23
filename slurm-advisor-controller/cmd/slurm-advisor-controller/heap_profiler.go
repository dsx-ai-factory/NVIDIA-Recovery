// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"net"
	"net/http"
	"net/http/pprof"
	"strconv"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"
)

// startHeapProfiler binds before returning so the caller can report startup
// failures. The caller owns closing the server when the process exits.
func startHeapProfiler(port int, logger *zap.Logger) (*http.Server, error) {
	listener, err := net.Listen("tcp4", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
	if err != nil {
		return nil, err
	}
	mux := http.NewServeMux()
	mux.Handle("GET /debug/pprof/heap", pprof.Handler("heap"))
	server := &http.Server{
		Addr:              listener.Addr().String(),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	logger.Info("heap profiler listening", zap.String("address", server.Addr))
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("heap profiler stopped unexpectedly", zap.Error(err))
		}
	}()
	return server, nil
}
