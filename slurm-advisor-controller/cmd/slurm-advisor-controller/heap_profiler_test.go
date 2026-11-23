// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"compress/gzip"
	"io"
	"net/http"
	"testing"
	"time"

	"go.uber.org/zap"
)

func TestHeapProfilerServesHeapOnly(t *testing.T) {
	server, err := startHeapProfiler(0, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close() })

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get("http://" + server.Addr + "/debug/pprof/heap")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("heap endpoint status = %s, want 200", resp.Status)
	}
	profile, err := gzip.NewReader(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	defer profile.Close()
	n, err := io.Copy(io.Discard, profile)
	if err != nil || n == 0 {
		t.Fatalf("heap profile decompressed to %d bytes, error = %v", n, err)
	}

	resp, err = client.Get("http://" + server.Addr + "/debug/pprof/goroutine")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("goroutine endpoint status = %s, want 404", resp.Status)
	}
}
