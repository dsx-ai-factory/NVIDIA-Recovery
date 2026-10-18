// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main_test

import (
	"os"
	"strings"
	"testing"
)

func TestApiGatewayContractDoesNotExposeMCP(t *testing.T) {
	for _, path := range []string{
		"../protos/heimdall/v1/heimdall.proto",
		"../gen/openapiv2/heimdall/v1/heimdall.swagger.yaml",
	} {
		contents, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}

		text := string(contents)
		for _, removedSymbol := range []string{
			"/v1/" + "mcp",
			"Get" + "Mcp" + "Servers",
			"MCP" + "Server",
		} {
			if strings.Contains(text, removedSymbol) {
				t.Fatalf("%s still contains removed MCP gateway symbol %q", path, removedSymbol)
			}
		}
	}
}
