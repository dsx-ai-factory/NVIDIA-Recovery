// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"io"

	pb "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/go/heimdall/v1"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/protobuf/encoding/protojson"
)

// jobUpdateJSON preserves the gateway's encoding and decoding defaults except
// for job updates. DiscardUnknown would silently turn an invalid enum name into
// an omitted state before the gRPC service has a chance to validate it.
type jobUpdateJSON struct {
	runtime.Marshaler
	strict runtime.JSONPb
}

// Unmarshal rejects unknown fields and enum names in job-update requests.
func (m *jobUpdateJSON) Unmarshal(data []byte, value any) error {
	if _, ok := value.(*pb.CreateOrUpdateJobRequest); ok {
		return m.strict.Unmarshal(data, value)
	}
	return m.Marshaler.Unmarshal(data, value)
}

// NewDecoder applies the same message-specific policy to HTTP request bodies.
func (m *jobUpdateJSON) NewDecoder(reader io.Reader) runtime.Decoder {
	strict := m.strict.NewDecoder(reader)
	defaultDecoder := m.Marshaler.NewDecoder(reader)
	return runtime.DecoderFunc(func(value any) error {
		if _, ok := value.(*pb.CreateOrUpdateJobRequest); ok {
			return strict.Decode(value)
		}
		return defaultDecoder.Decode(value)
	})
}

// newGatewayMux installs the same job-update decoding policy on both listeners.
func newGatewayMux(options ...runtime.ServeMuxOption) *runtime.ServeMux {
	marshaler := &jobUpdateJSON{
		Marshaler: &runtime.HTTPBodyMarshaler{Marshaler: &runtime.JSONPb{
			MarshalOptions:   protojson.MarshalOptions{EmitUnpopulated: true},
			UnmarshalOptions: protojson.UnmarshalOptions{DiscardUnknown: true},
		}},
	}
	options = append(options, runtime.WithMarshalerOption(runtime.MIMEWildcard, marshaler))
	return runtime.NewServeMux(options...)
}
