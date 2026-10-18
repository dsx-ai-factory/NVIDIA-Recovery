// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Package audit calls the downstream audit API.
package audit

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/cockroachdb/errors"
	notificationsbindings "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/notifications"
)

const (
	maxResponseBodySize     int64 = 4096
	maxReadResponseBodySize int64 = 4 << 20
)

// ErrResponseTooLarge reports a response exceeding the operation-specific memory bound.
var ErrResponseTooLarge = errors.New("downstream audit response exceeds size limit")

// Client calls the downstream audit service over HTTP.
type Client struct {
	auditAPI     *notificationsbindings.AuditAPIService
	auditReadAPI *notificationsbindings.AuditAPIService
}

// HTTPError reports a non-success response from the downstream audit service.
type HTTPError struct {
	// StatusCode is the downstream HTTP response code.
	StatusCode int
	// Status is the downstream HTTP status text.
	Status string
}

// Error implements error without exposing a downstream response body.
func (e *HTTPError) Error() string {
	return "downstream audit returned HTTP status " + e.Status
}

// NewClient constructs a downstream audit client from a base URL.
func NewClient(endpoint string, httpClient *http.Client) (*Client, error) {
	target, err := url.Parse(endpoint)
	if err != nil {
		return nil, errors.Wrap(err, "parse audit endpoint")
	}
	if target.Scheme != "http" && target.Scheme != "https" {
		return nil, errors.New("audit endpoint must use http or https")
	}
	if target.Host == "" {
		return nil, errors.New("audit endpoint must include a host")
	}
	if target.User != nil {
		return nil, errors.New("audit endpoint must not include credentials")
	}
	if target.RawQuery != "" || target.Fragment != "" {
		return nil, errors.New("audit endpoint must not include a query or fragment")
	}
	if httpClient == nil {
		return nil, errors.New("audit HTTP client must be provided")
	}

	transport := httpClient.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}
	// Bind the limit to the operation so redirects cannot turn an ingestion
	// response into a read response with the larger memory allowance.
	newAPI := func(limit int64) *notificationsbindings.AuditAPIService {
		generatedHTTPClient := *httpClient
		generatedHTTPClient.Transport = responseLimitingRoundTripper{base: transport, limit: limit}
		config := notificationsbindings.NewConfiguration()
		config.Servers = notificationsbindings.ServerConfigurations{{URL: strings.TrimRight(target.String(), "/")}}
		config.HTTPClient = &generatedHTTPClient
		return notificationsbindings.NewAPIClient(config).AuditAPI
	}
	return &Client{auditAPI: newAPI(maxResponseBodySize), auditReadAPI: newAPI(maxReadResponseBodySize)}, nil
}

// CreateAuditEvents submits one producer batch to the downstream audit service.
func (c *Client) CreateAuditEvents(ctx context.Context, request notificationsbindings.ProducerAuditEventBatch) (*notificationsbindings.AuditEventIngestionResponse, error) {
	result, response, err := c.auditAPI.
		AuditEventsCreate(ctx).
		ProducerAuditEventBatch(request).
		Execute()
	if response != nil && response.StatusCode != http.StatusAccepted {
		return nil, &HTTPError{StatusCode: response.StatusCode, Status: response.Status}
	}
	if err != nil {
		if errors.Is(err, ErrResponseTooLarge) {
			return nil, ErrResponseTooLarge
		}
		return nil, errors.Wrap(err, "call downstream audit ingestion")
	}
	if result == nil {
		return nil, errors.New("downstream audit ingestion returned no response body")
	}
	return result, nil
}

// GetAuditEvents executes a generated read request through this client's bounded transport.
func (c *Client) GetAuditEvents(ctx context.Context, configure func(notificationsbindings.ApiAuditEventsGetRequest) notificationsbindings.ApiAuditEventsGetRequest) (*notificationsbindings.AuditEventPage, error) {
	result, response, err := configure(c.auditReadAPI.AuditEventsGet(ctx)).Execute()
	if errors.Is(err, ErrResponseTooLarge) {
		return nil, ErrResponseTooLarge
	}
	if response != nil && response.StatusCode != http.StatusOK {
		return nil, &HTTPError{StatusCode: response.StatusCode, Status: response.Status}
	}
	if err != nil {
		return nil, errors.Wrap(err, "query downstream audit events")
	}
	if result == nil {
		return nil, errors.New("downstream audit query returned no response body")
	}
	return result, nil
}

type responseLimitingRoundTripper struct {
	base  http.RoundTripper
	limit int64
}

func (transport responseLimitingRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	response, err := transport.base.RoundTrip(request)
	if response != nil && response.Body != nil {
		response.Body = &limitedResponseBody{body: response.Body, remaining: transport.limit}
	}
	return response, err
}

type limitedResponseBody struct {
	body      io.ReadCloser
	remaining int64
}

// Read bounds memory consumed by the generated client, which reads each
// response body in full before decoding it.
func (body *limitedResponseBody) Read(buffer []byte) (int, error) {
	if body.remaining == 0 {
		var extra [1]byte
		count, err := body.body.Read(extra[:])
		if count > 0 {
			return 0, ErrResponseTooLarge
		}
		return 0, err
	}
	if int64(len(buffer)) > body.remaining {
		buffer = buffer[:body.remaining]
	}
	count, err := body.body.Read(buffer)
	body.remaining -= int64(count)
	return count, err
}

func (body *limitedResponseBody) Close() error {
	return body.body.Close()
}
