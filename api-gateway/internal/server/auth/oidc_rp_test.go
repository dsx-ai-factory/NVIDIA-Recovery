// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package auth

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	httphelper "github.com/zitadel/oidc/v3/pkg/http"
)

// minimalDiscovery returns a discovery document that points to the provided base URL
// for authorize and token endpoints. This is enough for the RP to build the auth URL.
func minimalDiscovery(t *testing.T, issuer string) []byte {
	t.Helper()
	doc := map[string]any{
		"issuer":                                issuer,
		"authorization_endpoint":                issuer + "/authorize",
		"token_endpoint":                        issuer + "/token",
		"jwks_uri":                              issuer + "/keys",
		"scopes_supported":                      []string{"openid", "profile", "email"},
		"response_types_supported":              []string{"code"},
		"subject_types_supported":               []string{"public"},
		"id_token_signing_alg_values_supported": []string{"none"},
	}
	b, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal discovery: %v", err)
	}
	return b
}

func setupStubOP(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	srv := httptest.NewServer(mux)

	// discovery
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(minimalDiscovery(t, srv.URL))
	})

	// authorize: just echo back params; RP only needs redirect target for URL formation in our test
	mux.HandleFunc("/authorize", func(w http.ResponseWriter, r *http.Request) {
		// This handler won't be actually followed in the first test; we just need it to exist.
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	return srv
}

// func newTestOP() (op.OpenIDProvider, error) {
// 	// Setup a test OP server from /zitadel/oidc

// 	config := &op.Config{
// 		CryptoKey: key,

// 		// will be used if the end_session endpoint is called without a post_logout_redirect_uri
// 		DefaultLogoutRedirectURI: pathLoggedOut,

// 		// enables code_challenge_method S256 for PKCE (and therefore PKCE in general)
// 		CodeMethodS256: true,

// 		// enables additional client_id/client_secret authentication by form post (not only HTTP Basic Auth)
// 		AuthMethodPost: true,

// 		// enables additional authentication by using private_key_jwt
// 		AuthMethodPrivateKeyJWT: true,

// 		// enables refresh_token grant use
// 		GrantTypeRefreshToken: true,

// 		// enables use of the `request` Object parameter
// 		RequestObjectSupported: true,

// 		// this example has only static texts (in English), so we'll set the here accordingly
// 		SupportedUILocales: []language.Tag{language.English},

// 		DeviceAuthorization: op.DeviceAuthorizationConfig{
// 			Lifetime:     5 * time.Minute,
// 			PollInterval: 5 * time.Second,
// 			UserFormPath: "/device",
// 			UserCode:     op.UserCodeBase20,
// 		},
// 	}

// 	handler, err := op.NewProvider(
// 		config,
// 		op.NewMemoryStorage(),
// 		issuer,
// 	)
// 	if err != nil {
// 		return nil, err
// 	}

// 	return nil
// }

func TestOIDCLoginUI_LoginRedirectBuildsAuthorizeURL(t *testing.T) {
	op := setupStubOP(t)
	defer op.Close()

	// Build the UI with the OP discovery URL
	cfg := OidcAuthConfig{
		ProviderURL:  op.URL,
		ClientID:     "client",
		ClientSecret: "secret",
		Scopes:       []string{"openid", "profile", "email"},
	}

	// redirectURI must be absolute and point back to our service callback
	ui, err := NewOIDCRelyingParty(context.Background(), cfg, "http://example.com"+CallbackPath)
	if err != nil {
		t.Fatalf("NewOIDCLoginUI: %v", err)
	}

	mux := http.NewServeMux()
	ui.RegisterHandlers(mux)

	req := httptest.NewRequest(http.MethodGet, LoginPath+"?x=1", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusFound {
		t.Fatalf("expected 302, got %d", w.Code)
	}
	loc := w.Header().Get("Location")
	if loc == "" {
		t.Fatalf("missing redirect Location header")
	}
	// Verify the redirect points to OP's /authorize
	u, err := url.Parse(loc)
	if err != nil {
		t.Fatalf("parse redirect: %v", err)
	}
	if u.Scheme+"://"+u.Host != op.URL || u.Path != "/authorize" {
		t.Fatalf("unexpected redirect target: %s", loc)
	}
	// Has required params
	if u.Query().Get("client_id") == "" || u.Query().Get("redirect_uri") == "" || u.Query().Get("state") == "" {
		t.Fatalf("missing required OIDC params in redirect: %s", loc)
	}
}

func TestOIDCRelyingPartySessionMiddlewarePreservesExplicitAuthorization(t *testing.T) {
	cookieHandler := httphelper.NewCookieHandler(
		make([]byte, 32),
		make([]byte, 32),
		httphelper.WithUnsecure(),
	)
	ui := &OIDCRelyingParty{sessionCookieHandler: cookieHandler}

	cookieResponse := httptest.NewRecorder()
	if err := ui.addSessionToResponse(cookieResponse, Session{
		accessToken:  "session-access-token",
		idToken:      "session-id-token",
		refreshToken: "session-refresh-token",
	}); err != nil {
		t.Fatalf("add session cookies: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/jobs", nil)
	for _, cookie := range cookieResponse.Result().Cookies() {
		req.AddCookie(cookie)
	}
	req.Header.Set("Authorization", "Bearer explicit-jwt")

	var gotAuthorization string
	handler := ui.SessionMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuthorization = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusNoContent)
	}))
	handler.ServeHTTP(httptest.NewRecorder(), req)

	if gotAuthorization != "Bearer explicit-jwt" {
		t.Fatalf("expected explicit Authorization header to be preserved, got %q", gotAuthorization)
	}
}

// func TestOIDCLoginUI_CodeExchangeCallbackSetsCookiesAndRedirects(t *testing.T) {

// }
