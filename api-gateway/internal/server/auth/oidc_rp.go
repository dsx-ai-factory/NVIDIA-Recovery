// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package auth

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/zitadel/logging"
	"github.com/zitadel/oidc/v3/pkg/client/rp"
	httphelper "github.com/zitadel/oidc/v3/pkg/http"
	"github.com/zitadel/oidc/v3/pkg/oidc"
	"go.uber.org/zap"
)

const (
	// Public paths to expose for the UI login flow
	LoginPath    = "/v1/auth/login"
	LogoutPath   = "/v1/auth/logout"
	RefreshPath  = "/v1/auth/refresh"
	CallbackPath = "/v1/auth/callback"
)

type OidcAuthConfig struct {
	ProviderURL   string
	ClientID      string
	ClientSecret  string
	Scopes        []string
	CookieKey     []byte
	SecureCookie  bool
	CookieDomain  string
	SubjectClaim  string
	ClientIDClaim string
	ScopesClaim   string
}

func (c *OidcAuthConfig) Validate() error {
	if c.ProviderURL == "" || c.ClientID == "" {
		return fmt.Errorf("OIDC configuration incomplete. Please set OIDC_PROVIDER_URL and OIDC_CLIENT_ID")
	}

	return nil
}

// OIDCLoginUI encapsulates handlers for starting and completing an OIDC login flow.
type OIDCRelyingParty struct {
	relyingParty         rp.RelyingParty
	sessionCookieHandler *httphelper.CookieHandler
	identityClaims       IdentityClaimConfig
}

type stateData struct {
	RedirectURI string `json:"redirectURI"`
}

type Session struct {
	accessToken  string
	idToken      string
	refreshToken string
}

type tokenResponse struct {
	BearerToken string `json:"bearerToken"`
	IDToken     string `json:"idToken"`
	ExpiresAt   int    `json:"expiresAt"`
}

// NewOIDCRelyingParty constructs an OIDC relying party suitable for driving a browser login flow.
// redirectURI should be the full external URL to this service's callback endpoint.
func NewOIDCRelyingParty(ctx context.Context, cfg OidcAuthConfig, redirectURI string) (*OIDCRelyingParty, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	var key []byte
	if len(cfg.CookieKey) == 0 {
		zap.L().Warn("no cookie key provided, generating ephemeral key")
		// Stateless ephemeral key material used for state/nonce cookies in dev. In production, consider
		// providing stable keys via configuration if persistence across restarts is required.
		key = make([]byte, 32)
		if _, err := rand.Read(key); err != nil {
			return nil, fmt.Errorf("failed generating cookie keys: %w", err)
		}
	} else {
		decodedKey, err := base64.StdEncoding.DecodeString(string(cfg.CookieKey))
		if err != nil {
			return nil, fmt.Errorf("failed decoding cookie key: %w", err)
		}

		if len(decodedKey) != 32 {
			return nil, fmt.Errorf("cookie key must be 32 bytes")
		}

		key = decodedKey
	}

	cookieOpts := []httphelper.CookieHandlerOpt{}

	if !cfg.SecureCookie {
		cookieOpts = append(cookieOpts, httphelper.WithUnsecure())
	}

	if cfg.CookieDomain != "" {
		cookieOpts = append(cookieOpts, httphelper.WithDomain(cfg.CookieDomain))
	}

	cookieHandler := httphelper.NewCookieHandler(key, key, cookieOpts...)

	httpClient := &http.Client{Timeout: time.Minute}
	logging.EnableHTTPClient(httpClient, logging.WithClientGroup("oidc-client"))

	scopes := cfg.Scopes
	if len(scopes) == 0 {
		scopes = []string{"openid", "profile", "email"}
	}

	options := []rp.Option{
		rp.WithCookieHandler(cookieHandler),
		rp.WithVerifierOpts(rp.WithIssuedAtOffset(5 * time.Second)),
		rp.WithHTTPClient(httpClient),
		rp.WithLogger(slog.Default()),
		rp.WithSigningAlgsFromDiscovery(),
	}

	rpClient, err := rp.NewRelyingPartyOIDC(ctx, cfg.ProviderURL, cfg.ClientID, cfg.ClientSecret, redirectURI, scopes, options...)
	if err != nil {
		return nil, fmt.Errorf("failed to create OIDC relying party: %w", err)
	}

	return &OIDCRelyingParty{
		relyingParty:         rpClient,
		sessionCookieHandler: cookieHandler,
		identityClaims: IdentityClaimConfig{
			SubjectClaim:  cfg.SubjectClaim,
			ClientIDClaim: cfg.ClientIDClaim,
			ScopesClaim:   cfg.ScopesClaim,
		},
	}, nil
}

func (ui *OIDCRelyingParty) marshalState(state stateData) (string, error) {
	json, err := json.Marshal(state)
	if err != nil {
		return "", err
	}
	return string(json), nil
}

func (ui *OIDCRelyingParty) unmarshalState(state string) (stateData, error) {
	var s stateData
	err := json.Unmarshal([]byte(state), &s)
	if err != nil {
		return stateData{}, err
	}
	return s, nil
}

func (ui *OIDCRelyingParty) sessionFromRequest(r *http.Request) (Session, error) {
	accessToken, err := ui.sessionCookieHandler.CheckCookie(r, "accessToken")
	if err != nil {
		return Session{}, err
	}

	refreshToken, err := ui.sessionCookieHandler.CheckCookie(r, "refreshToken")
	if err != nil {
		return Session{}, err
	}

	idToken, err := ui.sessionCookieHandler.CheckCookie(r, "idToken")
	if err != nil {
		return Session{}, err
	}

	return Session{
		accessToken:  accessToken,
		refreshToken: refreshToken,
		idToken:      idToken,
	}, nil
}

func (ui *OIDCRelyingParty) addSessionToResponse(w http.ResponseWriter, session Session) error {
	err := ui.sessionCookieHandler.SetCookie(w, "accessToken", session.accessToken)
	if err != nil {
		return err
	}
	err = ui.sessionCookieHandler.SetCookie(w, "refreshToken", session.refreshToken)
	if err != nil {
		return err
	}
	err = ui.sessionCookieHandler.SetCookie(w, "idToken", session.idToken)
	if err != nil {
		return err
	}
	return nil
}

func (ui *OIDCRelyingParty) authHttpHandler(w http.ResponseWriter, r *http.Request) {
	stateFunc := func() string {
		redirectURI := r.URL.Query().Get("redirectURI")
		if redirectURI == "" {
			redirectURI = "/"
		}

		state, err := ui.marshalState(stateData{RedirectURI: redirectURI})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return ""
		}
		return state
	}

	// Optional URL params can be extended here (e.g., response_mode, prompt)
	var urlOptions []rp.URLParamOpt

	handler := rp.AuthURLHandler(stateFunc, ui.relyingParty, urlOptions...)
	handler.ServeHTTP(w, r)
}

func (ui *OIDCRelyingParty) refreshHttpHandler(w http.ResponseWriter, r *http.Request) {
	session, err := ui.sessionFromRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	_, err = rp.VerifyTokens[*oidc.IDTokenClaims](r.Context(), session.accessToken, session.idToken, ui.relyingParty.IDTokenVerifier())
	if err != nil {
		zap.L().Warn("failed to verify tokens", zap.Error(err))
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	tokens, err := rp.RefreshTokens[*oidc.IDTokenClaims](r.Context(), ui.relyingParty, session.refreshToken, "", "") //clientAssertionJwt, "urn:ietf:params:oauth:client-assertion-type:jwt-bearer")
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	err = ui.addSessionToResponse(w, Session{
		accessToken:  tokens.AccessToken,
		idToken:      tokens.IDToken,
		refreshToken: tokens.RefreshToken,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	expiresAtIdToken := int64(tokens.IDTokenClaims.Expiration)
	expiresAtAccessToken := int64(tokens.Expiry.Unix())
	expiresAt := expiresAtIdToken

	if expiresAtAccessToken != 0 && expiresAtAccessToken < expiresAtIdToken {
		expiresAt = expiresAtAccessToken
	}

	err = json.NewEncoder(w).Encode(tokenResponse{
		// TODO: Switch to access tokens to for verifying user actions once we are ready for authz
		BearerToken: tokens.IDToken,
		IDToken:     tokens.IDToken,
		ExpiresAt:   int(expiresAt),
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (ui *OIDCRelyingParty) logoutHttpHandler(w http.ResponseWriter, r *http.Request) {
	session, err := ui.sessionFromRequest(r)
	if err != nil {
		zap.L().Warn("failed to get session from request", zap.Error(err))
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	redirectUrl, err := rp.EndSession(r.Context(), ui.relyingParty, session.idToken, "", "", "", nil)

	if err != nil {
		zap.L().Warn("failed to end session", zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// TODO: Delete this in a logout callback handler
	ui.sessionCookieHandler.DeleteCookie(w, "accessToken")
	ui.sessionCookieHandler.DeleteCookie(w, "refreshToken")
	ui.sessionCookieHandler.DeleteCookie(w, "idToken")

	http.Redirect(w, r, redirectUrl.String(), http.StatusFound)
}

// Set session cookies based on auth callback
func (ui *OIDCRelyingParty) codeExchangeCallback(w http.ResponseWriter, r *http.Request, tokens *oidc.Tokens[*oidc.IDTokenClaims], state string, rp rp.RelyingParty) {
	session := Session{
		accessToken:  tokens.AccessToken,
		idToken:      tokens.IDToken,
		refreshToken: tokens.RefreshToken,
	}

	stateData, err := ui.unmarshalState(state)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	redirectURI := stateData.RedirectURI

	err = ui.addSessionToResponse(w, session)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	zap.L().Info("redirecting to", zap.String("redirectURI", redirectURI))

	http.Redirect(w, r, redirectURI, http.StatusFound)
}

func (ui *OIDCRelyingParty) GetAuthenticator() *OidcAuth {
	return NewOidcAuth(ui.relyingParty.IDTokenVerifier(), ui.identityClaims)
}

// RegisterHandlers attaches the /auth/login and /auth/callback handlers to the provided mux.
func (ui *OIDCRelyingParty) RegisterHandlers(mux *http.ServeMux) {
	mux.Handle(LoginPath, http.HandlerFunc(ui.authHttpHandler))
	mux.Handle(RefreshPath, http.HandlerFunc(ui.refreshHttpHandler))
	mux.Handle(LogoutPath, http.HandlerFunc(ui.logoutHttpHandler))
	mux.Handle(CallbackPath, rp.CodeExchangeHandler(ui.codeExchangeCallback, ui.relyingParty))
}

func (ui *OIDCRelyingParty) SessionMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		session, err := ui.sessionFromRequest(r)
		if err != nil {
			// User has no cookies set; this may be fine if they have a Bearer token.
			zap.L().Debug("Session cookie not found", zap.Error(err))
		}

		if session.idToken != "" && r.Header.Get("Authorization") == "" {
			r.Header.Set("Authorization", fmt.Sprintf("Bearer %s", session.idToken))
		}

		next.ServeHTTP(w, r)
	})
}
