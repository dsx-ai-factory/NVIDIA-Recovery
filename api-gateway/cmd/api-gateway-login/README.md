# API Gateway OIDC Login CLI

This CLI tool performs OIDC authentication and outputs a JWT token that can be used with the API Gateway's auth middleware.

## Usage

### Command Line Flags

```bash
./api-gateway-login \
  --provider-url "https://your-oidc-provider.com" \
  --client-id "your-client-id" \
  --client-secret "your-client-secret"
```

### Environment Variables

Alternatively, you can use environment variables:

```bash
export HEIMDALL_OIDC_PROVIDER_URL="https://your-oidc-provider.com"
export HEIMDALL_OIDC_CLIENT_ID="your-client-id"
export HEIMDALL_OIDC_CLIENT_SECRET="your-client-secret"
./api-gateway-login
```

### Mixed Configuration

Command line flags take precedence over environment variables:

```bash
export HEIMDALL_OIDC_PROVIDER_URL="https://your-oidc-provider.com"
export HEIMDALL_OIDC_CLIENT_ID="your-client-id"
./api-gateway-login --client-secret "override-secret"
```

## Options

| Flag | Environment Variable | Default | Description |
|------|---------------------|---------|-------------|
| `--provider-url` | `HEIMDALL_OIDC_PROVIDER_URL` | - | OIDC provider URL (required) |
| `--client-id` | `HEIMDALL_OIDC_CLIENT_ID` | - | OIDC client ID (required) |
| `--client-secret` | `HEIMDALL_OIDC_CLIENT_SECRET` | - | OIDC client secret (required) |
| `--redirect-url` | `HEIMDALL_OIDC_REDIRECT_URL` | `http://localhost:8080/callback` | Redirect URL for OAuth2 flow |
| `--scopes` | `HEIMDALL_OIDC_SCOPES` | `openid,profile,email` | OAuth2 scopes to request |
| `--port` | `HEIMDALL_OIDC_PORT` | `8080` | Port for local callback server |

## Authentication Flow

1. The tool starts a local HTTP server for the OAuth2 callback
2. Opens your browser to the OIDC provider's authorization URL
3. After you authenticate, the provider redirects back to the local server
4. The tool exchanges the authorization code for tokens
5. Verifies the ID token and outputs it to stdout

## Integration with Auth Middleware

The output JWT token can be used as a Bearer token with the API Gateway's OIDC auth middleware in `internal/server/auth.go`:

```bash
# Get token using command line flags
TOKEN=$(./api-gateway-login --provider-url "..." --client-id "..." --client-secret "...")

# Or get token using environment variables
export HEIMDALL_OIDC_PROVIDER_URL="..."
export HEIMDALL_OIDC_CLIENT_ID="..."
export HEIMDALL_OIDC_CLIENT_SECRET="..."
TOKEN=$(./api-gateway-login)

# Use with API Gateway
curl -H "Authorization: Bearer $TOKEN" https://your-api-gateway.com/api/endpoint
```

The auth middleware will validate the token using the same OIDC provider configuration.
