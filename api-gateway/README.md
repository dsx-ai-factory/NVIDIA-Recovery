# API Gateway

This directory contains the API Gateway for NVIDIA Recovery, which provides authentication, authorization, and routing for the various services.

## Getting Started

### Environment Configuration

1. Copy the example environment file and configure your local environment:

```bash
cp .env.example .env
```

2. Edit `.env` with your actual configuration values. The file contains all necessary environment variables with placeholder values that need to be replaced with your actual credentials and configuration.

### Required Configuration

The API Gateway requires configuration for various services. Refer to `.env.example` for a complete list of environment variables.

## Generating the API

1) install [buf CLI](https://buf.build/docs/installation/)

```bash
brew install bufbuild/buf/buf
```

2) update deps in buf.yaml

```bash
buf dep update
```

3) generate the API

```bash
buf generate
```

### Notes:

We are using the [buf remote plugin](https://github.com/grpc-ecosystem/grpc-gateway?tab=readme-ov-file#usage-with-remote-plugins) for generation of proto files in order to avoid having to install the protobuf definitions / plugins in the host machine.

This requires no local installation of any plugins. Be careful to use the same version of the generator as the runtime library, i.e. if using v2.16.2, run

```bash
$ go get github.com/grpc-ecosystem/grpc-gateway/v2@v2.16.2
```
To get the same version of the runtime in your go.mod.

### Generating downstream Go clients

The Job Manager and Notifications Go HTTP clients are generated from their
service-owned OpenAPI documents with the repository-pinned Docker generator:

```bash
make generate-api-gateway-bindings
make check-generated-api-gateway-bindings
```

Do not edit files under `gen/jobmanager` or `gen/notifications` by hand.


## Authentication

The API Gateway supports a chain of generic authentication providers that are checked in order. At least one authenticator must succeed for a request to be considered valid.

For development simply copy .env.dev.example to .env and fill in any missing values.

#### Key Environment Variables

| Variable                         | Description |
|----------------------------------|-------------|
| `HEIMDALL_INSECURE`              | Disable authentication. Use only for local development. |
| `HEIMDALL_OIDC_ENABLED`          | Enable browser and bearer-token OIDC authentication. |
| `HEIMDALL_OIDC_PROVIDER_URL`     | OIDC issuer/provider URL. |
| `HEIMDALL_OIDC_CLIENT_ID`        | OIDC client ID. |
| `HEIMDALL_OIDC_CLIENT_SECRET`    | OIDC client secret for browser login. |
| `HEIMDALL_OIDC_COOKIE_KEY`       | 32-byte base64 cookie encryption key. Generate with `head -c 32 /dev/urandom | base64`. |
| `HEIMDALL_OIDC_SUBJECT_CLAIM`    | Claim used for the generic user ID. Defaults to `sub`. |
| `HEIMDALL_OIDC_CLIENT_ID_CLAIM`  | Claim used for the generic client ID. Defaults to `client_id`. |
| `HEIMDALL_OIDC_SCOPES_CLAIM`     | Claim used for scopes. Defaults to `scope`. |
| `HEIMDALL_JWT_ENABLED`           | Enable validation-only JWT bearer authentication. |
| `HEIMDALL_JWT_ISSUER`            | Expected JWT issuer. |
| `HEIMDALL_JWT_AUDIENCES`         | Accepted JWT audiences. |
| `HEIMDALL_JWT_JWKS_URL`          | JWKS URL used to validate JWT signatures. |
| `HEIMDALL_JWT_SUBJECT_CLAIM`     | Claim used for the generic user ID. Defaults to `sub`. |
| `HEIMDALL_JWT_CLIENT_ID_CLAIM`   | Claim used for the generic client ID. Defaults to `client_id`. |
| `HEIMDALL_JWT_SCOPES_CLAIM`      | Claim used for scopes. Defaults to `scope`. |
| `HEIMDALL_AUDIT_ENDPOINT` | Base URL for Audit Trail. Defaults to `http://notifications:8080` until the downstream service is renamed. |

JWT bearer tokens must include an `exp` claim. The gateway loads the configured
JWKS when JWT authentication starts; restart the gateway after signing-key
rotation so the verifier uses the new key set.

## Audit ingestion

Audit ingestion follows the Job Manager facade: `AuditService.CreateAuditEvents`
is registered alongside `HeimdallService` and has a generated
`POST /v1/audit/events` HTTP route, relative to `HEIMDALL_URL_PREFIX`.
SAC can use an `AuditServiceClient` on the same gRPC connection and credentials
it uses for Job Manager. The gateway forwards events to Audit Trail; it does
not store them in Job Manager.

The public HTTP route uses the gateway's protobuf JSON contract and normal
status mapping: successful ingestion returns HTTP 200, including partial
acceptance, and overload returns HTTP 429. Downstream Audit Trail still returns
HTTP 202 for acceptance. Use the generated gateway OpenAPI contract for HTTP
producer clients; `eventProducedAt` is Unix microseconds encoded as a decimal
string, and `partialSuccess.rejectedEvents` reports partial rejection. A
successful response does not promise durable storage. Do not retry partially
accepted batches; retain producer idempotency keys when retrying overload.

The downstream service always exposes audit ingestion.

## Audit reads

`AuditService.GetAuditEvents` exposes the generated `GET /v1/audit/events` route
on the same authenticated listeners as ingestion. Query by internal job ID or
scheduler job ID with an optional attempt; a job without an attempt includes all
attempts, and attempt zero remains explicit. Event type, level, producer, and
inclusive producer-time bounds narrow the result. Page size defaults to and
cannot exceed 100; timestamp ordering and opaque adjacent-page cursors are
preserved. Read responses are bounded to 4 MiB and fail with `ResourceExhausted`
instead of truncation; ingestion replies retain their 4 KiB bound.

See the [public audit read contract](../docs/public-api/api-gateway.md#audit-reads)
for query names, JSON encoding, and an authenticated example.
