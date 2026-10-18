# API Gateway Agent Guide

This file applies to `api-gateway/`. Follow root `AGENTS.md`, invoke the
`go-engineering` skill, and apply `docs/quality/go-engineering.md` first.

## Component Rules

- Keep the gateway a scheduler-neutral HTTP and gRPC service facade. Do not move
  Slurm policy or command execution into this module.
- Use `github.com/kelseyhightower/envconfig` with explicit retained environment
  names. Keep dependent defaults, normalization, and validation visible.
- Use `go.uber.org/zap.Logger` with typed fields and boundary-only error logging.
- Use `github.com/cockroachdb/errors` for production error construction and
  wrapping while preserving `Is` and `As` behavior.
- Use MockGen for interaction mocks. Stateful service doubles and fixture
  servers may remain purpose-built behavioral fakes.
- Treat `gen/` as generated output. For Buf-managed protobuf and OpenAPI output,
  run `make GO_PACKAGES=api-gateway generate-go` followed by
  `make GO_PACKAGES=api-gateway check-generated-go`. For the component-owned
  Job Manager and Notifications HTTP bindings, run
  `make generate-api-gateway-bindings` followed by
  `make check-generated-api-gateway-bindings`. Never hand-edit generated
  files.
- API contract changes must test dependent controller behavior because
  `slurm-advisor-controller` consumes this module through a local replacement.
- Authentication, authorization, identity, token, and secret-loading changes
  require explicit human review.

## Verification

Prefer root targets:

```bash
make GO_PACKAGES=api-gateway verify-go
```

Run the appropriate Buf-only or full legacy binding targets when API definitions
or bindings change, then rerun the module-scoped `verify-go`. Direct Go commands
are allowed only when no owning Make target exists or for focused diagnosis of a
target failure, and must be followed by the owning Make target.
