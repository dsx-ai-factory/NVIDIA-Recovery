# Go Engineering

This guide is the repository-wide standard for Go implementation, review, and
verification. Root `AGENTS.md`, the `go-engineering` skill, and the nearest
component `AGENTS.md` all apply. Component guidance may narrow these rules but
must not replace them.

## Modules And Authority

NVIDIA Recovery currently has two Go modules:

| Module | Role |
| --- | --- |
| `api-gateway/` | Scheduler-neutral public HTTP and gRPC service facade and generated API bindings. |
| `slurm-advisor-controller/` | Slurm adapter and level-based recovery controller. |

Advice Operator is removed from the supported source and Helm charts.
The shared Advice API, Job Manager models, and Slurm recovery controller remain
supported; they do not depend on the removed Kubernetes Advice CRD.

Use existing package boundaries and local patterns unless an approved
architecture decision changes them. Do not treat inherited structure
as authority when it conflicts with the Advisor API direction.

## Required Libraries

### Configuration

Use `github.com/kelseyhightower/envconfig` for environment decoding.

- Give every retained environment variable an explicit `envconfig:"NAME"` tag;
  migrations must preserve existing names exactly.
- Use typed fields and custom `Decode` behavior rather than handwritten
  `os.Getenv` or `os.LookupEnv` parsing.
- Keep each default in one place. The Slurm controller owns defaults in
  `WithDefaults`, so its envconfig structs must not duplicate them in `default`
  tags. A `default` tag is valid only when envconfig is the sole default owner.
- Treat `required:"true"` as a presence check, not a nonempty check. Reject
  explicit empty values in decoding or semantic validation when emptiness is
  invalid.
- Decode first with `envconfig.Process`, then apply dependent defaults,
  normalization, and semantic validation in explicit functions.
- Preserve the difference between an absent variable and an explicit zero,
  false, or empty value when existing behavior depends on it. Do not let a
  default tag silently erase that distinction.
- Return configuration errors to the process boundary. Do not log from the
  decoding or validation package.

### Logging

Use `go.uber.org/zap.Logger` and typed fields such as `zap.String`,
`zap.Bool`, `zap.Int`, `zap.Duration`, and `zap.Error`.

- Log once at a process, request, reconcile, or operation boundary that owns the
  outcome. Internal helpers should add error context and return it.
- Do not log an error and return the same error unless the log records a distinct
  event that will not be logged again.
- Keep messages stable and concise. Put variable values in typed fields.
- Never log secrets, credentials, tokens, raw private payloads, or unbounded
  identifiers as metric labels.
- Use `zap.Error(err)` directly. Do not add a separate `%+v` stack field.
- Zap adds an `errorVerbose` field for errors that implement `fmt.Formatter`.
  For errors that may contain sensitive decoder or transport details, verify
  encoded output and use a redacted non-formatter adapter before `zap.Error`.

### Errors

Use `github.com/cockroachdb/errors` for production error construction,
wrapping, inspection, and aggregation.

- Use `errors.New` or `errors.Newf` for new failures and `errors.Wrap` or
  `errors.Wrapf` when adding boundary context to an existing error.
- Preserve identity required by `errors.Is` and type inspection required by
  `errors.As`; do not replace a causal error with formatted text.
- Keep wrappers compatible with the standard-library `errors.Is` and
  `errors.As` functions. The sentinel or typed error must remain in the real
  unwrap chain; do not use library-only identity mechanisms at public package
  boundaries.
- Use `errors.Join` or `errors.CombineErrors` when independent failures must all
  be retained.
- Add actionable context without duplicating the underlying message. Keep
  sensitive or customer-identifying values out of error strings.
- A small private `Error` plus `Unwrap` adapter is allowed when a third-party
  error omits `Unwrap` or when the logging boundary must redact its message.
  Test standard `Is` and `As` behavior and the encoded log output.
- Tests may use standard-library sentinels when appropriate, but production
  construction and wrapping follow this package.

### Interaction Mocks And Behavioral Fakes

Use MockGen from `go.uber.org/mock/mockgen` for interaction mocks of interfaces.

- Keep `//go:generate` directives beside the interface owner and pin MockGen to
  the repository version.
- Never hand-edit generated mocks. Regenerate through the owning Make target.
- Use generated mocks when a test asserts calls, arguments, ordering, or
  cardinality.
- Purpose-built fakes, fixture servers, and in-memory stores are valid when they
  model state or behavior. Do not force stateful simulations into expectation
  mocks.
- Prefer narrow consumer-owned interfaces over broad dependency interfaces.

## Implementation Decisions

- Read the nearest component `AGENTS.md` and inspect existing patterns and tests
  before choosing an implementation.
- Retrieve current external documentation only when correctness depends on an
  exact library version or API detail; stop when the required fact is supported.
- Add or update focused tests for changed behavior. Use a test-first loop when
  it materially improves feedback, not as mandatory ceremony.
- Make the smallest maintainable implementation. Include generated output and
  dependency changes only when the behavior requires them.
- Treat scheduler mutation, authentication, secret handling, migrations, and
  release behavior as security-sensitive changes.

## Make-First Verification

The root Makefile is the public developer interface. Before using a direct
`go`, `golangci-lint`, `mockgen`, or generation command, check for an owning
Make target. For a change confined to one module, finish with
`make GO_PACKAGES=<module> verify-go`. For shared Go tooling, API contracts
consumed across modules, or other cross-module changes, finish with unscoped
`make verify-go`.

The standard targets include:

```bash
make GO_PACKAGES=api-gateway verify-go
make GO_PACKAGES=slurm-advisor-controller verify-go
make generate-api-gateway-bindings
make check-generated-api-gateway-bindings
make check-generated-go
make check-tidy-go
make lint-go
make vet-go
make test-go
make test-go-race
make verify-go
make test-slurm-advisor-harness
```

The scoped `generate-go` and `check-generated-go` targets run the CI-compatible
Buf protobuf and OpenAPI workflow for API Gateway. The explicit API Gateway
binding targets run the component-owned Job Manager and Notifications HTTP
client generation and check all generated trees; they are manual and are not
part of `verify-go`.

Use `GO_TEST_ARGS` with `make test-go` for a focused test when possible. Focused
direct commands are allowed to diagnose a failure or exercise a package when no
Make target exists. Record the gap and rerun the owning Make target before
claiming completion. Never substitute a convenient direct command for a failing
repository target.

## Review Checklist

- Existing environment names, defaults, normalization, and explicit-value
  semantics are preserved.
- Errors retain causal identity and are logged once with typed Zap fields at the
  owning boundary.
- Interaction mocks are generated; behavioral fakes model state rather than
  expectations.
- Generated files were produced through repository commands and contain no
  manual edits.
- Focused tests and the relevant root Make targets pass.
- Security-sensitive behavior has explicit human review evidence.

## Source Anchors

Repository decisions override generic guidance. Use the
[Go Code Review Comments](https://go.dev/wiki/CodeReviewComments),
[Go Test Comments](https://go.dev/wiki/TestComments), and
[Google Go Style Guide](https://google.github.io/styleguide/go/guide) for
language-level judgment. The
[cc-skills-golang](https://github.com/samber/cc-skills-golang) project is a
skill-taxonomy reference, not an imported rule set; its defaults do not override
this guide or the machine-checked repository contract.
