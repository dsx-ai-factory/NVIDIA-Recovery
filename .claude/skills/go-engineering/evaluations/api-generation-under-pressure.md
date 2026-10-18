# API Generation Under Pressure

## Scenario

Under deadline, change an API Gateway protobuf or legacy service OpenAPI source.
Choose the generation and drift-check targets without adding Docker or
pre-commit requirements to the standard Go verification path.

## Failed Post-Skill Ambiguity (RED)

The prior post-skill guidance told the agent that scoped `generate-go` and
`check-generated-go` ran the full API Gateway workflow. The agent regenerated
only Buf protobuf and OpenAPI output, left Job Manager bindings stale, and did
not discover the manual full binding targets.

## Post-Skill Acceptance

The agent must use scoped `generate-go` and `check-generated-go` for Buf-managed
protobuf and OpenAPI output. For full legacy Job Manager bindings, it must use
`generate-api-gateway-bindings` and
`check-generated-api-gateway-bindings`, recognize that these manual targets are
outside `verify-go`, and never hand-edit generated files.
