# Test a change

This guide is for contributors choosing validation for an NVIDIA Recovery
change. Start with a source checkout and the component dependencies from
[Start developing](getting-started.md). The expected result is evidence for
the behavior you changed, with a clear statement of what the checks do and do
not cover.

## Choose the smallest useful check

Run these commands from the repository root. Select the row that owns your
change; they are not all required for every contribution.

| Change | Focused check | What it covers |
| --- | --- | --- |
| Python package | `scripts/ci/test-python.sh --package watcher` | Unit tests for one package; replace `watcher` with the owning package name. Integration directories are excluded. |
| Go module | `make GO_PACKAGES=api-gateway verify-go` | Generated artifacts, module tidiness, lint, vet, unit tests, and race tests for the selected module. Use `slurm-advisor-controller` for controller or watchdog changes. |
| Dashboard | `make lint-typescript test-typescript` | Dashboard lint and tests, after installing its locked pnpm dependencies. |
| E2E harness and tooling | `make e2e-harness-test` | Cluster-free tests for `tests/e2e_harness` and, in `tests/tooling`, the CI, build, and E2E scripts. |
| E2E shell and Python surfaces | `make e2e-lint` | Shell checks, existing lifecycle self-tests, Python formatting and lint. Requires ShellCheck and uv. |
| Slurm scheduler behavior | `make test-slurm-advisor-harness` | Controller and watchdog behavior against real Slurm and a mock public Advisor API. |
| Complete local recovery path | `DEPLOY_INFRA=1 DEPLOY_STACK=1 make e2e` | Disposable Kind/Slinky environment, backend services, and ordered recovery scenarios. Read the lifecycle warning below first. |

The broad `make lint` and `make test` targets remain available. Unscoped
`make verify-go` checks both Go modules. Use broad checks when shared changes
justify them; a repository-wide preflight is not the default gate.

For file-scoped Python formatting and lint, pass the changed files explicitly:

```bash
scripts/ci/format-python.sh watcher/watcher/main.py
scripts/ci/lint-python.sh watcher/watcher/main.py
```

Both check without modifying files unless `--fix` is supplied. For a single Go
test while iterating, use the module selector and test arguments:

```bash
make GO_PACKAGES=slurm-advisor-controller test-go GO_TEST_ARGS='-run TestName'
```

Replace `TestName` with an existing test or test pattern. `verify-go` deliberately
clears `GO_TEST_ARGS` so its full selected-module checks cannot be filtered.

### Integration tests and optional provider checks

The dedicated Python integration entrypoint supports `recovery-libs` and
`notifications`:

```bash
scripts/ci/test-python-integration.sh --package recovery-libs
```

Check the owning suite's fixtures and dependency requirements before running
it. A package unit-test pass does not establish integration coverage.

Public component checks remain free of private provider credentials. Adapter
health, HTTP contract, and schema tests run with:

```bash
scripts/ci/test-python.sh --package attribution-adapter
```

Private provider qualification in `attribution_adapter/tests/provider/` needs
explicit selection, the private client, and the ADI workspace package. Missing
dependencies fail that selection rather than silently skipping it. See
[NVIDIA Attribution](../public-api/optional-attribution.md).

Nightly selection is defined in [CONTRIBUTING.md](../../CONTRIBUTING.md#nightly-tests).
Ordinary local tests are unfiltered unless you supply a selection flag; PR and
main-branch CI explicitly exclude nightly tests.

## Test scheduler behavior with the controller harness

The [controller and watchdog harness](../../slurm-advisor-controller/harness/README.md)
requires Docker with Compose, Go, and access to its public build dependencies:

```bash
make test-slurm-advisor-harness
```

It builds its images, starts Slurm with three simulated compute nodes and a
stateful mock of the public `HeimdallService`, then exercises the safety matrix.
It proves scheduler behavior such as environment isolation, observe-mode
inhibition, requeue/hold/release, cancellation, and watchdog handling. It does
not run the complete Watcher, Job Manager, and log-ingestion path.

The smoke runner removes its Compose resources and volumes on exit. Set
`KEEP_SLURM_ADVISOR_HARNESS=1` to retain them for inspection and follow the
harness guide for cleanup. Use an isolated Docker environment if its resources
or published ports would conflict with another harness run.

## Use the complete local stack

Follow the [local tutorial](../getting-started.md) for first startup and
dashboard access. Local development and E2E share the Tilt deployment graph:

| Target | Lifecycle |
| --- | --- |
| `make dev` | Starts interactive Tilt, leaves the cluster available, and skips scenarios. |
| `make dev-ci` | Runs headless Tilt readiness checks and leaves the cluster available; no scenarios. |
| `DEPLOY_INFRA=1 DEPLOY_STACK=1 make e2e` | Converges the stack, runs scenarios, and tears down its resources on exit. |
| `DEPLOY_INFRA=1 DEPLOY_STACK=1 make e2e-up` | Runs the same suite but retains the cluster for inspection. |

The checked-in [Kind template](../../scripts/ci/e2e/values/kind-node-separated.yaml)
has one control plane and three workers. Two workers host the separate Slinky
compute nodes for node-local logging checks. No hardware minimum has been
qualified for this topology; provision enough CPU, memory, and disk for its
resource requests and inspect pending Pods or image-build failures if it does
not converge. Initial pulls and builds can take several minutes.

> [!WARNING]
> `make dev`, `make dev-ci`, and `make e2e` recreate the selected Kind cluster
> when `DEPLOY_KIND_CLUSTER` is enabled, including an existing cluster with
> the same name. Teardown also removes the configured local registry container,
> even if it reused that container at startup. Defaults are `kind-registry` on
> port `5000`, `heimdall-e2e-dev` for development, and `heimdall-e2e` for E2E.
> A distinct cluster name alone does not isolate concurrent runs. Use a
> dedicated Docker context, or choose a distinct cluster name, registry name,
> and free registry port. Never use a production cluster or queue.

For example, after confirming these names and port are available:

```bash
CLUSTER_NAME=recovery-e2e-test \
KIND_REGISTRY_NAME=recovery-e2e-test-registry \
KIND_REGISTRY_PORT=5002 \
DEPLOY_INFRA=1 DEPLOY_STACK=1 \
make e2e
```

The owned kubeconfig is `/tmp/kind-<cluster-name>.kubeconfig`; the harness does
not overwrite the default kubeconfig. Keep the same cluster and registry
settings when cleaning up a retained environment. `make dev-down` uses the
development lifecycle cleanup; `make e2e-down` deletes the selected Kind cluster
and kubeconfig but does not remove its registry container.

### Profiles and optional phases

`E2E_PROFILE=advisor-act` is the default for both local development and E2E.
It enables the controller's action mode and scheduler-action acknowledgment
inside the test environment. `E2E_PROFILE=advisor-observe` selects observe mode
without acknowledgment. Select explicitly when it matters:

```bash
E2E_PROFILE=advisor-observe make dev
```

Attribution defaults to `E2E_ATTRIBUTION_MODE=available` and currently requires
authorized source access. Explicit `E2E_ATTRIBUTION_MODE=unavailable` selects
degraded operation without provider source. Public E2E CI remains explicitly
degraded until upstream publication; it does not qualify Attribution ON.

Log-store selection is independent of the recovery profile. Loki remains the
default; set `E2E_LOG_BACKEND=victorialogs` on `make dev`, `make dev-ci`, or the
complete E2E commands to provision the disposable VictoriaLogs variant and
route collection and native evidence queries to it. Recreate the disposable
cluster when changing backends. See
[log-query backend configuration](../ops/configure-log-query-backends.md#local-tilt-and-e2e)
for the supported selectors, query UI, and diagnostics.

Optional phase flags follow the harness convention: an empty value is off;
any non-empty value, including `0`, is on. `make dev` defaults to infrastructure
and stack deployment; explicit empty overrides skip those phases. For the
complete disposable E2E path, set both `DEPLOY_INFRA=1` and `DEPLOY_STACK=1` as
shown above.

`DEV_CI_TIMEOUT` defaults to `90m` and bounds the Tilt build/deploy/readiness
phase. Scenario timeouts are separate. `SERVICE_WAIT_TIMEOUT` adjusts service
wait budgets in slow environments. The lifecycle implementation and further
settings live in [e2e.sh](../../scripts/ci/e2e/e2e.sh) and
[env.sh](../../scripts/ci/e2e/env.sh); GitHub invokes the lifecycle script directly.

## Rerun scenarios on a ready test cluster

You can reuse a dedicated, already-ready local cluster without starting Tilt
again, for example one kept by `make e2e-up`. Export the settings the cluster
was deployed with, then run only the test phase:

```bash
export CLUSTER_NAME=heimdall-e2e
export E2E_PROFILE=advisor-act
export E2E_LOG_BACKEND=loki
export E2E_ATTRIBUTION_MODE=unavailable
export E2E_STACK_NAMESPACE=slurm
make e2e-rerun
```

`make e2e-rerun` uses `/tmp/kind-${CLUSTER_NAME}.kubeconfig`, applies the
profile's readiness gate, and runs ordered scenarios. It prints each failure as
it happens; set `PYTEST_ADDOPTS` to pass other pytest options instead. The
runner writes `reports/e2e/<profile>-<log-backend>/job_state/junit.xml`; use
`E2E_XUNIT_PATH` to change the report path.

Use the following targets to find and rerun scenarios:

| Command | Result |
|---------|--------|
| `make e2e-list` | Lists every scenario name. |
| `E2E_SCENARIOS="10-simple-segfault 30-job-state-crash" make e2e-rerun` | Reruns the named scenarios. |
| `make e2e-failed` | Lists the scenarios that failed in the last report. |
| `make e2e-rerun-failed` | Reruns only those scenarios. |

Each rerun replaces the report, so repeating `make e2e-rerun-failed` narrows to
the scenarios that still fail. Set `E2E_REPORT` to read a different report.

Scenarios run on four pytest-xdist workers. Set `E2E_PYTEST_WORKERS` to change
that, or to `0` to run serially:

```bash
E2E_PYTEST_WORKERS=0 make e2e-rerun
```

Scenarios marked `exclusive` still run alone: they wait for running scenarios
to finish, and others wait for them. Mark a scenario module with
`pytestmark = pytest.mark.exclusive` when it restarts, scales, or reconfigures a
shared service, or injects node-level faults.

Decorate an action factory with `@unsafe` when its actions change shared stack
state. Actions that change only the scenario's own jobs or files are safe. A
scenario that uses an unsafe action prints a yellow warning when it starts.

E2E defaults to Advice-only recovery. Scenario `32-requeue-limit-guardrail`
checks the job-level limit; `57-system-requeue-limit` checks a lower system cap.
Scenarios 55 (checkpoint stopping) and 57 temporarily change JM policy and must
run serially on a dedicated test stack.

`56-eager-requeue` skips unless eager recovery is enabled. Run it separately on
a fresh stack with:

```bash
E2E_ENABLE_EAGER_REQUEUE=1 E2E_SCENARIOS=56-eager-requeue make e2e
```

For direct pytest iteration on that same ready stack:

```bash
VALIDATE_TIMEOUT=300 uv run --locked --group e2e pytest -ra --capture=tee-sys \
  tests/e2e/scenarios/10-simple-segfault/test_simple_segfault_recovery.py::test_simple_segfault_recovery
```

Direct pytest also defaults to `advisor-act`; it does not load all shell-profile
settings. Prefer the sourceable runner when you need the complete readiness
gate. Native fault-injection cases additionally need a reachable `LOKI_URL`,
such as `http://loki-gateway.monitoring.svc.cluster.local` for the bundled stack.
Their cleanup cancels captured allocations but waits for the whole Slurm
queue to become empty, so unrelated jobs can make cleanup time out.

## Validate configuration and docs

Evaluate the minimal Tilt graph without starting the stack or depending on the
current Kubernetes context:

```bash
DEPLOY_INFRA= DEPLOY_STACK= make dev-validate
```

An unqualified `make dev-validate` evaluates the full default graph and needs
the intended local context. Rendering proves configuration structure, not that
the services will start or a workload will recover. Use native Helm lint/schema
checks for chart structure and focused rendered-resource or integration checks
for meaningful deployment behavior.

For documentation changes, follow [documentation tooling](../../scripts/docs/README.md)
for link, schema, MDX, and local-render checks. Next, record the commands you ran
and any untested behavior in your PR using the
[contribution guide](../../CONTRIBUTING.md).
