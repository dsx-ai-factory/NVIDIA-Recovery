# ADR 0052: Run E2E Tests as Isolated Staged Scenarios

Status: accepted

Date: 2026-10-08

> **Implementation note (2026-10-08):** Disruptive scenarios still share the
> stack with ordinary ones, serialized by the `exclusive` marker and stack lock;
> the separate disruptive stack is not built yet. Evidence:
> [source](../../tests/e2e/conftest.py).

## Context

The Slurm recovery E2E suite runs each scenario inside one pytest test. That
test submits Slurm jobs through `kubectl exec` into the login pod, performs
mid-run actions such as cancelling a job or releasing a DAG parent, changes
shared stack state for some scenarios, and asserts evidence read through the
Kubernetes API server's service proxy. Setup and readiness checks are pytest
fixtures, and cleanup assumes nothing else uses the cluster.

That coupling has three costs:

- A run cannot coexist with other runs. Cleanup in `managed_job` waits for the
  whole Slurm queue to drain, so concurrent scenarios time out in each other's
  teardown. Running pytest-xdist workers against one stack exposed this
  immediately.
- Scenarios that restart services or change global Job Manager retry limits
  change the outcome of every other job on the stack, so they cannot run
  alongside ordinary scenarios.
- Every test reaches Slurm and the services through Kubernetes, which ties the
  suite to one deployment platform even though Slurm is a product target in
  its own right.

ADR 0005 defines what evidence a recovery scenario must establish. This ADR
defines how scenarios are executed and verified; it does not change the
evidence rules.

## Decision

Each E2E test is a staged scenario that launches its own jobs through a
scheduler-neutral launcher and verifies its own evidence over HTTP, through
separate components with narrow contracts. Tests are independent of every other
test and run, so they can run in parallel. Cluster setup and stack deployment
run outside pytest.

### Components

- **Slurm access** runs Slurm commands against the login node over
  `kubectl exec`, SSH, or the Slurm REST API. `kubectl exec` is the default. It
  knows nothing about scenarios or evidence.
- **Job launcher** submits a scheduler-neutral job spec and translates it for
  its scheduler. Scheduler-specific detail, such as a Slurm batch script or
  hold, travels in the spec's workload and metadata, so a launcher for another
  scheduler can replace it without changing scenarios. It returns job snapshots
  with identities and timestamps, performs mid-run actions such as cancelling
  or releasing a job, and cleans up only the jobs it submitted.
- **Recovery client** gives HTTP access to Job Manager, Notifications, the API
  Gateway, the advisor controller's metrics, and the log backend, with one
  scoped API per service. It reads evidence, and it writes only to Job
  Manager's job ingress to replay scheduler observations that the scheduler
  cannot produce. Each service has a configured base URL. On Kubernetes, the
  harness supplies port-forwarded URLs when none are configured.
- **Tests** are staged scenarios. A test returns the jobs to launch by role and
  ordered stages. Each stage waits for evidence, requires evidence to hold for a
  period, or acts on the run, and setup acts prepare what the jobs need before
  they launch. A runner executes the scenario, reports a timeline of stages,
  and undoes the scenario's changes on the way out. Tests do not run kubectl,
  build service URLs, or call Slurm directly.
- **Stack control** makes the changes disruptive scenarios need, such as
  restarting or scaling a service, through scenario actions that register their
  own undo. Cluster creation, stack deployment, and readiness run outside
  pytest.

### Isolation

- Each test owns its jobs and evidence. Launchers and tests act only on the
  identities their test created, and never wait on or read global state such
  as the whole Slurm queue.
- A test does not depend on test order, other tests, or other pytest sessions
  using the same stack.

### Instrumentation And Timeline

The recovery client, Slurm access, and evidence waits are instrumented with
OpenTelemetry. Each HTTP request and Slurm command is a span with its operation,
outcome, and duration; each wait for evidence is a span named for its phase,
such as job submission, queue wait, detection, Advice, scheduler action, and
recovery, with its poll count and the requests it made nested inside; and each
test is a root span named for its scenario. HTTP requests carry W3C trace
context, so instrumented services join the test's trace.

Instrumentation composes around the components rather than living inside them.
Wrapper classes implement the same protocol as the transport or HTTP layer they
wrap and add spans and trace context, and a decorator names the spans of
operations such as submitting a job or reading Advice. The components contain no
tracing code, and the harness wires the instrumented versions together in one
place.

Spans are exported to the stack's OTLP collector when one is configured and are
always written to a per-run file. A test's timeline and the run summary are
derived from those finished spans: the timeline explains where one test's time
went and names the phase that consumed a timeout, and the summary aggregates
phase durations per scenario and across the run in a stable format that
compares between runs. The summary is the benchmark for E2E performance work: a
change to the harness, a fixture, or the product is judged by its effect on
these phase durations.

### Interfaces

Components depend on small protocols and receive their dependencies through
their constructors. They exchange immutable values, such as job handles and
evidence, and keep no mutable shared state; timing is carried by spans rather
than by objects that tests update.

### Disruptive scenarios

Scenarios that restart, scale, or reconfigure shared services, or that change
global Job Manager policy, run on their own disposable stack. They never share a
stack with ordinary scenarios. Where the product already supports a per-job
setting, such as `aps_spec.max_requeue_times`, a scenario uses it instead of
changing global state and stays ordinary.

## Consequences

- Ordinary scenarios run in parallel under pytest-xdist without locks or
  exclusivity markers, and wall time approaches the slowest scenario rather
  than the sum.
- Most of the suite can run against a non-Kubernetes deployment by configuring
  SSH and service URLs. Disruptive scenarios stay platform-specific until stack
  tasks exist for that platform.
- Slow and failing tests report which phase took the time, so a timeout points
  at submission, scheduling, detection, Advice, action, recovery, or polling
  rather than at the test as a whole.
- E2E performance becomes measurable. Run summaries give a baseline, show
  whether time goes to fixtures, polling, scheduling, or product latency, and
  confirm whether an optimization helped.
- Disruptive scenarios need a second stack in CI, which adds provisioning time
  and runner capacity.
- The migration is incremental. The recovery client, Slurm access, the job
  launcher, and the move of each scenario land as separate changes, and the
  current scenarios keep working until each is migrated.
- Evidence reads move from the Kubernetes service proxy to direct HTTP. This
  narrows ADR 0005's allowance for service-proxy transport to the Kubernetes
  fallback; its evidence requirements are unchanged.

## Alternatives Rejected

- **Coordinate pytest workers with a stack-wide lock.** Scenarios marked
  exclusive took a file lock that other workers shared. It only coordinates
  workers inside one pytest session and leaves global cleanup assumptions in
  place.
- **Run the suite serially.** This avoids interference but keeps wall time at
  the sum of all scenarios.
- **Launch every scenario in a separate phase and verify from a manifest.**
  This decouples relaunching from assertion fixes, but splits each scenario
  across two tools and loses the single test that owns a scenario end to end.
- **Read evidence through the Kubernetes API server proxy.** This works on Kind
  but ties every assertion to Kubernetes access and to API-server permissions
  that a client of the deployed system does not otherwise need.
