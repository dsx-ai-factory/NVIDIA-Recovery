# ADR 0005: Require Causal Evidence in Slurm Recovery E2E Tests

Status: accepted

Date: 2026-07-31

Related:
[ADR 0002](0002-carry-slurm-log-paths-in-scheduler-metadata.md) defines how
Slurm log paths cross the scheduler metadata boundary,
[ADR 0003](0003-discover-slurm-job-log-paths.md) defines exact-attempt path
discovery, and
[ADR 0004](0004-materialize-slurm-log-links.md) defines optional log-link
materialization and its collector mount contract.

## Context

The Slurm E2E suite was initially organized as closed-box recovery tests. Most
scenarios submit a managed job, inject or emit a failure, and pass after Slurm
reports a later attempt. That outcome is necessary for recovery testing, but it
does not establish which NVIDIA Recovery signal caused the recovery.

Every job submitted by the current harness enables APS automatic requeue mode.
The E2E Job Manager configuration also enables a state-based recovery path: if
a managed job reaches the failed scheduler state and no anomaly has arrived
within a configured threshold, Job Manager creates a generic crash event. The
normal recovery machinery can then produce advice and requeue the job.

This ADR calls that behavior **job-state-based failed-job recovery**. It is a
deliberate availability mechanism for failed jobs whose intended log or metric
signal was not observed. It is distinct from Watcher's log-pattern detection,
metrics detection, the watchdog fail-open release path, Slurm's own job state
transition, and an operator-initiated requeue. The shorter term "fallback" is
not precise enough for test names, assertions, or diagnostics because each of
those mechanisms may otherwise be described as a fallback.

Job-state-based failed-job recovery currently creates its event quickly in the
E2E profile. Consequently, a log-detection scenario can pass when:

1. the workload exits unsuccessfully
2. Alloy never collects the intended output, or Watcher never matches it
3. Job Manager observes the failed scheduler state and creates a generic crash
   event
4. the recovery path requeues the job
5. the test observes the new attempt without inspecting the causal evidence

Several suite details make that ambiguity material:

- the collector watches a log-link glob ending in `.log`, while ordinary Slurm
  batch output commonly uses `.out`
- the current hang fixture sleeps for a bounded period and exits unsuccessfully,
  so job-state-based failed-job recovery can act without a no-output detector
  having fired
- the current straggler-labelled fixture does not inject the production metric
  signal required to prove straggler detection
- the FACT scenarios deliberately push fixtures directly to Loki and therefore
  do not exercise the filesystem collector
- the filename-correlation scenario proves many useful behaviors but combines
  deployment inspection, manual Slurm transitions, link rotation, collection,
  detection, and Job Manager polling in one large test

The existing `detectedBy` Advice value is not sufficient source evidence.
Issue creation currently assigns `Heimdall Watcher` without distinguishing a
Watcher observation from a generic event created by job-state-based failed-job
recovery. A test that checks only that value can therefore preserve the same
false positive under a more detailed-looking assertion.

The public product contract requires an integrated detection-to-restart loop,
production-like log onboarding, and an operator-visible audit history. The
supported architecture requires signals to
enter through production ingestion paths, recovery action sequences to be
asserted in the production audit store, and explicit coverage for watchdog,
retry guardrail, and scheduler failure modes. E2E tests need enough open-box
evidence from production read interfaces to establish those claims without
introducing hidden test-only ingress.

The current Kind deployment gives the Slinky controller, login, compute, and
collector pods access to a common host-backed `/home`. This is a useful
single-node implementation of shared storage, but it does not by itself define
a representative Slinky or bare-metal Slurm log-collection contract. In
particular, it does not validate a multi-node shared volume, ordinary Slurm
output names, future-file permissions, or a node-local bare-metal collector.

## Decision

### Treat recovery as a chain of independently asserted evidence

An E2E scenario may claim that a particular detection mechanism caused recovery
only when it establishes the applicable sequence through production interfaces:

1. the intended signal reached the backend used by the deployed component
2. an anomaly or equivalent detection record identifies the exact job attempt
3. Advice for that attempt contains the expected state, issue type, and
   source-specific evidence
4. the selected recovery action is persisted through the production audit path
5. the Slurm Advisor Controller or other production adapter applies the action
6. Slurm reports the expected resulting attempt or terminal state

A scenario does not need every layer when its stated scope is narrower. It must
name the layers it covers and must not claim the omitted behavior. For example,
a direct-to-Loki Watcher fixture can prove Watcher and Advice behavior without
claiming filesystem collection. A collector scenario must prove the file was
read through the deployed collector before claiming the full log path.

Scheduler recovery alone is never sufficient evidence for log-pattern,
no-output, straggler, attribution, or collector behavior.

### Use production read surfaces for open-box assertions

The suite will use production APIs and stores to inspect outcomes. The initial
read surfaces are:

- Job Manager's Advice query for an exact job ID and attempt
- the public Advisor API `ListAdvice` boundary in dedicated contract coverage
- Job Manager job status and anomaly queries
- Loki queries scoped to unique fixture markers, job identity, and time
- the notification service's workload query for persisted recovery actions
- Slurm accounting and control commands used by normal operators and the
  production Slurm adapter

Kubernetes service proxying is acceptable transport for an E2E runner to reach
an existing production HTTP endpoint. Tests must not add a test-only endpoint,
write directly to a component database, or inject internal messages merely to
make assertions convenient.

Service logs remain diagnostics rather than the primary success oracle when a
structured production read surface exists.

### Require trustworthy detection-source evidence

Every source-specific scenario must distinguish its intended detector from
job-state-based failed-job recovery, watchdog release, and operator action.
Checking `detectedBy` alone does not meet this requirement until that field
actually preserves the distinction.

The first implementation may use a compound oracle consisting of:

- exact job ID and attempt index
- expected anomaly and Advice type
- a fixture-specific relevant log line or metric observation
- expected Advice state and remediation action
- an audit record created after the test started
- explicit absence of the generic job-state-based failed-job recovery event

If these values cannot distinguish the sources reliably, NVIDIA Recovery must persist
a trustworthy detection-origin discriminator and expose it through an existing
production read surface before the scenario can claim source-specific coverage.
This ADR does not require that discriminator to become a new public Advisor API
field. Adding or changing a public contract requires its own schema review and,
if durable, a follow-up ADR.

### Test job-state-based failed-job recovery as its own mechanism

Job-state-based failed-job recovery remains enabled in the representative
action-mode deployment. Its activation in a log- or metric-detection scenario
is a test failure, even when the workload subsequently recovers.

The suite will include a dedicated job-state-based failed-job recovery scenario
that disables or withholds the earlier detector through normal workload policy,
allows the job to enter a failed scheduler state, and asserts the generic event,
Advice, persisted action, and Slurm result. This preserves coverage of the
availability behavior without allowing it to substitute for another detector.

Submission helpers will accept scenario-specific APS policy rather than
silently attaching one identical automatic-requeue comment to every job.

### Add a healthy negative control

At least one managed workload will complete successfully once. After a bounded
observation window, the test will assert that the workload has no failure
anomaly, recovery Advice, persisted recovery action, or extra attempt.

This scenario establishes that ordinary completion does not produce recovery
evidence and helps identify cross-scenario contamination.

### Adopt one shared-filesystem Slinky reference topology

The primary Slurm-on-Kubernetes E2E profile will model a shared job-log
filesystem:

- login and compute workloads see the same job-log root at the same absolute
  path
- the Advisor Controller sees that root when log-link materialization is
  enabled
- the collector sees the link destination and source roots at the same absolute
  paths required by ADR 0004
- ordinary Slurm parent output such as `slurm-%j.out` or `%x-%j.out` is a valid
  collected input; `.log` is not a mandatory extension
- directory ownership, group access, and default ACL or equivalent future-file
  permissions are configured explicitly
- the collector uses a dedicated unprivileged identity and the least-privilege
  mounts defined by ADR 0004

The E2E Helm values will express this as a shared-volume contract. A single-node
Kind environment may use host-backed storage to implement the volume, but tests
and documentation will not present hostPath or single-node placement as the
production contract.

Bare-metal Slurm remains a separate qualification profile. It may use a
collector on every compute node for node-local output or one collector over a
genuinely shared filesystem. Passing the Slinky/Kind profile does not claim
that systemd packaging, node-local paths, or BCM permissions have been tested.

### Give every scenario one explicit claim

The suite will converge on these scenario roles:

- healthy completion with no recovery
- crash detected through ordinary Slurm output and the deployed file collector
- custom-step output collection, only if retained as a supported contract
- job-state-based failed-job recovery
- no-output hang detection using a workload that remains alive and a timeout
  supplied through normal configuration
- straggler detection only when a production metrics signal is available
- direct-to-Loki Watcher and FACT scenarios explicitly labelled as collector
  bypasses
- exact-attempt log-path and symlink rotation
- watchdog fail-open and unproductive-restart guardrail coverage
- scheduler-boundary cancellation, timeout, preemption, and `NODE_FAIL`

Until a valid production metrics fixture exists, a bounded sleep followed by a
failed exit must not be described as a straggler test. A no-output hang fixture
must remain running until NVIDIA Recovery acts; a failed exit tests a different signal.

Each run will use a unique marker and scope observations by job, attempt, node
where applicable, and test start time. Broad queries that can match evidence
from an earlier scenario are not sufficient.

### Separate filename rotation from deployment conformance

The exact-attempt filename and symlink-rotation scenario will remain one
sequential workload lifecycle, because its assertions depend on attempt order.
Reusable clients will own Job Manager, Advice, Loki, notification, and
filesystem polling. The scenario body will describe only the lifecycle:

1. observe attempt zero's discovered path and owned link
2. cause a controlled requeue through a production Slurm operation
3. observe attempt one's distinct path and link
4. verify attempt zero remains addressable as required
5. emit and collect an attempt-specific marker
6. assert exact-attempt anomaly, Advice, and action evidence

Collector sandbox, mount, and controller security checks will run as separate
deployment-conformance tests. They are preconditions for the scenario, not
steps interleaved with its behavioral assertions.

### Tier CI by cost without weakening the oracle

Fast healthy, crash, job-state-based failed-job recovery, and metadata/link
coverage should form the blocking merge-request lane once stable. Long-running
hang, guardrail, multi-node, and real-FACT cases may run in qualification lanes.
Moving a scenario to a slower lane changes cadence, not its evidence contract.

CI retries must not turn a deterministic assertion failure into an unexplained
pass. When infrastructure retries remain necessary, reports will preserve the
failed attempt and identify whether the failure was setup, timeout, or product
evidence.

Structured timestamps for signal ingestion, detection, Advice, action, and
restart will be retained where the production surfaces expose them. They provide
the evidence needed for later MTTR checks without adding a test-only timing path.

## Consequences

The E2E suite will detect broken collectors, missing Watcher matches, wrong
attempt correlation, incorrect Advice, and missing audit writes even when Slurm
eventually requeues the workload. Failures will identify which recovery stage
was absent instead of reporting only that a retry timed out.

Tests become more open-box, but only through interfaces that ship and are used
operationally. This increases coupling to production schemas while reducing
coupling to prose log messages and internal timing accidents.

The reference Slinky profile becomes more realistic and documents what it
actually proves. It still does not replace multi-node or bare-metal
qualification, so those deployment claims require their own lanes and
environments.

Some current scenarios will be renamed, split, or temporarily quarantined
because their fixtures do not supply the signal named by the test. This may
temporarily reduce the apparent scenario count while increasing trustworthy
coverage.

Job-state-based failed-job recovery may cause detector scenarios to fail while
the workload successfully recovers. That is intentional: production
availability and evidence that a specific detector works are different test
claims.

The current Advice representation may not carry enough source information. A
small persistence or API change may therefore be required, but this ADR does
not pre-authorize a public schema change.

Maintaining audit assertions adds setup and polling cost. Shared typed clients,
unique run markers, and scenario-layer declarations are required to keep that
cost manageable and diagnostics consistent.

## Alternatives Rejected

### Disable job-state-based failed-job recovery in all E2E deployments

This would prevent one false-positive path but make the action-mode environment
less representative and remove coverage of a production availability
mechanism. Keeping it enabled, rejecting it as evidence for other detectors,
and testing it explicitly provides stronger separation.

### Continue asserting only Slurm retries

A retry cannot identify whether Watcher, job-state-based failed-job recovery,
the watchdog, an operator, or Slurm itself caused progress. This preserves the
current ambiguity and cannot satisfy the public audit requirements.

### Use service log messages as the primary oracle

Log text is useful for diagnostics but is not the production Advice or audit
contract. It is vulnerable to wording changes, incomplete context, and broad
matches from another scenario.

### Require only `detectedBy = Heimdall Watcher`

The current assignment does not distinguish Watcher observations from generic
events created after a failed scheduler state. The assertion would look more
specific without proving a different behavior.

### Push every fixture directly to Loki

Direct Loki ingestion is appropriate for focused Watcher and FACT tests, but it
bypasses Slurm output, permissions, log-link materialization, and the deployed
collector. It cannot replace at least one ordinary-file end-to-end scenario.

### Make one Kind topology stand in for both Slinky and bare-metal Slurm

The environments have different process, mount, permission, and collector
shapes. A shared behavioral contract is useful, but claiming deployment parity
from one topology would hide the risks this decision is intended to expose.

### Preserve the current combined filename-correlation scenario

Combining deployment security, filesystem lifecycle, collector behavior,
detection, and recovery in one body makes failures difficult to localize and
discourages reuse. A sequential rotation scenario plus separate conformance
checks preserves coverage with clearer ownership.
