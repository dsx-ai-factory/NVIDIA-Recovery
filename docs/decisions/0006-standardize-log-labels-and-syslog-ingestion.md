# ADR 0006: Standardize Log Labels and Syslog Ingestion

Status: accepted

Date: 2026-08-03

Related:
[ADR 0003](0003-discover-slurm-job-log-paths.md) defines exact-attempt
application-log path discovery,
[ADR 0004](0004-materialize-slurm-log-links.md) defines the optional
application-log link materializer and collector boundary, and
[ADR 0005](0005-require-causal-evidence-in-slurm-recovery-e2e-tests.md)
defines causal evidence requirements for Slurm recovery E2E tests. This
decision supersedes ADR 0005 only where that record permits direct-to-Loki
system-log fixtures; ADR 0005's remaining evidence requirements stay active.

## Context

NVIDIA Recovery consumes two log domains with different ownership and correlation
semantics:

- Watcher detects workload failures from training application logs. Those logs
  belong to an exact scheduler job attempt and need job identity for detection
  and correlation. Exact-attempt association may come from scheduler state and
  the source path rather than a log label.
- NV Attribution consumes system logs to diagnose node failures. A system log
  belongs to a node and time, not to whichever workload happened to be using
  the node when the message was emitted.

The current repository does not preserve that distinction consistently.

The getting-started Watcher values select application streams with
`app="heimdall"` and `cluster=<environment>` and require
`slurm_job_id`, `slurm_job_name`, and `slurm_job_attempt`. The linked Alloy
runbook instead labels application streams with `job="slurm-job"` and `job_id`,
and labels system streams with `job="systemd-journal"`, `cluster`, `nodetype`,
`host`, `unit`, and `syslog_identifier`. Following both documents verbatim does
not give Watcher the documented application-log labels.

The local Slurm E2E profile has a coherent application-log path through Alloy,
but its FACT scenarios bypass the node logging and collector path. A workload
helper posts both application and kernel fixtures directly to Loki and applies
one shared label set, including `slurm_job_id`, to both streams. The configured
syslog selector does not require that job label, but the fixture incorrectly
models system logs as job-owned and cannot detect a broken node syslog or Alloy
configuration.

The legacy BCM collector tails application `.log` and `.out` files only. Its
Watcher and application-log attribution selectors match the labels the
collector emits, but its system-log selector deliberately uses the impossible
`job_id="__syslog_unavailable__"`. That deployment therefore has no qualified
system-log path even though its attribution components are enabled.

The Run:ai QA suite demonstrates a useful Kubernetes fault-injection pattern.
It mounts the node's `/var/log/syslog` file at the same path in a training pod,
with write access, and has rank zero append a correctly formatted kernel/Xid
record. The fixture then allows time for the collector to ingest the record,
emits the application-log crash signature expected by Watcher, and exits with
status 139. A paired crash-without-Xid scenario provides the negative control.
This exercises collection from the node's real syslog file without giving the
fixture a direct Loki write path.

The inherited deployments are useful compatibility evidence. Their
portable Slinky and E2E examples query system logs with `cluster`,
`component="journal"`, and `node_name`; their application consumers use
`appName="slurm-app-log"` and the logical attribute `job_id`. Some inherited
environments map `host`, `node`, or `host_name` toward `node_name`, but many
production selectors preserve only provider-specific labels such as
`service_name`, `p_cluster`, or `s_cluster`. The production selectors vary
substantially, which reinforces the SRD requirement for one documented
contract and deployment-time mapping rather than preserving every
environment's label vocabulary.

The PRD and SRD require independently validated syslog and application-log
signals, an application-log contract shared with NV Attribution, internal
resolution of Slinky node identifiers, and test signal injection through normal
production ingestion paths. The SRD explicitly rejects side-channel test
writes.

## Decision

### Label contracts at a glance

NVIDIA Recovery defines separate minimum Loki metadata contracts for training
application logs and node system logs. A deployment may add bounded labels, but
the reference configurations and consumers do not depend on deployment-specific
aliases.

#### Application logs

| Field | Requirement | Meaning |
| --- | --- | --- |
| `cluster` | required stream label | Stable NVIDIA Recovery scheduler-cluster identifier. |
| `appName` | required stream label | Fixed value `slurm-app-log`, following the portable inherited selector. |
| `job_id` | required queryable field | Scheduler job ID and established NVIDIA Recovery consumer attribute. It may move to structured metadata when supported. |
| `filename` | recommended queryable field | Actual source filename or path. Preserve it so Job Manager can match an observation to scheduler-discovered per-attempt log paths. |
| `job_name` | optional | Scheduler job name when the collector can obtain it reliably. |
| `node_name` | optional | Canonical scheduler-visible node when one stream has one known node. |
| `slurm_job_attempt` | optional, not portable | Slurm restart count only when a trusted producer knows it directly. Collectors must not infer or synthesize it. |

The minimum label-form selector is:

```logql
{cluster="customer-slurm", appName="slurm-app-log", job_id="12345"}
```

If `job_id` is stored as Loki structured metadata, the equivalent query starts
with the low-cardinality selector and applies a metadata filter:

```logql
{cluster="customer-slurm", appName="slurm-app-log"} | job_id="12345"
```

#### System logs

| Field | Requirement | Meaning |
| --- | --- | --- |
| `cluster` | required stream label | Stable NVIDIA Recovery scheduler-cluster identifier. |
| `component` | required stream label | Fixed value `journal`, including deployments backed by a conventional syslog file. |
| `node_name` | required stream label | Validated scheduler-visible Slurm `NodeName`. |
| `unit`, `syslog_identifier`, `facility`, `level` | optional | Bounded refinements for deployment-specific queries. |
| `job_id`, `slurm_job_id`, workflow ID, attempt ID | forbidden | System observations belong to a node and time, not a workload. |

The minimum system selector is:

```logql
{cluster="customer-slurm", component="journal", node_name="gpu-001"}
```

Explicit domain selectors prevent Watcher from scanning system streams and
prevent the attribution syslog query from selecting application output.
Existing `app`, `job`, `log`, and filename selectors may be mapped during
migration, but they are not the portable source-domain contract.

### Resolve application attempts outside the required log labels

The inherited file collector extracts `job_id` from filenames but does not
produce `slurm_job_attempt`. Watcher can preserve that field when an upstream
stream already supplies it, but the inherited deployments treat it as optional. Job Manager's
primary attempt-resolution path matches the observed log filename against the
per-attempt `log_file_paths` stored with scheduler execution records.

NVIDIA Recovery therefore does not require `job_attempt` or `slurm_job_attempt` in the
portable application-log contract. Watcher correlates application observations
on `job_id` and preserves the actual filename or path when available. Job
Manager resolves the attempt from scheduler state and the discovered log path.
An optional `slurm_job_attempt` is supporting evidence only when its producer
directly knows the Slurm restart count; a collector must not derive it from the
latest known attempt or current node occupancy.

An E2E test that claims exact-attempt evidence must prove the attempt using the
scheduler execution record, source path, time window, and a unique marker. An
ambiguous application stream may still support job-level detection, but it
cannot by itself prove which attempt emitted the line.

### Keep scheduler job identity out of system logs

`node_name` is the canonical scheduler-visible node identity used in the job
status supplied to NV Attribution. For Slurm, it is the Slurm `NodeName`, not
an assumed Kubernetes node, pod, VM, or collector hostname. Raw labels such as
`host`, `hostname`, `host_name`, or `node` may be retained for diagnostics, but
the deployment mapping must produce `node_name` before the stream crosses the
NVIDIA Recovery/NV Attribution contract.

During migration, the mapping may use the established inherited mechanisms:
collector relabeling, LogQL `label_format`, or compatibility normalization of
`host` or `node` in the Attribution Adapter. Regardless of where it runs, the
result must be validated against scheduler identity. New reference collectors
emit `node_name` directly.

Slinky deployments must resolve Slurm node, compute pod, and Kubernetes or VM
identifiers explicitly as required by SRD REQ-3. A collector must not assert
that its local hostname is the Slurm node name unless the deployment has
validated that equivalence.

Deployment-specific inherited severity or failure-signature filters may be
appended to the base system selector to control volume. They are optional query
policy and do not replace the source and node identity contract or define the
set of failures Attribution can classify.

A node can run multiple workloads, messages can occur between allocations, and
buffered delivery can cross allocation boundaries. Adding a job label would
create false attribution rather than reliable correlation. Attribution instead
correlates system observations with a workload using:

- the workload's scheduler-reported node allocation
- the canonical `node_name` on system streams
- the bounded observation window around the workload and anomaly timestamps

The Attribution Adapter retains the inherited separate `applog_query` and
`syslog_query` configuration boundaries and may retain separate endpoints or
credentials for them. The application query is formatted with `job_id`. The
system query selects the cluster and journal component and preserves
`node_name`; it has no job-ID placeholder. The job allocation and time bounds
are supplied separately to Attribution.

### Inject E2E system fixtures through the node syslog file

An E2E or deployment-qualification test that claims system-log coverage must
write its marker into the target node's configured system-log source and let
the deployed collector deliver it. The Kubernetes and Slinky reference pattern
follows the proven Run:ai QA approach:

1. the fault-injection workload is scheduled on the target compute node and
   receives a writable `hostPath` mount of that node's exact syslog file, such
   as `/var/log/syslog`, at the same path in the workload
2. only the failing rank appends a uniquely identifiable, correctly formatted
   system record, such as a kernel Xid fixture, to that file
3. the independently deployed per-node Alloy instance reads the same host file
   through its normal source configuration
4. Alloy derives the validated scheduler node identity from the collector
   target and attaches it as `node_name`; it does not derive a workload ID from
   the injecting pod
5. Alloy pushes the stream to Loki, and the test waits a bounded interval or
   polls for the unique marker before relying on the observation
6. the workload emits the application-log signature expected by Watcher and
   terminates with the intended failure status, such as 139 for the crash
   fixture
7. the Attribution Adapter queries the system record through the production
   node-and-time selector

The host-file mount is an E2E mechanism, not a production workload contract.
Because it grants a workload permission to modify a host log, it is allowed
only on trusted, disposable test nodes. The mount must expose the exact file,
not the entire `/var/log` directory, and it must not be enabled for arbitrary
or production workloads. The Alloy mount remains independently configured and
read-only. A bare-metal qualification test may instead use `logger` or another
site-approved node-local syslog ingress when that exercises the configured
collector source.

The fixture marker must not encode or attach a Slurm job ID to the system
stream. The test records the target node separately from the message and proves
that the observed `node_name` matches the scheduler allocation. A paired crash
that emits the same application failure without writing a system fixture must
prove that a workload crash alone does not create hardware attribution.

E2E and deployment-qualification tests must not POST a system-log payload to
Loki. Direct Loki pushes bypass the node syslog source, node identity mapping,
Alloy permissions, relabeling, positions, and delivery. Unit tests may use
mocked Loki responses, but a direct push cannot be described as syslog
ingestion or collector coverage.

Application-log tests continue to use the production path appropriate to their
claim. At least one recovery scenario must exercise Slurm output through the
deployed file collector as required by ADR 0005. Focused application-log tests
may retain a narrower boundary only when they state the bypass explicitly.

### Align reference profiles and validate the contract

Implementation of this decision will:

- update the getting-started Alloy example and Watcher values to emit and
  select the inherited `appName` and `job_id` application contract and
  preserve the source filename or path
- add the node system-log pipeline and system selector to the reference logging
  guide
- replace the E2E direct kernel-stream push with a trusted workload that writes
  through an exact-file `hostPath` mount of the target node's syslog, then
  produces the expected application crash and failure status
- remove job labels from E2E system fixtures and assert their absence
- make the legacy BCM deployment either deploy and validate the same node
  system-log path or keep attribution explicitly disabled; an impossible selector is not a functioning
  attribution configuration
- configure Watcher for application streams only and configure the Attribution
  Adapter with distinct application and system selectors
- retain the existing query-reachability checks as a fast first layer, then
  extend dependency checks to require at least one recent application stream
  with `job_id` and one recent system stream with `node_name`

Contract tests will prove both positive and negative boundaries:

- application streams are visible to Watcher and carry job correlation
- exact-attempt test claims are supported by scheduler state, source path, time
  window, and a unique marker rather than a required attempt label
- system streams are visible to the Attribution Adapter and carry canonical
  node correlation
- system streams do not carry job or attempt labels
- Watcher does not select system streams
- a wrong or unmapped node identity fails qualification instead of being
  silently accepted
- a crash without a system fixture does not produce hardware attribution
- breaking the host-file mount, Alloy source, relabel rule, or Loki delivery
  makes the system-log E2E claim fail

## Consequences

The reference deployment gains a clear telemetry boundary: workload logs are
job-scoped and system logs are node-scoped. Operators can map existing Loki
labels once at deployment time instead of changing Watcher or Attribution for
each environment.

Exact attempt identity remains scheduler state rather than a mandatory log
field. Deployments that already emit a trustworthy `slurm_job_attempt` may keep
using it as supporting evidence, while portable collectors preserve the source
path and do not invent a restart count they cannot observe.

`node_name` becomes a cross-component identity contract. Bare-metal Slurm is
usually direct, while Slinky needs explicit identifier resolution and tests.
That work cannot be hidden in a LogQL alias when one Kubernetes node hosts
multiple Slurm compute identities.

The E2E environment becomes more representative but needs a real node syslog
file and an Alloy source with access to it. A writable host-file mount is a
deliberately privileged test facility and limits these scenarios to isolated,
trusted nodes. It avoids granting `/dev/kmsg` access while still exercising the
file source, node labeling, and delivery behavior used by the deployment.

The legacy BCM deployment cannot claim complete NV Attribution input coverage
until it adds and validates system-log collection. Disabling attribution is a truthful degraded
profile; selecting a sentinel stream is not evidence of a working integration.

Existing deployments do not have to rewrite stored historical streams. They
can map current labels at collection or query time during migration. The
deployment-time configurator remains responsible for detecting and explaining
contract gaps.

This decision standardizes metadata and ingestion boundaries, not the set of
system failure signatures, Loki retention policy, or the internal NV
Attribution classification rules.

## Alternatives Rejected

### Require an attempt label on every application stream

The inherited collector can derive `job_id` from a filename but has no
general source for the Slurm restart count. Requiring `job_attempt` would make
the reference contract impossible for otherwise valid file collectors, while
inferring the latest attempt introduces a race with requeue and delayed
delivery. Attempt-specific claims instead use scheduler execution records,
source paths, time bounds, and unique evidence markers.

### Put the active Slurm job ID on every system log

System logs describe a node, may overlap multiple workloads, and can be delayed.
Inferring a job from current occupancy creates incorrect attribution and makes
the collector depend on scheduler state.

### Let Watcher use one broad selector for application and system logs

Watcher implements application-log detection and exact job correlation. A
broad selector scans unrelated system volume, obscures the application input
contract, and makes missing source labels harder to detect.

### Preserve every deployment's existing label names

Inherited environments use incompatible combinations of `appName`, `app`,
`job`, `log`, `component`, `cluster`, `p_cluster`, `service_name`, `host`,
`host_name`, `node`, and `node_name`. Preserving all combinations in consumers
would move deployment mapping into product code and perpetuate the portability
gap.

### Use the collector hostname as `node_name` without validation

This happens to work in some bare-metal deployments but fails when a collector
runs centrally, when a pod name differs from the Slurm node, or when Slinky has
distinct Slurm, pod, and VM identifiers. The canonical value must be validated
against scheduler identity.

### Push E2E system fixtures directly to Loki

This can exercise a Loki query or Attribution parser but cannot prove the node
syslog source, Alloy source, permissions, relabeling, or delivery path. Mocked
responses provide the same focused component coverage without presenting a
side-channel write as production ingestion.

### Parse node identity only from the system-log message body

Message formats vary by distribution, syslog implementation, service, and
vendor. A normalized stream label provides a bounded contract while preserving
the raw message for attribution rules.
