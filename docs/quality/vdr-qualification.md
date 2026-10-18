# VDR remediation and qualification

This register follows the reviewed VDR at `a70faf2`, the 21 comment threads,
and the maintainer's subsequent scope decisions. Implementation starts from
`35f8eeaf745d4fbf2fc176ff3f2f239d4644f53e`; it includes the earlier Advice-only,
service-discovery, and Notifications health changes rather than reimplementing
them. Source findings and test results below are not a claim of completed VDR
or production qualification.

The stack was rebased onto `4a4dadfbe87e78cab44e1e03a14bfcd75e43376f`.
Revision references below retain the original test-evidence boundary; the
qualification PR records the rebased heads and their separate validation.

## Evidence levels

- **Reported:** a review observation not yet established in this environment.
- **Source-confirmed:** current implementation establishes the behavior.
- **Reproduced:** a runnable check or controlled runtime demonstrates it.
- **Qualified:** the required installation, platform, and workload acceptance
  checks have passed and their exact revision and artifacts are recorded.

No live VDR, native amd64 stack, or GPU qualification is claimed by this record.
Skipped checks and closed tickets do not count as behavioral closure. Human
review remains required for scheduler actions, authentication, migrations, and
release machinery.

## Comment dispositions

The opaque comment IDs retain traceability to the supplied review without
copying private discussion or test-environment details.

| Comment | Finding | Resolution and evidence boundary |
| --- | --- | --- |
| AAACIPf7ZQg | Review coordination | Resolved administrative request to assign action/no-action dispositions; no product patch. |
| AAACIPf7ZSg | F1.1a/b, F1.3a/b | RabbitMQ requests and limits are 4 CPU/4 GiB. Native renders verify the minimum allocation. No startup validator, scheduler tuning, or many-core reproduction is required. |
| AAACIPf7ZSo | F1.1b, F2.1 | Native/Slinky prerequisites document `AccountingStoreFlags=job_comment`, retaining other flags. Live completed-job comment retention and successive recovery remain to be qualified; historical records are not repaired. |
| AAACIPf7ZSs | F1.3b | Duplicate shared-file ingestion reproduced against both supported log stores. Split node-system and single persistent application reader passes controlled count/lifecycle checks. Full Slinky installation remains pending. |
| AAACIPf7ZS0 | F1.1b | Slurm auth-secret ownership retains the development-setup/no-action disposition. No ownership takeover or automatic deletion added. |
| AAACIPf7ZS4 | F1.5b | Observe rollback suppresses actions; documentation inventories owned holds and explicit operator choices. No automatic or blanket release. |
| AAACIPf7ZS8 | F2.3 | Invalid APS caps retain system fallback with bounded diagnostics. Existing permanent rejection classification abandons the Advice generation; transient/unknown errors retry. No native `--no-requeue` reproduction. |
| AAACIPf7ZTE | F2.2/X2 | Existing SAC mode, acknowledgement, environment, partition and temporary username controls are documented. None bypasses APS/HEIMDALL enrollment; no account selector or automatic enrollment added. |
| AAACIPf7ZTI | F2.3 | Helm now defaults to Advice-only recovery. Unit/harness checks cover delayed Advice and no speculative successor; scenarios 55/57 cover absent caps and initial plus two failed attempts. Cumulative live E2E remains pending. |
| AAACIPf7ZTM | F3 | Enrollment/effective-mode visibility retains no-action disposition; no UI/API feature added. |
| AAACIPf7ZTQ | F3 | Failure-detail UI requires retest with Attribution enabled; no speculative UI redesign. |
| AAACIPf7ZTU | F5 | Readable Attribution evidence requires retest with Attribution enabled; no historical payload rewrite. |
| AAACIPf7ZTY | F5 | Synthetic and Watcher paths persist the original cause before attribution; missing registration and transient Advice storage failures use existing retry paths. Independent affected tests cover failures, repair, original timestamp/provenance and duplicate causal audit suppression. Live closure remains pending. |
| AAACIPf7ZTc | F3.3/F5 | Job Manager default discovery completes earlier Watcher/ADI routing fixes. Arbitrary-release renders pass; all-producer persisted runtime evidence remains required. |
| AAACIPf7ZTg | F3.3 | Decision-time notifications describe issued Advice and link to validated exact-attempt identity, including attempt zero. Source/API/UI checks pass; deployed links and scheduler-not-applied example remain qualification checks. |
| AAACIPf7ZTs | F4 | Intentional crash-keyword matching retained. This is an accepted behavior with limitations, not proof that false-positive risk is eliminated. |
| AAACIPf7ZTw | F4/F5 | XID/ECC/node isolation requires applicable anomaly and diagnostic evidence with Attribution enabled. CPU fixture diagnosis is software-path evidence; hardware isolation requires hardware qualification. |
| AAACIPf7ZT0 | F4.3 | Actual progress markers, phases, overrides, buffering and timing tradeoffs are documented. Detector behavior is preserved; generic silence detection remains out of scope. |
| AAACIPf7ZT4 | F4 | TIMEOUT, OOM, NODE_FAIL and PREEMPTED each require retest. Enabling Attribution does not establish correct treatment of every scheduler state. |
| AAACIPf7ZUE | F5 | Classified permanent rejection terminates generation retries; process-local bounded suppression removes unchanged observe evaluations. Reconciliation continues; restart may reannounce current state once. |
| AAACIPf7ZUI | F5 | Existing dashboard reads are preserved. Indexed whole-job/event-type queries, additive backfill and authenticated generated public reads are implemented with focused storage/transport/UI checks; integrated migrated deployment remains pending. |

## Qualification gates

Use the published values and record the cumulative Git revision, architecture,
release name, image/source revisions, configuration differences and actual
artifacts. A render or HTTP 200 alone does not establish these outcomes.

1. **Attribution:** clean public amd64 build with publicly available pinned
   service/client inputs; readiness and real diagnosis through provider,
   Adapter and ADI. Explicit OFF must make no provider requests. Enabled but
   unavailable must expose operational failure. Public untrusted CI receives
   no private source credentials; its explicit OFF lane cannot satisfy ON.
2. **Installation:** nondefault release name; every audit producer persists an
   event; 4 CPU/4 GiB RabbitMQ allocation; no hidden endpoint or eager-recovery
   corrections. Verify completed-job comments for newly submitted native and
   Slinky jobs, then successive recoveries.
3. **Recovery:** missing job cap, system cap, three consecutive failures with
   no checkpoint and no fourth attempt, checkpoint progress, delayed Advice,
   uncertain scheduler outcomes, and explicit eager compatibility. Permanent
   rejection, failed status publication, and newer generations must retain the
   tested disposition/retry behavior.
4. **Collection:** two node identities, unique markers, legitimate identical
   lines, persistent-reader replacement, rotation, truncation, and sufficient
   settling time to expose delayed duplicates. Planned replacement must not
   overlap shared readers.
5. **Audit:** deploy additive schema before writers, bounded backfill, drain old
   writers, final catch-up, then readers. Compare exact-attempt and whole-job
   results; exercise event type and other filters, tied timestamps, both cursor
   directions, malformed history, authentication denial, cancellation, pages
   larger than 4 KiB and explicit failure above the 4 MiB read bound. Keep
   schema/history on read rollback.
6. **No-action retests:** individually record failure-detail presentation,
   readable Attribution evidence, XID/ECC/isolation, TIMEOUT/OOM/NODE_FAIL and
   PREEMPTED. Preserve residual limitations instead of silently expanding scope.

## Existing evidence and limits

- The immediate implementation layers are dependency ordered below. The
  qualification PR description records published PR heads and current CI
  boundaries; each code revision remains independently inspectable.

| Layer | Original code/decision revision | Source and owning checks |
| --- | --- | --- |
| 1. Default-on Attribution | `476ace30` | [Build](../../scripts/build-images.sh), [Helm contract checks](../../deploy/helm/nvidia-recovery/tests/test_attribution.py), ADR 0048. |
| 2. Installation | `50b7d6e2` | [Resources](../../deploy/helm/nvidia-recovery/values.yaml), [discovery tests](../../deploy/helm/job-manager/tests/test_notifications.py), [operating guidance](../operators/operations.md). |
| 3. Recovery | `25bb1110` | [SAC](../../slurm-advisor-controller/internal/reconcile), [scenario 55](../../tests/e2e/scenarios/55-checkpoint-stop-guardrail), ADR 0045. |
| 4. Collection | `95917d48` | [Application reader](../../scripts/ci/e2e/services/values/alloy-application-config.river), [ingestion/lifecycle scenario](../../tests/e2e/scenarios/02-log-ingestion). |
| 5. Causal evidence | `6f9e9dcb` | [Persistence and repair](../../job_manager/job_manager/storage_utils.py), [periodic tests](../../job_manager/tests/test_periodic_tasks.py), [ingress tests](../../job_manager/tests/test_job_manager.py). |
| 6. Notifications | `6f32a11f` | [Templates](../../notifications/notifications/templates/base.py), [card tests](../../dashboard/src/app/dashboard/notifications/components/NotificationCard.test.tsx). |
| 7. Audit data | `cd40d12c` | [Projection/backfill](../../notifications/notifications/audit), [native database cases](../../notifications/tests/integration/test_database_baselines.py). |
| 8. Public reads | `ddd43521` | [Generated contract](../../api-gateway/protos/heimdall/v1/audit.proto), [transport tests](../../api-gateway/cmd/api-gateway/audit_read_test.go), ADR 0049. |
| 9. Qualification code | `3b9707d9` | [Protected workflow](../../.github/workflows/e2e-attribution.yml), [mode scenario](../../tests/e2e/scenarios/03-attribution-modes), [gateway comparisons](../../tests/e2e_harness/checks.py). |

- Native Helm tests verify Attribution service wiring for ordinary, overlapping
  and long names, explicit OFF/external-provider configuration, missing enabled
  dependencies, amd64 placement, and Job Manager notification/audit overrides.
- The scheduler package passed generated/tidy/lint/vet/unit/race checks. All 27
  SafetyMatrix cases passed on native Linux ARM64; final startup-warning-only
  changes were separately tested. This is not native amd64 qualification.
- Pinned Alloy 1.17.1 runtime reproduced old duplicate ingestion and passed
  controlled replacement/rotation/truncation/count checks against Loki 3.5.3 and
  VictoriaLogs 1.52.0. This used disposable standalone processes, not Slinky.
- An unmodified provider engine at
  `f78746bea7553c11b069d63909ee97f9658b0f8c` processed the checked-in XID 79
  fixture and returned an infrastructure/hardware diagnosis tied to the
  fixture node. The exact generated client parsed the response. This was an
  in-process ARM64 CPU test; it did not test GPU behavior or container readiness.
- Notifications tests and native MySQL 8 fixtures cover additive migration,
  malformed/unmapped identities, mixed old/new writers, final catch-up and
  pagination. Production DDL duration and historical data quality remain
  deployment-specific.
- On the combined layer-8 head, 211 Notifications tests, 99 dashboard tests,
  TypeScript, full Go verification for both modules, and the generated gateway
  binding check passed. Independent causal-evidence review passed 317 tests and
  35 subtests. The complete immediate stack passed all 395 E2E-runner tests.
  These checks do not replace the live gates above.
- The protected mode scenario requires successful ON exchange, bounded explicit
  OFF silence after non-vacuous producer evidence, and structured provider-down
  failure. Scenario 42 supplies the diagnosis/signature assertions. The existing
  authentication smoke checks the internal listener with ServiceAccount tokens;
  external OIDC/JWT remains a separate live qualification gate.
- Four Helm/Tilt combinations rendered for `recovery-qualification`. Existing
  chart limitations remain for names containing multiple component names
  (possible fullname collision) and very long gateway names (the internal
  service suffix can exceed 63 characters). This evidence does not qualify every
  syntactically valid Helm release name or silently change existing resource
  identities.

## Separate follow-up scope

Uncommented defects are delivered separately: stale Watcher evidence and
restart replay, throughput source-time correctness, registry host/container
ports and batch-comment preservation, and retention/teardown/runbook accuracy.
Prioritize stale-evidence workload actions and Loki data retention. Preserve
evidence before teardown and verify resource ownership before cleanup.

UV concurrency/file-limit changes require reproduction of the affected path;
the shared Buildx paths already increase file limits. No such change is
justified by the local evidence. Automatic enrollment, account selection,
contextual crash matching, generic silence detection and Attribution-independent
hardware isolation remain separate product decisions.

The lasting checks are direct published-configuration tests, explicit separation
of detection/diagnosis/Advice/action/outcome, and one recovery-policy owner in
Job Manager. These do not require a new service or a second scheduler policy.
