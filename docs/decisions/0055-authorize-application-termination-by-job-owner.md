# ADR 0055: Authorize Application Termination By Job Owner

Status: accepted

Date: 2026-11-02

## Context

[ADR 0054](0054-persist-application-requested-job-termination.md) lets an
application request cancellation using only its scheduler job ID, including
before SAC registers the job. Authenticating a caller does not establish that
the caller owns that job. A user who knows another user's job ID must not be
able to cancel it or prevent its owner from submitting a legitimate request.

SAC reports Slurm's `slurm_user`, which Job Manager stores as `user`. Gateway
already obtains an authenticated identity from verified JWT or OIDC claims.
Those values are not automatically equivalent: an opaque token `sub` or a
shared service account does not identify a Slurm owner.

## Decision

Require a verified JWT/OIDC caller identity whose configured subject claim
matches the scheduler username exactly. Operators must establish that mapping
with their trusted issuer before enabling this operation. Do not trim, fold
case, strip a domain, trust caller-provided usernames, or infer ownership from
job IDs. Missing identity is unauthenticated; Kubernetes service accounts and
unsupported identity mechanisms are denied. The mutation remains unavailable
on anonymous/insecure Gateway listeners.

Gateway derives `requested_by` from its authenticated request context and
passes it in the internal Job Manager request. The public request continues to
contain only job ID, desired state, and optional reason. Job Manager's internal
API, MCP exposure, and scheduler-ingestion endpoints must be reachable only by
trusted producers. This decision relies on that existing deployment boundary;
it does not introduce per-user trust in a public Job Manager endpoint. Shared
application tokens require a separate delegated-identity or job-credential
design before they can represent an individual job owner.

Job Manager persists one intention per `(job_id, requested_by)`, using a
case-sensitive requester key. The first request for that pair keeps its
original reason and time. This partially supersedes ADR 0054's job-ID-only
idempotency rule. Ownership decisions are:

| Situation | API result | Durable outcome | Recovery effect |
| --- | --- | --- | --- |
| Registered job; requester matches owner | HTTP 200 | `AUTHORIZED` | Eligible for termination reconciliation |
| Registered job; requester differs | HTTP 403 | No new request | Normal recovery continues |
| Registered job; owner missing or `unknown` | Ownership unavailable error | No new request | Normal recovery continues |
| Job not registered | HTTP 200 | `PENDING` | No cancellation or recovery suppression |
| Pending request; registration supplies matching owner | Read returns `AUTHORIZED` | Persist authorization | Reconcile current and successor attempts |
| Pending request; registration supplies another owner | Read returns `REJECTED` | `REQUESTER_NOT_JOB_OWNER` | Normal recovery continues |

HTTP 200 acknowledges durable receipt; it does not promise authorization or
scheduler cancellation. Responses include `authorization_status` and an
optional `reason_code`. Applications inspect their own request through
`GET /v1/application-intentions/{job_id}` using the same authenticated identity.
This read returns 404 for callers with no request, even if another caller has
one. It does not expose the actual job owner's identity. A pending request
remains pending when no authoritative owner is available. No expiry is added
in this phase.

After registration, Job Manager resolves pending requests under transaction
locks. A rejected request remains rejected. The real owner's request occupies
a separate row and remains eligible even when someone else submitted first.
Only an authorized request whose requester still matches the observed attempt
owner may create terminal Advice or suppress late restart Advice. Registration
and attribution paths use the same guard, including HTTP and RabbitMQ job
updates. Request acceptance and authorization never wait for SAC cancellation.

Resolved requests emit best-effort audit events against the latest known
attempt: `application_termination_requested` for authorized requests and
`application_termination_rejected` for denied pending requests. Include the
verified requester, authorization outcome, and safe rejection reason. Claim
each request's audit association once; subsequent polls do not duplicate it.
Existing Advice and scheduler-action events retain their exact-attempt scope.

This extends `application_intentions` with requester, authorization status,
and rejection reason through an additive Alembic migration. Existing rows lack
verified identity and remain non-actionable; when ownership is known they are
rejected. Existing terminal Advice or scheduler actions cannot be undone by
this migration. Deploy the migration before the updated Job Manager and Gateway.

SAC continues to act only on Advice under its existing safety checks. Authorized
parent termination keeps ADR 0011's DAG propagation semantics; descendants do
not need separate application requests. Recovery configuration and DAG policy
remain owned by Job Manager.

## Consequences

Unregistered jobs retain asynchronous request acceptance while cancellation
waits for verified ownership. Applications must inspect pending requests to
learn whether authorization was granted. A failed ownership lookup must never
produce cancellation Advice; existing attribution retry handling continues to
protect against competing restart Advice when intention storage is unavailable.

The deployment requires a trustworthy identity-to-scheduler-user mapping and
restricted access to Job Manager and scheduler ingestion. Direct use of a
shared service token is insufficient to prove an individual user's ownership.
No administrator override, identity-directory integration, job credential
issuance, retention worker, or public Job Manager authentication is added here.
Scheduler job-ID reuse retains the limitation recorded by ADR 0054; matching
ownership alone does not distinguish two logical jobs owned by the same user.

Confirmation includes signed-token identity tests on HTTP/gRPC, Job Manager
authorization guards, and real-MySQL tests for pending decisions, denied users,
case-sensitive idempotency, concurrent duplicates, and legacy migration.

## Alternatives Rejected

- **Reject every unregistered job.** Simpler, but removes the early-request
  behavior required by ADR 0054.
- **Keep one pending row per job ID.** An unauthorized requester could claim
  that slot before the owner and block legitimate cancellation.
- **Trust a username in the public payload.** A caller could impersonate the
  owner without proving that identity.
- **Compare every credential's subject with the owner.** A service-account
  subject or opaque issuer ID has no established Slurm username mapping.
