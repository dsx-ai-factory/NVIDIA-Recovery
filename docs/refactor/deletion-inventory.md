# Deletion and Quarantine Inventory

This inventory records inherited surfaces that do not belong in the public
release. It describes private systems by category and does not repeat private
hostnames, project identifiers, credentials, employee data, or deployment
details.

Use the four repository dispositions consistently:

- **drop** removes a surface with no supported public contract.
- **adapt** replaces private assumptions with operator-supplied configuration.
- **quarantine** keeps a surface outside the release pending a decision.
- **port** preserves supported behavior behind a public interface.

## Completed Removals

| Surface | Representative scope | Disposition | Status |
| --- | --- | --- | --- |
| Product-unscoped support services and their orphaned storage helpers | retired service directories and shared quota helpers | drop | removed |
| Private provisioning, deployment-environment, and secret-rotation material | retired infrastructure, environment overlays, and service-account bootstrap paths | drop | removed |
| Inherited CI helper templates and environment substitution tools | obsolete CI and Helm validation helpers | drop | removed |
| Internal agent, access-management, metrics, and unsupported telemetry charts | retired service, dashboard, API, and chart surfaces | drop | removed |
| ADI-to-ARM result queue | ADI default result queue declaration, binding, and configuration | drop | removed; default result publishing and Job Manager consumer retained |
| JM-to-ARM anomaly relay | Watcher forwarding, generated anomaly publication, publisher configuration, and relay telemetry | drop | removed; anomaly persistence, Advice, and direct ADI requests retained |
| On-demand job refresh protocol | Job Manager publisher, request schema, ORM tracking, and queue configuration | drop | removed; stale detection retained; existing broker queues require operator retirement |
| Legacy workflow-controller runtime and deployment modes | retired controller, chart, dashboard, and per-environment deployment artifacts | drop | removed in favor of the Slurm Advisor Controller |
| Internal planning, requirements, acceptance, resilience, and on-call documents | historical planning and operator-only documentation | drop | removed from the release tree |
| Development-only overlays and local environment files | legacy orchestration material, component overlays, and tracked local environment files | drop | removed from the release tree |
| Embedded credentials and private defaults in retained configuration | Helm values, local Compose, dashboards, and service configuration | adapt | replaced with empty, generated, or operator-supplied values |
| Private images, endpoints, cluster names, and employee fixtures in non-code material | documentation, chart defaults, dashboards, and policy examples | adapt or drop | replaced with public examples or removed |
| Proprietary FACT qualification packages and automated consumers | `third_party/fact/`, hosted E2E lanes, and disposable Tilt resources | quarantine | removed from the current tree; retained production adapter source is excluded from default workspace and CI builds |

## Open Or Quarantined Surfaces

| Surface | Representative scope | Disposition | Release condition |
| --- | --- | --- | --- |
| Internal module paths, generated imports, and source comments | Go modules, protocol definitions, generated clients, and application source | adapt | update atomically in a code-authorized follow-up |
| Internal CI and E2E implementation | workflows and executable test/deployment scripts | quarantine | replace private image and infrastructure assumptions and obtain release-automation review |
| Temporary cluster deployment implementation | deployment shell modules and targeting tests | quarantine | replace private bastion, registry, and cluster behavior in a code-authorized follow-up |
| Private authentication provider support | dashboard application and chart integration | quarantine | decide the public authentication contract and obtain security review |
| Corporate identity and notification coupling | remaining application source, generated clients, and fixtures | adapt or drop | use operator-supplied identity and notification configuration |
| Local secret-printing helper | RabbitMQ port-forward helper | quarantine | delete or rewrite without reading or printing credentials |

A public-release review must confirm that every open row has been resolved or
is excluded from the distributed source and artifacts. Repository history was
not rewritten by this scrub.
