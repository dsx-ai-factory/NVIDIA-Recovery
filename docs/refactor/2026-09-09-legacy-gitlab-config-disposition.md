# Legacy deployment configuration disposition

This note records the GitLab configuration retirement for the legacy BCM
helper, now maintained outside this repository. The helper is provider-specific;
the public installation path is
[getting started](../getting-started.md) with Attribution disabled by default.

| Surface | Disposition | Result |
| --- | --- | --- |
| Legacy deployment source-control-hosted registry and chart defaults | adapt | Explicit operator registry, image repositories, image tags/digests, OCI chart repository, and local FACT chart archive; missing inputs fail before target lookup |
| Legacy deployment provider-specific pull-secret and staging names | adapt | Configurable pull-secret reference with a neutral default and neutral staging filename; existing cluster secrets are neither renamed nor deleted |
| ADI Compose obsolete source-control build arguments and build-secret wiring | drop | Removed unused build configuration; the current Dockerfile does not consume it |
| Dashboard provider-specific ignored report names | drop | Removed obsolete provider report patterns; generic build-artifact patterns retained |
| Grafana documentation provider-specific CI job | adapt | Provider-neutral invocation using the existing CLI |

Validation is limited to isolated shell fixtures, shell syntax/lint checks,
and scoped diff review. The fixtures use non-resolving `example.invalid`
registries and a temporary FACT chart stub with fake Helm/SSH. They do not
qualify chart/image availability or recovery behavior. Existing scheduler
commands, action-mode settings, checksum gates, and rollback behavior remain.
No deployment or live scheduler command, secret scan, credential cleanup,
secret deletion, or rotation was performed. Other cluster-specific host,
network, and filesystem assumptions remain outside this configuration-only
change and retain their prior disposition.
