### Dependency Checker: Architecture and Tests

This document describes the companion dependency checker included in the NVIDIA Recovery Helm chart. It continuously verifies connectivity and readiness of core external dependencies and exposes the results as Prometheus textfile metrics for scraping.

### What it does
- Periodically runs lightweight database, selected log-query backend,
  Prometheus, Slack, and dashboard FQDN checks.
- Emits per-test status and a summary to a textfile directory consumed by a node-exporter sidecar.
- Lets Prometheus scrape results either via pod annotations or the optional PodMonitor.

### High-level architecture
- Deployment: `heimdall-dependency-checker` (gated by `dependency_checker.enabled`).
  - Sidecar `node-exporter` with `--collector.textfile` enabled; exposes a `metrics` port.
  - Container `checker` runs shell scripts from a ConfigMap in a configurable loop interval.
- ConfigMap: `dependency-checker-scripts` provides the test scripts and helpers. Scripts are sourced from the chart at `deploy/helm/nvidia-recovery/files/checker-scripts` and bundled via Helm `.Files.Glob`.
- Volumes: a shared `emptyDir` for the textfile directory, and a ConfigMap volume for scripts.
- Scraping: pod annotations enable scraping on `metrics_port`; an optional `PodMonitor` can also be enabled.

Code references
```yaml
{{- if .Values.dependency_checker.enabled }}
apiVersion: apps/v1
kind: Deployment
metadata:
  name: heimdall-dependency-checker
  namespace: {{ .Release.Namespace }}
spec:
  replicas: 1
  template:
    metadata:
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "{{ .Values.dependency_checker.metrics_port }}"
      labels:
        app.kubernetes.io/name: dependency-checker
    spec:
      containers:
      - name: node-exporter
```

```yaml
- name: node-exporter
  image: {{ .Values.dependency_checker.node_exporter_image.repository }}:{{ .Values.dependency_checker.node_exporter_image.tag }}
  args:
  - --web.listen-address=:{{ .Values.dependency_checker.metrics_port }}
  - --web.disable-exporter-metrics
  - --collector.disable-defaults
  - --collector.textfile
  - --collector.textfile.directory={{ .Values.dependency_checker.textfile_dir }}
  ports:
  - name: metrics
    containerPort: {{ .Values.dependency_checker.metrics_port }}
  volumeMounts:
  - name: textfile-dir
    mountPath: {{ .Values.dependency_checker.textfile_dir }}
```

```yaml
- name: checker
  image: {{ .Values.dependency_checker.image.repository }}:{{ .Values.dependency_checker.image.tag | default (default $.Chart.Version .Values.global.appVersion) }}
  command: ["/bin/bash","-lc"]
  args:
  - |
    chmod +x /scripts/*.sh || true;
    while true; do /scripts/run.sh; sleep {{ .Values.dependency_checker.schedule_seconds }}; done
```

### Configuration (values)
Native probes live in the selected global provider; scheduling and credentials
remain under `dependency_checker`:

```yaml
# Dependency checker companion app (separate pod)
global:
  log_query:
    providers:
      loki:
        queries:
          health:
            # Override for deployment-specific labels; no workload placeholders.
            application: '{appName="slurm-app-log"}'
            system: '{component="journal"}'
      victorialogs:
        queries:
          health:
            application: '_stream:{probe="dependency-checker-app"}'
            system: '_stream:{probe="dependency-checker-system"}'
dependency_checker:
  enabled: false
  image:
    repository: <operator-supplied-image>
    tag: v1.0.0
    pullPolicy: IfNotPresent
  imagePullSecrets:
  - name: regcred
  schedule_seconds: 60
  resources: {}
  extraEnv: []
  extraVolumeMounts: []
  extraVolumes: []
  # Node exporter sidecar configuration for metrics exposure
  node_exporter_image:
    repository: quay.io/prometheus/node-exporter
    tag: v1.8.1
    pullPolicy: IfNotPresent
  metrics_port: 9100
  textfile_dir: /var/node_exporter/textfile
  log_query:
    recent_window_seconds: 300
    loki:
      tokenSecretRef:
        name: loki-token
        key: token
      appTokenSecretRef:
        name: ""
        key: token
      sysTokenSecretRef:
        name: ""
        key: token
    victorialogs:
      tokenSecretRef:
        name: ""
        key: token
      appTokenSecretRef:
        name: ""
        key: token
      sysTokenSecretRef:
        name: ""
        key: token
      usernameSecretRef:
        name: ""
        key: username
      passwordSecretRef:
        name: ""
        key: password
      appUsernameSecretRef:
        name: ""
        key: username
      appPasswordSecretRef:
        name: ""
        key: password
      sysUsernameSecretRef:
        name: ""
        key: username
      sysPasswordSecretRef:
        name: ""
        key: password
      caCertSecretRef:
        name: ""
        key: ca.crt
      clientCertSecretRef:
        name: ""
        key: tls.crt
      clientKeySecretRef:
        name: ""
        key: tls.key
```

Environment sources and secrets:
- Database: derives from `global.mysql.secret.*` and `mysql.*` if present.
- Log query: `global.log_query.backend` selects `loki` or `victorialogs`.
  Connection and application/system tenant values come from
  `global.log_query.providers`; credentials use the provider-specific
  `dependency_checker.log_query` SecretRefs. VictoriaLogs accepts anonymous,
  bearer-token, or complete Basic-auth credentials plus optional CA and paired
  client-certificate/key Secrets. Bearer and Basic modes are mutually
  exclusive for each effective domain. Application/system SecretRefs override
  the legacy shared SecretRefs; omitted domain values fall back to shared
  credentials. Loki supports the same shared-token fallback for its distinct
  application/system token Secrets.
- Both query-domain URLs must be explicit in the selected provider block,
  including test deployments. Neither URLs nor tenant identity fall back to
  legacy values or another domain. `global.loki.*` is provisioning/ingestion
  configuration only. Selecting VictoriaLogs does not enable, disable, or
  redirect bundled Loki or any collector.
- Prometheus: `global.prometheus.*` and `heimdall-prometheus-token` Secret.
- Slack: `notifications.app.slackEnabled/slackChannel` and `nvidia-recovery-slack-token` Secret.
- FQDN: `dashboard.ingress_fqdn` and fixed `FQDN_PATH=/mission-control`.

Code references
```yaml
# DB settings default to MySQL subchart/global settings
- name: DB_HOST
  value: "{{ default "nvidia-recovery-mysql" (default (default "" .Values.global.mysql.secret.host) (default "" .Values.mysql.host)) }}"
- name: DB_PORT
  value: "{{ default 3306 (default "" .Values.mysql.port) }}"
- name: DB_USER
  value: "{{ default "root" (default "" .Values.global.mysql.secret.user) }}"
- name: DB_NAME
  value: "{{ default "heimdall" (default "" .Values.global.mysql.secret.db) }}"
- name: DB_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ default "nvidia-recovery-mysql" (default "" .Values.global.mysql.secret.name) }}
      key: {{ default "mysql-root-password" (default "" .Values.global.mysql.secret.key) }}
```

```yaml
- name: LOG_QUERY_BACKEND
  value: "{{ .Values.global.log_query.backend }}"
- name: VICTORIALOGS_APP_URL
  value: "{{ .Values.global.log_query.providers.victorialogs.application.url }}"
- name: VICTORIALOGS_APP_ACCOUNT_ID
  value: "{{ .Values.global.log_query.providers.victorialogs.application.account_id }}"
```

```yaml
- name: PROM_URL
  value: "{{ default "" .Values.global.prometheus.prometheus_url }}"
- name: PROM_TENANT
  value: "{{ default "" .Values.global.prometheus.prometheus_tenants }}"
- name: PROM_TOKEN
  valueFrom:
    secretKeyRef:
      name: heimdall-prometheus-token
      key: token
- name: PROM_CLUSTER
  value: "{{ default "" .Values.global.prometheus.prometheus_cluster }}"
```

```yaml
- name: SLACK_CHANNEL
  value: "{{ default "" (default "" .Values.notifications.app.slackChannel) }}"
- name: SLACK_ENABLED
  value: "{{ if and .Values.notifications .Values.notifications.app .Values.notifications.app.slackEnabled }}true{{ else }}false{{ end }}"
- name: SLACK_TOKEN
  valueFrom:
    secretKeyRef:
      name: nvidia-recovery-slack-token
      key: token
```

```yaml
- name: FQDN_HOST
  value: "{{ default "" .Values.dashboard.ingress_fqdn }}"
- name: FQDN_PATH
  value: "/mission-control"
- name: TEXTFILE_DIR
  value: "{{ .Values.dependency_checker.textfile_dir }}"
volumeMounts:
- name: checker-scripts
  mountPath: /scripts
- name: textfile-dir
  mountPath: {{ .Values.dependency_checker.textfile_dir }}
```

### Test suite
The checker executes modular shell scripts (packaged into the ConfigMap from `files/checker-scripts`) each run cycle.

- db: MySQL connectivity and simple query.
- log-query: Runs the dedicated application/system health queries configured
  under `dependency_checker.log_query.<provider>.*_health_query`, plus the
  selected Watcher query prefix when one is configured. It never reuses
  Attribution workload templates (which may contain placeholders such as
  `{job_id}`).
  Protocol, response, and tenant validation determine query health. Recent
  matching data is reported separately, so a successful empty response is
  healthy without claiming ingestion freshness.
- prometheus: Runs a basic `up` query.
- slack: Validates Slack token using `auth.test` when Slack notifications are enabled.
- fqdn: GET request to the dashboard FQDN path.

Scripts source in ConfigMap
```yaml
data:
  {{- $root := . }}
  {{- range $path, $_ := .Files.Glob "files/checker-scripts/*" }}
  {{ base $path }}: |-
{{ $root.Files.Get $path | nindent 4 }}
  {{- end }}
```

### Metrics exposed
All metrics are written as Prometheus textfile format under `TEXTFILE_DIR` and scraped by the node-exporter.
- dependency_checker_last_run_timestamp_seconds
- dependency_checker_tests_total
- dependency_checker_passed_tests
- dependency_checker_failed_tests
- `dependency_checker_test_status{test="db|log_query|prometheus|slack|fqdn"}`
- `dependency_checker_log_query_health{backend}`
- `dependency_checker_log_query_query_health{backend,source}`
- `dependency_checker_log_query_recent_data{backend,source}`
- `dependency_checker_log_query_http_code{backend,source}`

The legacy `dependency_checker_loki_*` series continue while Loki is selected
for one compatibility window. Their per-query status retains the historical
recent-data meaning; use the backend-neutral health series for availability.

Per-test emissions are written into separate files (for example,
`dependency_checker_db.prom` and `dependency_checker_log_query.prom`), and
`dependency_checker_summary.prom` aggregates the run.

### Scraping
- Pod annotations enable scraping on the named `metrics` port by default.
- Optional PodMonitor can be enabled with `global.prometheus.pod_monitor_enabled: true` to scrape pods with label `app.kubernetes.io/name` and ports `metrics` and `8080`.

Code reference
```yaml
{{ if .Values.global.prometheus.pod_monitor_enabled }}
apiVersion: monitoring.coreos.com/v1
kind: PodMonitor
metadata:
  labels:
    release: kube-prometheus-stack
```

### How to enable
1. In values, set `dependency_checker.enabled: true`.
2. Ensure required secrets are present:
   - MySQL (`global.mysql.secret.*` or subchart-provided secret)
   - Prometheus (`heimdall-prometheus-token` with `token` key)
   - the selected log-query provider Secret configured in
     `dependency_checker.log_query`
   - Slack (`nvidia-recovery-slack-token` with `token` key) if notifications are enabled
3. Optionally enable `global.prometheus.pod_monitor_enabled: true` for PodMonitor-based scraping.
4. Adjust `schedule_seconds`, `metrics_port`, and `textfile_dir` if needed.

### Notes
- Native probes come from the dedicated
  `global.log_query.providers.<backend>.queries.health.application` and
  `.system` values. The checker may also probe `queries.watcher.query_prefix`
  when Watcher is enabled for log-query polling and that prefix is configured;
  it does not reuse Attribution workload query templates. Old component-local
  health-query settings are rejected.
- The checker auto-installs minimal utilities (`mysql-client`, `curl`, `jq`,
  `coreutils`) when available via apt; set `DEPCHK_SKIP_INSTALL=1` to skip.
- A VictoriaLogs production rollout still requires separately qualified
  ingestion, canonical fields, body/timestamp preservation, retention, and
  causal E2E evidence. VictoriaLogs 1.39.0 or newer is required for the shared
  time-window and tenant-response semantics. Passing this query check alone
  does not qualify the server version or those ingestion properties.

### Contributing new checks
- Add a new executable `.sh` file under `deploy/helm/nvidia-recovery/files/checker-scripts`.
- The ConfigMap will automatically include it with the key equal to the base filename.
- Reference it from `run.sh` (also in `files/checker-scripts`) if it should be part of the periodic suite.
