{{/*
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
*/ -}}
{{/* vim: set filetype=mustache: */}}
{{- define "nvidia-recovery.logQueryBackend" -}}
{{- $logQuery := ((.Values.global).log_query) | default dict -}}
{{- $logQuery.backend | default "loki" -}}
{{- end -}}

{{- define "nvidia-recovery.logQueryProviderValue" -}}
{{- $root := index . 0 -}}
{{- $backend := index . 1 -}}
{{- $domain := index . 2 -}}
{{- $key := index . 3 -}}
{{- $logQuery := (($root.Values.global).log_query) | default dict -}}
{{- $providers := $logQuery.providers | default dict -}}
{{- $provider := get $providers $backend | default dict -}}
{{- $domainConfig := get $provider $domain | default dict -}}
{{- if hasKey $domainConfig $key -}}
{{- get $domainConfig $key -}}
{{- end -}}
{{- end -}}

{{/*
OpenTelemetry environment variables shared by Heimdall application
containers. The token variable is intentionally declared before the headers
variable so Kubernetes expands it in OTEL_EXPORTER_OTLP_HEADERS.
The slurm-advisor-controller chart keeps a copy for standalone releases; change
both together (deploy/helm/scripts/check-otel-env-parity.sh enforces it).
*/}}
{{- define "heimdall.otelEnv" -}}
{{- $otelcollector := ((.Values.global).otelcollector) | default dict -}}
{{- if ($otelcollector.enabled | default false) }}
- name: OTEL_EXPORTER_OTLP_PROTOCOL
  value: {{ required "global.otelcollector.protocol is required when global.otelcollector.enabled=true" $otelcollector.protocol | quote }}
- name: OTEL_EXPORTER_OTLP_ENDPOINT
  value: {{ required "global.otelcollector.endpoint is required when global.otelcollector.enabled=true" $otelcollector.endpoint | quote }}
{{- $auth := $otelcollector.auth | default dict }}
{{- if ($auth.enabled | default false) }}
- name: OTEL_EXPORTER_OTLP_TOKEN
  valueFrom:
    secretKeyRef:
      name: {{ required "global.otelcollector.auth.secretName is required when global.otelcollector.auth.enabled=true" $auth.secretName | quote }}
      key: {{ required "global.otelcollector.auth.secretKey is required when global.otelcollector.auth.enabled=true" $auth.secretKey | quote }}
- name: OTEL_EXPORTER_OTLP_HEADERS
  value: "Authorization=Bearer $(OTEL_EXPORTER_OTLP_TOKEN)"
{{- end }}
{{- end }}
{{- end }}
