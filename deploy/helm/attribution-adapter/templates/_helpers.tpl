{{/*
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
*/ -}}
{{/* vim: set filetype=mustache: */}}
{{/*
Expand the name of the chart.
*/}}
{{- define "attribution-adapter.name" -}}
  {{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "attribution-adapter.fullname" -}}
  {{- if .Values.fullnameOverride -}}
    {{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
  {{- else -}}
    {{- $name := default .Chart.Name .Values.nameOverride -}}
    {{- if contains $name .Release.Name -}}
      {{- .Release.Name | trunc 63 | trimSuffix "-" -}}
    {{- else -}}
      {{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
    {{- end -}}
  {{- end -}}
{{- end -}}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "attribution-adapter.chart" -}}
  {{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create the name of the service account to use
*/}}
{{- define "attribution-adapter.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "attribution-adapter.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "attribution-adapter.labels" -}}
app.kubernetes.io/full-name: {{ include "attribution-adapter.fullname" . }}
helm.sh/chart: {{ include "attribution-adapter.chart" . }}
{{ include "attribution-adapter.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "attribution-adapter.selectorLabels" -}}
app.kubernetes.io/name: {{ include "attribution-adapter.name" . }}
{{- if (default .Values.global.enableExtendedLabelSelectors .Values.enableExtendedLabelSelectors)}}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}
{{- end }}

{{/*
Fully qualified container image reference. Precedence for the registry
prefix: chart-local image.repository, else global.registry (shared
across all NVIDIA Recovery subcharts). Neither ships a default; an empty
registry yields a bare "name:tag" reference.
*/}}
{{- define "attribution-adapter.image" -}}
{{- $registry := default ((.Values.global).registry) .Values.image.repository -}}
{{- $tag := default (default $.Chart.Version ((.Values.global).appVersion)) .Values.image.tag -}}
{{- if $registry -}}
{{- printf "%s/%s:%s" $registry .Values.image.name $tag -}}
{{- else -}}
{{- printf "%s:%s" .Values.image.name $tag -}}
{{- end -}}
{{- end -}}

{{/*
Reject retired connection and tenant locations instead of silently ignoring them.
*/}}
{{- define "attribution-adapter.validateLogQueryLocations" -}}
{{- range $key := list "applog_query" "syslog_query" "epilogue_query" "victorialogs_applog_query" "victorialogs_syslog_query" "victorialogs_epilogue_query" "logql_json_filter" "logql_syslog_index_filter" "logql_kernel_filter" "logql_applog_exclude_filter" "logql_prolog_exclude_filter" "logql_epilog_exclude_filter" "logql_applog_format" -}}
{{- if hasKey $.Values $key -}}
{{- fail (printf "%s is retired; configure native templates in global.log_query.providers.<backend>.queries.attribution; unused filter fragments were removed" $key) -}}
{{- end -}}
{{- end -}}
{{- $settings := .Values.loki | default dict -}}
{{- range $key := list "loki_url" "loki_app_url" "loki_sys_url" "loki_tenants" "loki_app_tenants" "loki_sys_tenants" "tenants" "org_id" "use_x_scope_org_id" "x_scope_org_id" -}}
{{- if get $settings $key -}}
{{- fail (printf "loki.%s is retired; configure connection and tenant identity in global.log_query.providers.loki.application/system" $key) -}}
{{- end -}}
{{- end -}}
{{- if or .Values.use_x_scope_org_id .Values.x_scope_org_id -}}
{{- fail "use_x_scope_org_id and x_scope_org_id are retired; configure org_id in global.log_query.providers.loki.application/system" -}}
{{- end -}}
{{- end -}}

{{- define "attribution-adapter.logQueryBackend" -}}
{{- $logQuery := ((.Values.global).log_query) | default dict -}}
{{- $logQuery.backend | default "loki" -}}
{{- end -}}

{{- define "attribution-adapter.providerValue" -}}
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

{{/* Match the sibling provider's default fullname. Custom names/ports need fact.url. */}}
{{- define "attribution-adapter.factURL" -}}
{{- if hasKey .Values.fact "url" -}}
{{- required "fact.url must not be empty; omit it to use the same-release provider" (.Values.fact.url | trim) -}}
{{- else -}}
{{- printf "http://%s-attribution-service:8000" (.Release.Name | trunc 43 | trimSuffix "-") -}}
{{- end -}}
{{- end -}}
