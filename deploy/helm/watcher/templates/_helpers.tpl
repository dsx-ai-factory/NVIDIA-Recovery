{{/*
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
*/ -}}
{{/* vim: set filetype=mustache: */}}
{{/*
Expand the name of the chart.
*/}}
{{- define "watcher.name" -}}
  {{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "watcher.fullname" -}}
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
{{- define "watcher.chart" -}}
  {{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Get KubeVersion removing pre-release information.
*/}}
{{- define "kubeVersion" -}}
  {{- default .Capabilities.KubeVersion.Version (regexFind "v[0-9]+\\.[0-9]+\\.[0-9]+" .Capabilities.KubeVersion.Version) -}}
{{- end -}}

{{/*
Return the appropriate apiVersion for ingress.
*/}}
{{- define "ingress.apiVersion" -}}
  {{- if and (.Capabilities.APIVersions.Has "networking.k8s.io/v1") (semverCompare ">= 1.19.x" (include "kubeVersion" .)) -}}
      {{- print "networking.k8s.io/v1" -}}
  {{- else if .Capabilities.APIVersions.Has "networking.k8s.io/v1beta1" -}}
    {{- print "networking.k8s.io/v1beta1" -}}
  {{- else -}}
    {{- print "extensions/v1beta1" -}}
  {{- end -}}
{{- end -}}


{{/*
Return if ingress is stable.
*/}}
{{- define "ingress.isStable" -}}
  {{- eq .Values.ingress.apiVersion "networking.k8s.io/v1" -}}
{{- end -}}

{{/*
Return if ingress supports ingressClassName.
*/}}
{{- define "ingress.supportsIngressClassName" -}}
  {{- or (eq (include "ingress.isStable" .) "true") (and (eq (include "ingress.apiVersion" .) "networking.k8s.io/v1beta1") (semverCompare ">= 1.18.x" (include "kubeVersion" .))) -}}
{{- end -}}

{{/*
Return if ingress supports pathType.
*/}}
{{- define "ingress.supportsPathType" -}}
  {{- or (eq (include "ingress.isStable" .) "true") (and (eq (include "ingress.apiVersion" .) "networking.k8s.io/v1beta1") (semverCompare ">= 1.18.x" (include "kubeVersion" .))) -}}
{{- end -}}

{{/*
Create the name of the service account to use
*/}}
{{- define "watcher.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "watcher.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "watcher.labels" -}}
helm.sh/chart: {{ include "watcher.chart" . }}
app.kubernetes.io/full-name: {{ include "watcher.fullname" . }}
{{ include "watcher.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "watcher.selectorLabels" -}}
app.kubernetes.io/name: {{ include "watcher.name" . }}
{{- if (default .Values.global.enableExtendedLabelSelectors .Values.enableExtendedLabelSelectors)}}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}
{{- end }}

{{/*
Validate provider-only query connections. Bundled storage and ingestion values
never supply query URLs or tenant identity.
*/}}
{{- define "watcher.validateLogQueryConnection" -}}
{{- $logQuery := ((.Values.global).log_query) | default dict -}}
{{- $providers := $logQuery.providers | default dict -}}
{{- $backend := include "watcher.logQueryBackend" . -}}
{{- $datasources := ((.Values.config).datasources) | default dict -}}
{{- range $name := list "loki" "log_query_policy" -}}
{{- $policy := get $datasources $name | default dict -}}
{{- if hasKey $policy "query_prefix" -}}
{{- fail (printf "Move config.datasources.%s.query_prefix to global.log_query.providers.%s.queries.watcher.query_prefix" $name $backend) -}}
{{- end -}}
{{- range $key := list "url" "tenants" "org_id" "x_scope_org_id" "account_id" "project_id" -}}
{{- if and (hasKey $policy $key) (ne (toString (get $policy $key)) "") -}}
{{- fail (printf "Move config.datasources.%s.%s to global.log_query.providers.%s.application" $name $key $backend) -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- range $key := list "loki_tenants" "loki_x_scope_org_id" -}}
{{- if get $.Values $key -}}
{{- fail (printf "Move %s to global.log_query.providers.loki.application" $key) -}}
{{- end -}}
{{- end -}}
{{- $localLogQuery := get $datasources "log_query" | default dict -}}
{{- $localProviders := $localLogQuery.providers | default dict -}}
{{- range $name, $provider := $localProviders -}}
{{- if or (hasKey $provider "query_prefix") (hasKey $provider "queries") -}}
{{- fail (printf "Move config.datasources.log_query.providers.%s query templates to global.log_query.providers.%s.queries.watcher" $name $name) -}}
{{- end -}}
{{- range $key := list "url" "tenants" "org_id" "account_id" "project_id" "require_tenant" -}}
{{- if and (hasKey $provider $key) (ne (toString (get $provider $key)) "") -}}
{{- fail (printf "Move config.datasources.log_query.providers.%s.%s to global.log_query.providers.%s.application" $name $key $name) -}}
{{- end -}}
{{- end -}}

{{- end -}}
{{- if ne ((.Values.config).event_source | default "log_query") "rabbitmq" -}}
{{- $provider := get $providers $backend | default dict -}}
{{- $application := $provider.application | default dict -}}
{{- $_ := required (printf "global.log_query.providers.%s.application.url is required for log-query polling" $backend) $application.url -}}
{{- end -}}
{{- end -}}


{{/* Check workflow-local native queries even when the workflow is disabled. */}}
{{- define "watcher.validateWorkflowQueryLocations" -}}
{{- range $workflow := . -}}
{{- $attributes := $workflow.attributes | default dict -}}
{{- $query := $attributes.query | default dict -}}
{{- if or (hasKey $query "loki") (hasKey $query "victorialogs") (hasKey $attributes "nooutput_validation_query") -}}
{{- fail (printf "Move workflow %s native query templates to global.log_query.providers.<backend>.queries.watcher.workflows.%s" $workflow.name $workflow.name) -}}
{{- end -}}
{{- include "watcher.validateWorkflowQueryLocations" ($workflow.children | default list) -}}
{{- end -}}
{{- end -}}

{{/* Enabled polling workflows must have templates for the selected provider. */}}
{{- define "watcher.validateWorkflowNativeQueries" -}}
{{- $templates := .templates -}}
{{- range $workflow := .workflows -}}
{{- if has $workflow.type (list "log_query" "loki_query" "nooutput") -}}
{{- $native := get $templates $workflow.name | default dict -}}
{{- $_ := required (printf "global.log_query.providers.%s.queries.watcher.workflows.%s.query is required" $.backend $workflow.name) $native.query -}}
{{- if eq $workflow.type "nooutput" -}}
{{- $validation := required (printf "Provider nooutput_validation_query is required for workflow %s" $workflow.name) $native.nooutput_validation_query -}}
{{- if not (contains "{job_id}" $validation) -}}
{{- fail (printf "Provider nooutput_validation_query for %s must contain {job_id}" $workflow.name) -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- include "watcher.validateWorkflowNativeQueries" (dict "workflows" ($workflow.children | default list) "templates" $templates "backend" $.backend) -}}
{{- end -}}
{{- end -}}

{{/* The selector is deliberately independent from global.loki.bundled. */}}
{{- define "watcher.logQueryBackend" -}}
{{- $logQuery := ((.Values.global).log_query) | default dict -}}
{{- $logQuery.backend | default "loki" -}}
{{- end -}}

{{/*
RabbitMQ publisher host. Required when event_source is "rabbitmq"; passes
through the configured value for "log_query" and its legacy "loki" alias so
the field is not forced on query-only installs.
*/}}
{{- define "watcher.rabbitmqHost" -}}
{{- $host := (((((.Values.config).datasources).rabbitmq).publisher).host) | default "" -}}
{{- if eq ((.Values.config).event_source | default "rabbitmq") "rabbitmq" -}}
{{- required "config.datasources.rabbitmq.publisher.host must be set when event_source is rabbitmq" $host -}}
{{- else -}}
{{- $host -}}
{{- end -}}
{{- end -}}

{{/*
Effective image tag: chart-local image.tag, else global.appVersion, else the
chart's own version.
*/}}
{{- define "watcher.imageTag" -}}
{{- default (default $.Chart.Version ((.Values.global).appVersion)) .Values.image.tag -}}
{{- end -}}

{{/*
Fully qualified container image reference. Precedence for the registry
prefix: chart-local image.repository, else global.registry (shared
across all NVIDIA Recovery subcharts). Neither ships a default; an empty
registry yields a bare "name:tag" reference.
*/}}
{{- define "watcher.image" -}}
{{- $registry := default ((.Values.global).registry) .Values.image.repository -}}
{{- $tag := include "watcher.imageTag" . -}}
{{- if $registry -}}
{{- printf "%s/%s:%s" $registry .Values.image.name $tag -}}
{{- else -}}
{{- printf "%s:%s" .Values.image.name $tag -}}
{{- end -}}
{{- end -}}
