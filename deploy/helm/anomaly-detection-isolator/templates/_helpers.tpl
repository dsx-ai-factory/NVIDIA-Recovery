{{/*
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
*/ -}}
{{/* vim: set filetype=mustache: */}}
{{/*
Expand the name of the chart.
*/}}
{{- define "anomaly-detection-isolator.name" -}}
  {{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "anomaly-detection-isolator.fullname" -}}
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
{{- define "anomaly-detection-isolator.chart" -}}
  {{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create the name of the service account to use
*/}}
{{- define "anomaly-detection-isolator.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "anomaly-detection-isolator.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "anomaly-detection-isolator.labels" -}}
app.kubernetes.io/full-name: {{ include "anomaly-detection-isolator.fullname" . }}
helm.sh/chart: {{ include "anomaly-detection-isolator.chart" . }}
{{ include "anomaly-detection-isolator.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "anomaly-detection-isolator.selectorLabels" -}}
app.kubernetes.io/name: {{ include "anomaly-detection-isolator.name" . }}
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
{{- define "anomaly-detection-isolator.image" -}}
{{- $registry := default ((.Values.global).registry) .Values.image.repository -}}
{{- $tag := default (default $.Chart.Version ((.Values.global).appVersion)) .Values.image.tag -}}
{{- if $registry -}}
{{- printf "%s/%s:%s" $registry .Values.image.name $tag -}}
{{- else -}}
{{- printf "%s:%s" .Values.image.name $tag -}}
{{- end -}}
{{- end -}}

{{/* Match the sibling adapter's default fullname. Custom names/ports need an explicit URL. */}}
{{- define "anomaly-detection-isolator.attributionURL" -}}
{{- if hasKey .Values "attribution_service_url" -}}
{{- required "attribution_service_url must not be empty when attribution_enabled=true; omit it to use the same-release adapter" (.Values.attribution_service_url | trim) -}}
{{- else -}}
{{- $name := .Release.Name -}}
{{- if not (contains "attribution-adapter" $name) -}}
{{- $name = printf "%s-attribution-adapter" $name -}}
{{- end -}}
{{- printf "http://%s:8000" ($name | trunc 63 | trimSuffix "-") -}}
{{- end -}}
{{- end -}}
