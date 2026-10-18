{{/*
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
*/ -}}
{{/* vim: set filetype=mustache: */}}
{{/*
Expand the name of the chart.
*/}}
{{- define "api-gateway.name" -}}
  {{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "api-gateway.fullname" -}}
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
{{- define "api-gateway.chart" -}}
  {{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}


{{/*
Create the name of the service account to use
*/}}
{{- define "api-gateway.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "api-gateway.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{- define "api-gateway.validate" -}}
{{- if .Values.auth.kubernetes.enabled }}
{{- if eq (.Values.auth.kubernetes.audience | trim) "" }}
{{- fail "auth.kubernetes.audience is required when auth.kubernetes.enabled=true" }}
{{- end }}
{{- if eq (.Values.auth.kubernetes.expectedServiceAccount | trim) "" }}
{{- fail "auth.kubernetes.expectedServiceAccount is required when auth.kubernetes.enabled=true" }}
{{- end }}
{{- $serviceAccountName := include "api-gateway.serviceAccountName" . | trim }}
{{- if eq $serviceAccountName "default" }}
{{- fail "auth.kubernetes.enabled=true requires a named non-default ServiceAccount (set serviceAccount.create=true or serviceAccount.name)" }}
{{- end }}
{{- end }}
{{- $tlsSecretName := .Values.ingress.tls.secretName | trim }}
{{- if ne $tlsSecretName "" }}
{{- if or (not .Values.ingress.enabled) (eq (.Values.ingress.fqdn | trim) "") }}
{{- fail "ingress.tls.secretName requires ingress.enabled=true and ingress.fqdn" }}
{{- end }}
{{- if eq (default "ingress" ((.Values.global).routing).type) "gateway" }}
{{- fail "ingress.tls is only supported when global.routing.type is ingress" }}
{{- end }}
{{- end }}
{{- if .Values.ingress.tls.certManager.enabled }}
{{- if eq $tlsSecretName "" }}
{{- fail "ingress.tls.certManager.enabled=true requires ingress.tls.secretName" }}
{{- end }}
{{- if eq (.Values.ingress.tls.certManager.issuerRef.name | trim) "" }}
{{- fail "ingress.tls.certManager.issuerRef.name is required when ingress.tls.certManager.enabled=true" }}
{{- end }}
{{- end }}
{{- end }}

{{- define "api-gateway.tokenReviewRoleName" -}}
{{- $base := include "api-gateway.fullname" . | trunc 23 | trimSuffix "-" -}}
{{- $namespace := .Release.Namespace | trunc 16 | trimSuffix "-" -}}
{{- $identity := printf "%s/%s/%s" (include "api-gateway.fullname" .) .Release.Name .Release.Namespace -}}
{{- $identityHash := sha256sum $identity | trunc 8 -}}
{{- printf "%s-tokenreviewer-%s-%s" $base $namespace $identityHash -}}
{{- end }}

{{/*
Create the name of the nvsentinel config map
*/}}
{{- define "api-gateway.nvsentinelConfigMapName" -}}
  {{- printf "%s-nvs-config" (include "api-gateway.fullname" .) -}}
{{- end -}}

{{/*
Common labels
*/}}
{{- define "api-gateway.labels" -}}
app.kubernetes.io/full-name: {{ include "api-gateway.fullname" . }}
helm.sh/chart: {{ include "api-gateway.chart" . }}
{{ include "api-gateway.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "api-gateway.selectorLabels" -}}
app.kubernetes.io/name: {{ include "api-gateway.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Fully qualified container image reference. Precedence for the registry
prefix: chart-local image.repository, else global.registry (shared
across all NVIDIA Recovery subcharts). Neither ships a default; an empty
registry yields a bare "name:tag" reference.
*/}}
{{- define "api-gateway.image" -}}
{{- $registry := default ((.Values.global).registry) .Values.image.repository -}}
{{- $tag := default (default $.Chart.Version ((.Values.global).appVersion)) .Values.image.tag -}}
{{- if $registry -}}
{{- printf "%s/%s:%s" $registry .Values.image.name $tag -}}
{{- else -}}
{{- printf "%s:%s" .Values.image.name $tag -}}
{{- end -}}
{{- end -}}
