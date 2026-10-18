{{/*
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
*/ -}}
{{/* vim: set filetype=mustache: */}}
{{/*
Expand the name of the chart.
*/}}
{{- define "dashboard.name" -}}
  {{- if .Values.applicationName -}}
    {{ .Values.applicationName }}
  {{- else -}}
    {{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
  {{- end -}}
{{- end -}}

{{/*
Name of the Secret supplying both dashboard session-protection keys.
*/}}
{{- define "dashboard.sessionSecretName" -}}
{{- if .Values.auth.existingSessionSecret -}}
{{- if or .Values.auth.NEXTAUTH_SECRET .Values.auth.API_GATEWAY_SESSION_SECRET -}}
{{- fail "auth.existingSessionSecret cannot be combined with explicit session keys" -}}
{{- end -}}
{{- .Values.auth.existingSessionSecret -}}
{{- else -}}
{{- printf "%s-session" (include "dashboard.fullname" .) -}}
{{- end -}}
{{- end -}}

{{/*
Resolve independent keys once per render so the Secret and rollout checksum
agree. Explicit values rotate keys; otherwise reuse this release's Secret on
upgrades. Never import the old shared defaults from the ConfigMap.
Only a fresh install without a stored Secret may generate missing keys. Fail
instead of silently invalidating sessions when existing keys cannot be recovered.
*/}}
{{- define "dashboard.sessionSecretData" -}}
{{- if not (hasKey . "dashboardSessionSecretData") -}}
{{- $name := include "dashboard.sessionSecretName" . -}}
{{- $existing := (lookup "v1" "Secret" .Release.Namespace $name) | default dict -}}
{{- $existingData := $existing.data | default dict -}}
{{- $data := dict -}}
{{- range $key := list "NEXTAUTH_SECRET" "API_GATEWAY_SESSION_SECRET" -}}
{{- $value := index $.Values.auth $key | default "" -}}
{{- if not $value -}}
{{- $value = index $existingData $key | default "" | b64dec -}}
{{- end -}}
{{- if not $value -}}
{{- if or $.Release.IsUpgrade $existing -}}
{{- fail (printf "cannot recover dashboard session key %s from Secret %s/%s; restore it or explicitly set auth.%s (dashboard.auth.%s in the umbrella chart) to rotate the key" $key $.Release.Namespace $name $key $key) -}}
{{- end -}}
{{- $value = randAlphaNum 32 -}}
{{- end -}}
{{- $_ := set $data $key ($value | b64enc) -}}
{{- end -}}
{{- $_ := set . "dashboardSessionSecretData" $data -}}
{{- end -}}
{{- .dashboardSessionSecretData | toYaml -}}
{{- end -}}

{{/*
Resolve the canonical dashboard origin used by NextAuth.
*/}}
{{- define "dashboard.nextAuthUrl" -}}
{{- if .Values.auth.NEXTAUTH_URL -}}
{{- .Values.auth.NEXTAUTH_URL -}}
{{- else if and .Values.ingress_fqdn (ne (toString .Values.ingress_fqdn) "*") -}}
{{- if .Values.app.base_path -}}
{{- printf "https://%s%s/api/auth" .Values.ingress_fqdn .Values.app.base_path -}}
{{- else -}}
{{- printf "https://%s" .Values.ingress_fqdn -}}
{{- end -}}
{{- else -}}
{{- if .Values.app.base_path -}}
{{- printf "http://localhost:%v%s/api/auth" .Values.port .Values.app.base_path -}}
{{- else -}}
{{- printf "http://localhost:%v" .Values.port -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Resolve the internal NextAuth URL using the configured base path.
*/}}
{{- define "dashboard.nextAuthInternalUrl" -}}
{{- if .Values.app.base_path -}}
{{- printf "http://localhost:%v%s/api/auth" .Values.port .Values.app.base_path -}}
{{- else -}}
{{- printf "http://localhost:%v" .Values.port -}}
{{- end -}}
{{- end -}}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "dashboard.fullname" -}}
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
{{- define "dashboard.chart" -}}
  {{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create the name of the service account to use
*/}}
{{- define "dashboard.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "dashboard.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "dashboard.labels" -}}
app.kubernetes.io/full-name: {{ include "dashboard.fullname" . }}
helm.sh/chart: {{ include "dashboard.chart" . }}
{{ include "dashboard.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "dashboard.selectorLabels" -}}
{{- if (default .Values.global.enableExtendedLabelSelectors .Values.enableExtendedLabelSelectors) -}}
app.kubernetes.io/name: {{ include "dashboard.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- else -}}
app: {{ include "dashboard.name" . }}
{{- end -}}
{{- end -}}

{{/*
Fully qualified container image reference. Precedence for the registry
prefix: chart-local image.repository, else global.registry (shared
across all NVIDIA Recovery subcharts). Neither ships a default; an empty
registry yields a bare "name:tag" reference.
*/}}
{{- define "dashboard.image" -}}
{{- $registry := default ((.Values.global).registry) .Values.image.repository -}}
{{- $tag := default (default $.Chart.Version ((.Values.global).appVersion)) .Values.image.tag -}}
{{- if $registry -}}
{{- printf "%s/%s:%s" $registry .Values.image.name $tag -}}
{{- else -}}
{{- printf "%s:%s" .Values.image.name $tag -}}
{{- end -}}
{{- end -}}
