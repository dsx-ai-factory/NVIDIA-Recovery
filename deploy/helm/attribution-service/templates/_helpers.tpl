{{/*
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
*/}}{{/* Preserve the provider suffix so long release names cannot collide with the adapter. */}}
{{- define "attribution-service.fullname" -}}
{{- printf "%s-attribution-service" (.Release.Name | trunc 43 | trimSuffix "-") -}}
{{- end -}}

{{- define "attribution-service.selectorLabels" -}}
app.kubernetes.io/name: attribution-service
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "attribution-service.labels" -}}
{{ include "attribution-service.selectorLabels" . }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | quote }}
{{- end -}}

{{/* Match the application charts' registry and source-build tag overrides. */}}
{{- define "attribution-service.image" -}}
{{- $registry := default .Values.global.registry .Values.image.repository -}}
{{- $tag := default (default .Chart.Version .Values.global.appVersion) .Values.image.tag -}}
{{- if $registry -}}
{{- printf "%s/%s:%s" $registry .Values.image.name $tag -}}
{{- else -}}
{{- printf "%s:%s" .Values.image.name $tag -}}
{{- end -}}
{{- end -}}
