{{/*
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
*/ -}}
{{- define "slurm-advisor-controller.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "slurm-advisor-controller.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{- define "slurm-advisor-controller.watchdogFullname" -}}
{{- $base := include "slurm-advisor-controller.fullname" . | trunc 54 | trimSuffix "-" -}}
{{- printf "%s-watchdog" $base -}}
{{- end }}

{{- define "slurm-advisor-controller.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "slurm-advisor-controller.labels" -}}
helm.sh/chart: {{ include "slurm-advisor-controller.chart" . }}
{{ include "slurm-advisor-controller.commonSelectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "slurm-advisor-controller.commonSelectorLabels" -}}
app.kubernetes.io/name: {{ include "slurm-advisor-controller.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "slurm-advisor-controller.controllerSelectorLabels" -}}
{{ include "slurm-advisor-controller.commonSelectorLabels" . }}
app.kubernetes.io/component: controller
{{- end }}

{{- define "slurm-advisor-controller.watchdogSelectorLabels" -}}
{{ include "slurm-advisor-controller.commonSelectorLabels" . }}
app.kubernetes.io/component: watchdog
{{- end }}

{{- define "slurm-advisor-controller.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "slurm-advisor-controller.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- required "serviceAccount.name is required when serviceAccount.create=false" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{- define "slurm-advisor-controller.image" -}}
{{- printf "%s:%s" .repository .tag }}
{{- end }}

{{- define "slurm-advisor-controller.logLinkSourceRootsJSON" -}}
{{- $roots := list -}}
{{- range .Values.controller.logLinks.sources -}}
{{- $roots = append $roots .mountPath -}}
{{- end -}}
{{- mustToJson $roots -}}
{{- end }}

{{- define "slurm-advisor-controller.validateLogLinkPath" -}}
{{- $name := .name -}}
{{- $path := .path -}}
{{- if or (not (hasPrefix "/" $path)) (ne (clean $path) $path) -}}
{{- fail (printf "%s must be a normalized absolute path" $name) -}}
{{- end -}}
{{- end }}

{{- define "slurm-advisor-controller.validateLogLinkStorage" -}}
{{- $name := .name -}}
{{- $storage := .storage -}}
{{- include "slurm-advisor-controller.validateLogLinkPath" (dict "name" (printf "%s.mountPath" $name) "path" $storage.mountPath) -}}
{{- $hasClaim := hasKey $storage "claimName" -}}
{{- $hasHost := hasKey $storage "hostPath" -}}
{{- if eq $hasClaim $hasHost -}}
{{- fail (printf "%s must set exactly one of claimName or hostPath" $name) -}}
{{- end -}}
{{- if $hasHost -}}
{{- include "slurm-advisor-controller.validateLogLinkPath" (dict "name" (printf "%s.hostPath" $name) "path" $storage.hostPath) -}}
{{- end -}}
{{- end }}

{{- define "slurm-advisor-controller.validatePositiveDuration" -}}
{{- $name := .name -}}
{{- $value := .value | toString -}}
{{- if or (not (regexMatch "^([0-9]+([.][0-9]+)?(ns|us|µs|ms|s|m|h))+$" $value)) (not (regexMatch "[1-9]" $value)) -}}
{{- fail (printf "%s must be a positive duration" $name) -}}
{{- end -}}
{{- end }}

{{- define "slurm-advisor-controller.validate" -}}
{{- $tls := .Values.controller.gatewayTLS -}}
{{- if and (not $tls.enabled) (or $tls.serverName $tls.caConfigMap.name) -}}
{{- fail "controller.gatewayTLS.serverName and caConfigMap.name require controller.gatewayTLS.enabled=true" -}}
{{- end -}}
{{- if and (eq .Values.controller.mode "act") (not .Values.controller.acknowledgeSchedulerActions) }}
{{- fail "controller.mode=act requires controller.acknowledgeSchedulerActions=true" }}
{{- end }}
{{- if and .Values.watchdog.enabled (eq .Values.watchdog.mode "act") (not .Values.watchdog.acknowledgeSchedulerActions) }}
{{- fail "watchdog.mode=act requires watchdog.acknowledgeSchedulerActions=true" }}
{{- end }}
{{- if and .Values.networkPolicy.enabled (or (eq (len .Values.networkPolicy.ingress) 0) (eq (len .Values.networkPolicy.egress) 0)) }}
{{- fail "networkPolicy.enabled=true requires explicit non-empty ingress and egress rules" }}
{{- end }}
{{- range $label := list "app.kubernetes.io/name" "app.kubernetes.io/instance" "app.kubernetes.io/component" }}
{{- if hasKey $.Values.podLabels $label }}
{{- fail (printf "podLabels must not set reserved selector label %s" $label) }}
{{- end }}
{{- end }}
{{- if and .Values.serviceMonitor.enabled (not .Values.metrics.controller.enabled) (or (not .Values.watchdog.enabled) (not .Values.metrics.watchdog.enabled)) }}
{{- fail "serviceMonitor.enabled=true requires at least one enabled metrics endpoint" }}
{{- end }}
{{- if .Values.controller.kubernetesAuth.enabled }}
{{- if eq (.Values.controller.kubernetesAuth.audience | trim) "" }}
{{- fail "controller.kubernetesAuth.audience is required when controller.kubernetesAuth.enabled=true" }}
{{- end }}
{{- if lt (int .Values.controller.kubernetesAuth.expirationSeconds) 600 }}
{{- fail "controller.kubernetesAuth.expirationSeconds must be at least 600" }}
{{- end }}
{{- end }}
{{- if le (int .Values.controller.auditTrail.maxBatchSize) 0 }}
{{- fail "controller.auditTrail.maxBatchSize must be positive" }}
{{- end }}
{{- if le (int .Values.controller.auditTrail.bufferSize) 0 }}
{{- fail "controller.auditTrail.bufferSize must be positive" }}
{{- end }}
{{- include "slurm-advisor-controller.validatePositiveDuration" (dict "name" "controller.auditTrail.flushInterval" "value" .Values.controller.auditTrail.flushInterval) -}}
{{- if le (int .Values.watchdog.auditTrail.maxBatchSize) 0 }}
{{- fail "watchdog.auditTrail.maxBatchSize must be positive" }}
{{- end }}
{{- if le (int .Values.watchdog.auditTrail.bufferSize) 0 }}
{{- fail "watchdog.auditTrail.bufferSize must be positive" }}
{{- end }}
{{- include "slurm-advisor-controller.validatePositiveDuration" (dict "name" "watchdog.auditTrail.flushInterval" "value" .Values.watchdog.auditTrail.flushInterval) -}}
{{- $logLinks := .Values.controller.logLinks -}}
{{- if $logLinks.enabled -}}
{{- if empty $logLinks.destination -}}
{{- fail "controller.logLinks.destination is required when controller.logLinks.enabled=true" -}}
{{- end -}}
{{- if eq (len $logLinks.sources) 0 -}}
{{- fail "controller.logLinks.sources requires at least one source when controller.logLinks.enabled=true" -}}
{{- end -}}
{{- include "slurm-advisor-controller.validateLogLinkStorage" (dict "name" "controller.logLinks.destination" "storage" $logLinks.destination) -}}
{{- $destinationPath := $logLinks.destination.mountPath -}}
{{- $destinationPrefix := printf "%s/" (trimSuffix "/" $destinationPath) -}}
{{- $destinationHasClaim := hasKey $logLinks.destination "claimName" -}}
{{- $destinationHasHost := hasKey $logLinks.destination "hostPath" -}}
{{- $sourcePaths := dict -}}
{{- range $index, $source := $logLinks.sources -}}
{{- $sourceName := printf "controller.logLinks.sources[%d]" $index -}}
{{- include "slurm-advisor-controller.validateLogLinkStorage" (dict "name" $sourceName "storage" $source) -}}
{{- if hasKey $sourcePaths $source.mountPath -}}
{{- fail (printf "controller.logLinks.sources contains duplicate mountPath %s" $source.mountPath) -}}
{{- end -}}
{{- $_ := set $sourcePaths $source.mountPath true -}}
{{- if or (eq $source.mountPath $destinationPath) (hasPrefix $destinationPrefix $source.mountPath) -}}
{{- fail (printf "%s.mountPath must not equal or be inside controller.logLinks.destination.mountPath" $sourceName) -}}
{{- end -}}
{{- if and $destinationHasClaim (hasKey $source "claimName") (eq $logLinks.destination.claimName $source.claimName) -}}
{{- fail (printf "%s.claimName must differ from controller.logLinks.destination.claimName" $sourceName) -}}
{{- end -}}
{{- if and $destinationHasHost (hasKey $source "hostPath") -}}
{{- $destinationHostPrefix := printf "%s/" (trimSuffix "/" $logLinks.destination.hostPath) -}}
{{- $sourceHostPrefix := printf "%s/" (trimSuffix "/" $source.hostPath) -}}
{{- if or
      (eq $source.hostPath $logLinks.destination.hostPath)
      (hasPrefix $destinationHostPrefix $source.hostPath)
      (hasPrefix $sourceHostPrefix $logLinks.destination.hostPath)
-}}
{{- fail (printf "%s.hostPath must not overlap controller.logLinks.destination.hostPath" $sourceName) -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- if gt (len $logLinks.applicationPatterns) (int $logLinks.maxPatterns) -}}
{{- fail "controller.logLinks.applicationPatterns exceeds controller.logLinks.maxPatterns" -}}
{{- end -}}
{{- range $name, $value := dict
  "controller.logLinks.scanInterval" $logLinks.scanInterval
  "controller.logLinks.cleanupInterval" $logLinks.cleanupInterval
  "controller.logLinks.retention" $logLinks.retention
  "controller.logLinks.scanTimeout" $logLinks.scanTimeout
-}}
{{- include "slurm-advisor-controller.validatePositiveDuration" (dict "name" $name "value" $value) -}}
{{- end -}}
{{- end -}}
{{- end }}

{{/*
OpenTelemetry collector environment, matching the NVIDIA Recovery chart's
heimdall.otelEnv. This chart also installs standalone, where that umbrella
helper does not exist. The token variable is declared before the headers
variable so Kubernetes expands it in OTEL_EXPORTER_OTLP_HEADERS.
*/}}
{{- define "slurm-advisor-controller.otelEnv" -}}
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
