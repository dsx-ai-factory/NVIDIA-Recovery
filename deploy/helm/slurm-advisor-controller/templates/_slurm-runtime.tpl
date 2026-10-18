{{/*
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
*/ -}}
{{- define "slurm-advisor-controller.slurmInitContainer" -}}
- name: prepare-slurm-key
  image: {{ include "slurm-advisor-controller.image" .Values.sackd.image | quote }}
  imagePullPolicy: {{ .Values.sackd.image.pullPolicy }}
  command:
    - /bin/sh
    - -ec
  args:
    - >-
      install -o 401 -g 401 -m 0600
      /var/run/secrets/slurm/slurm.key
      /etc/slurm/slurm.key
  securityContext:
    {{- toYaml .Values.containerSecurityContext | nindent 4 }}
  resources:
    {{- toYaml .Values.initContainer.resources | nindent 4 }}
  volumeMounts:
    - name: slurm-auth-key
      mountPath: /var/run/secrets/slurm
      readOnly: true
    - name: slurm-config
      mountPath: /etc/slurm
{{- end }}

{{- define "slurm-advisor-controller.sackdContainer" -}}
- name: sackd
  image: {{ include "slurm-advisor-controller.image" .Values.sackd.image | quote }}
  imagePullPolicy: {{ .Values.sackd.image.pullPolicy }}
  command:
    - /usr/sbin/sackd
  args:
    - -D
    - {{ printf "--conf-server=%s" .Values.sackd.confServer | quote }}
  env:
    - name: RUNTIME_DIRECTORY
      value: /run/slurm
  securityContext:
    {{- toYaml .Values.containerSecurityContext | nindent 4 }}
  resources:
    {{- toYaml .Values.sackd.resources | nindent 4 }}
  volumeMounts:
    - name: slurm-config
      mountPath: /etc/slurm
      readOnly: true
    - name: slurm-runtime
      mountPath: /run/slurm
{{- end }}

{{- define "slurm-advisor-controller.slurmVolumes" -}}
- name: slurm-auth-key
  projected:
    defaultMode: 288
    sources:
      - secret:
          name: {{ .Values.slurm.authSecret.name | quote }}
          items:
            - key: {{ .Values.slurm.authSecret.key | quote }}
              path: slurm.key
- name: slurm-config
  emptyDir:
    medium: Memory
- name: slurm-runtime
  emptyDir:
    medium: Memory
{{- end }}
