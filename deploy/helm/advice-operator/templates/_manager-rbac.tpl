{{- /*
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0

Generated file: do not edit. Run make generate-go GO_PACKAGES=advice-operator.
*/ -}}
{{- define "advice-operator.managerRules" }}
- apiGroups:
  - heimdall.nvidia.com
  resources:
  - advices
  verbs:
  - create
  - delete
  - get
  - list
  - patch
  - update
  - watch
- apiGroups:
  - heimdall.nvidia.com
  resources:
  - advices/finalizers
  verbs:
  - update
- apiGroups:
  - heimdall.nvidia.com
  resources:
  - advices/status
  verbs:
  - get
  - patch
  - update
- apiGroups:
  - run.ai
  resources:
  - distributedworkloads
  verbs:
  - get
{{- end }}
