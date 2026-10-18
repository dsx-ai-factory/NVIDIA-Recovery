#!/bin/bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


ROOT_FOLDER=$(git rev-parse --show-toplevel)
if ! hash "docker" >/dev/null 2>&1; then
  pip install datamodel-code-generator
  INPUT_FOLDER="${ROOT_FOLDER}/recovery_libs/recovery_libs/schema/json_schema"
  OUTPUT_FOLDER="${ROOT_FOLDER}/recovery_libs/recovery_libs/schema"
  datamodel-codegen --use-double-quotes \
    --input "${INPUT_FOLDER}" \
    --input-file-type jsonschema \
    --output-model-type pydantic_v2.BaseModel \
    --output "${OUTPUT_FOLDER}" \
    --disable-timestamp \
    --use-one-literal-as-default \
    --use-schema-description
else
  INPUT_FOLDER="/local/recovery_libs/recovery_libs/schema/json_schema"
  OUTPUT_FOLDER="/local/recovery_libs/recovery_libs/schema"
  docker run --rm -v "${ROOT_FOLDER}":/local \
    koxudaxi/datamodel-code-generator --use-double-quotes \
    --input "${INPUT_FOLDER}" \
    --input-file-type jsonschema \
    --output-model-type pydantic_v2.BaseModel \
    --output "${OUTPUT_FOLDER}" \
    --disable-timestamp \
    --use-one-literal-as-default \
    --use-schema-description
fi

pip install black
black .. --line-length 120
