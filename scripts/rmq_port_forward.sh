#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Forward the RabbitMQ HTTP port to the local machine
# and print the password for the default user

set -euo pipefail

NAMESPACE=${NAMESPACE:-heimdall}

# By default, the release name is heimdall
RELEASE_NAME=${RELEASE_NAME:-nvidia-recovery}
SERVICE_NAME=${SERVICE_NAME:-${RELEASE_NAME}-rabbitmq}
RMQ_HTTP_PORT=${RMQ_HTTP_PORT:-15672}
RMQ_AMQP_PORT=${RMQ_AMQP_PORT:-5672}
kubectl port-forward -n "${NAMESPACE}" service/"${SERVICE_NAME}" "${RMQ_HTTP_PORT}" &
PORT_FORWARD_PID=$!

kubectl port-forward -n "${NAMESPACE}" service/"${SERVICE_NAME}" "${RMQ_AMQP_PORT}" &
PORT_FORWARD_AMQP_PID=$!


trap 'kill ${PORT_FORWARD_PID} ${PORT_FORWARD_AMQP_PID}' EXIT

USERNAME=heimdall
PASSWORD=$(kubectl get -n "${NAMESPACE}" secrets "${SERVICE_NAME}" -o json | jq -r '.data["rabbitmq-password"]' | base64 --decode)

echo "Forwarding the RabbitMQ HTTP port to the local machine"
echo "You can now access the RabbitMQ management UI at http://localhost:${RMQ_HTTP_PORT}"
echo "Username: ${USERNAME}"
echo "Password: ${PASSWORD}"


wait "${PORT_FORWARD_PID}"
