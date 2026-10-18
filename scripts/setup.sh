#!/usr/bin/env bash

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

MINIKUBE_VERSION=${MINIKUBE_VERSION:-v1.32.0}

set -e

if ! command -v virtualenv >/dev/null; then
  echo "Missing virtualenv, attempting to install..."

  if ! command -v pip3 >/dev/null; then
    echo "Missing pip3 (used to install virtualenv), please install pip3 or virtualenv!"
    exit 1
  fi

  pip3 install virtualenv
fi

if ! command -v minikube >/dev/null; then
  echo "Downloading minikube $MINIKUBE_VERSION..."

  platform=$(uname -m)

  if [ "$platform" == "x86_64" ]; then
    platform="amd64"
  fi

  case $(uname) in
    Darwin)
      curl -LO https://github.com/kubernetes/minikube/releases/download/$MINIKUBE_VERSION/minikube-darwin-$platform
      sudo install minikube-darwin-$platform /usr/local/bin/minikube
      ;;
    Linux)
      curl -L https://github.com/kubernetes/minikube/releases/download/$MINIKUBE_VERSION/minikube-linux-$platform -o ~/.local/bin/minikube
      chmod +x ~/.local/bin/minikube
      ;;
  esac
fi

SCRIPT_PATH=$(dirname $(realpath -s $0))
cd $SCRIPT_PATH/..

virtualenv .venv
source .venv/bin/activate
find . -name requirements.txt -exec pip install -r {} \;
pip install "pre-commit==4.6.0"
pre-commit install --install-hooks

echo -e "\nStart the minikube cluster with \`minikube start\`"
echo "Source the virtualenv with \`source .venv/bin/activate\`"
