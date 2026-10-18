// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

"use client";

import {
  Button,
  Flex,
  ModalContent,
  ModalFooter,
  ModalHeading,
  ModalMain,
  ModalRoot,
  Text,
  TextInput,
} from "@nvidia/foundations-react-core";
import { CheckCircle, CopyDoc, Key } from "@nvidia/react-gui-icons/icons";
import { useState } from "react";
import type { V1ApiKey } from "~/gen/api-gateway";

interface ApiKeyDisplayModalProps {
  isOpen: boolean;
  onClose: () => void;
  apiKey: V1ApiKey;
}

export function ApiKeyDisplayModal({
  isOpen,
  onClose,
  apiKey,
}: ApiKeyDisplayModalProps) {
  const [copied, setCopied] = useState(false);

  const handleCopy = async () => {
    await navigator.clipboard.writeText(apiKey.value ?? "");
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  const handleConfirm = () => {
    setTimeout(() => {
      onClose();
      setCopied(false);
    }, 500);
  };

  return (
    <ModalRoot onOpenChange={onClose} open={isOpen}>
      <ModalContent className="!w-[600px]">
        <ModalHeading>API Key Created Successfully</ModalHeading>
        <ModalMain>
          <div className="flex flex-col space-y-4">
            <div className="rounded-lg border border-yellow-200 bg-yellow-50 p-4">
              <Flex align="center" gap="1" className="mb-2 text-yellow-800">
                <Key />
                <Text fontWeight="bold" className="text-yellow-800">
                  Important: Save Your API Key
                </Text>
              </Flex>
              <Text>
                This is the only time you will see the full API key. Please copy
                it to a secure location before closing this dialog.
              </Text>
            </div>

            <div className="flex flex-col">
              <div>API Key Value</div>
              <div className="flex items-center gap-2">
                <TextInput
                  value={apiKey.value ?? ""}
                  disabled
                  size="small"
                  className="text-12 min-w-0 flex-1 font-mono"
                />
                <Button
                  size="small"
                  kind="tertiary"
                  onClick={handleCopy}
                  disabled={copied}
                  className="flex-shrink-0"
                >
                  {copied ? <CheckCircle /> : <CopyDoc />}
                  {copied ? "Copied!" : "Copy"}
                </Button>
              </div>
            </div>

            <div className="flex flex-col space-y-2 rounded border bg-gray-50 p-3">
              <div className="flex flex-col">
                <Text
                  kind="body/regular/sm"
                  className="font-medium text-gray-600"
                >
                  Description:
                </Text>
                <Text
                  kind="body/regular/sm"
                  className="break-words text-gray-600"
                >
                  {apiKey.description ?? "No description"}
                </Text>
              </div>
              <div className="flex flex-col">
                <Text
                  kind="body/regular/sm"
                  className="font-medium text-gray-600"
                >
                  Expires:
                </Text>
                <Text kind="body/regular/sm" className="text-gray-600">
                  {apiKey.expiresAt
                    ? new Date(apiKey.expiresAt).toLocaleDateString()
                    : "No expiration"}
                </Text>
              </div>
            </div>
          </div>
        </ModalMain>
        <ModalFooter>
          <Button
            color="brand"
            onClick={handleConfirm}
            className="w-full sm:w-auto"
          >
            I have saved my API key
          </Button>
        </ModalFooter>
      </ModalContent>
    </ModalRoot>
  );
}
