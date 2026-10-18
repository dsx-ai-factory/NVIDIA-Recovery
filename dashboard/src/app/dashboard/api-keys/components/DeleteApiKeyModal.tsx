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
} from "@nvidia/foundations-react-core";
import { Trash, Warning } from "@nvidia/react-gui-icons/icons";
import { useState } from "react";
import type { V1ApiKey } from "~/gen/api-gateway";
import { deleteApiKeyAction } from "../actions";

interface DeleteApiKeyModalProps {
  isOpen: boolean;
  onClose: () => void;
  apiKey: V1ApiKey;
  onDeleted: () => void;
}

export function DeleteApiKeyModal({
  isOpen,
  onClose,
  apiKey,
  onDeleted,
}: DeleteApiKeyModalProps) {
  const [deleteApiKeyError, setDeleteApiKeyError] = useState<string | null>(
    null,
  );
  const [isPending, setIsPending] = useState(false);

  async function handleDeleteApiKey(): Promise<void> {
    if (!apiKey.id) return;

    setIsPending(true);
    try {
      await deleteApiKeyAction(apiKey.id);
      onDeleted();
      onClose();
    } catch (error) {
      setDeleteApiKeyError(
        error instanceof Error ? error.message : "Failed to delete API key",
      );
    } finally {
      setIsPending(false);
    }
  }

  const handleConfirm = () => {
    setDeleteApiKeyError(null);
    void handleDeleteApiKey();
  };

  const handleClose = () => {
    if (!isPending) {
      onClose();
    }
  };

  return (
    <ModalRoot onOpenChange={handleClose} open={isOpen}>
      <ModalContent className="!w-[600px]">
        <ModalHeading className="flex items-center gap-2 text-red-600">
          Delete API Key
        </ModalHeading>
        <ModalMain>
          <div className="flex flex-col gap-2 space-y-6">
            {deleteApiKeyError && (
              <div className="rounded-md border border-red-200 bg-red-50 p-3">
                <Text className="text-red-700">{deleteApiKeyError}</Text>
              </div>
            )}
            <div className="rounded-lg border border-red-200 bg-red-50 p-4">
              <Flex align="center" gap="1" className="mb-3">
                <div className="flex-shrink-0">
                  <Warning className="h-5 w-5 text-red-600" />
                </div>
                <Text fontWeight="bold" className="font-semibold text-red-800">
                  Warning
                </Text>
              </Flex>

              <Text className="mb-3 text-red-700">
                Are you sure you want to delete the API key{" "}
                <span className="font-semibold text-red-900">
                  &ldquo;{apiKey.description}&rdquo;
                </span>
                ?
                <br />
                <br />
              </Text>

              <Text className="text-red-600">
                This action cannot be undone. Once deleted, this API key will no
                longer work for authentication.
              </Text>
            </div>

            {/* API Key Display */}
            <div>
              <Text className="mb-1 block">API Key Value</Text>
              <div className="rounded-lg border border-gray-200 bg-gray-50 p-4">
                <Text className="text-12 font-mono break-all text-gray-800">
                  {apiKey.value}
                </Text>
              </div>
            </div>
          </div>
        </ModalMain>
        <ModalFooter className="gap-3">
          <Button kind="secondary" onClick={handleClose} disabled={isPending}>
            Cancel
          </Button>
          <Button
            color="danger"
            onClick={handleConfirm}
            disabled={isPending}
            className="min-w-[140px] border-red-600 bg-red-600 hover:border-red-700 hover:bg-red-700"
          >
            {isPending ? (
              <Flex align="center" gap="1">
                <div className="h-4 w-4 animate-spin rounded-full border-2 border-white border-t-transparent" />
                Deleting...
              </Flex>
            ) : (
              <Flex align="center" gap="1">
                <Trash className="h-4 w-4" />
                Delete API Key
              </Flex>
            )}
          </Button>
        </ModalFooter>
      </ModalContent>
    </ModalRoot>
  );
}
