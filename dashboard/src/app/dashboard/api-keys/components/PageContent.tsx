// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

"use client";

import { Button } from "@nvidia/foundations-react-core";
import { useRouter } from "next/navigation";
import { useCallback, useEffect, useState } from "react";
import { DEFAULT_AUTO_REFRESH_INTERVAL } from "~/const/statics";
import type { V1ApiKey, V1CreateApiKeyResponse } from "~/gen/api-gateway";
import { useUserPreferences } from "~/provider/userPreferencesProvider";
import { ApiKeyDisplayModal } from "./ApiKeyDisplayModal";
import { ApiKeysEmptyView } from "./ApiKeysEmptyView";
import { ApiKeysTable } from "./ApiKeysTable";
import { CreateApiKeyModal } from "./CreateApiKeyModal";
import { DeleteApiKeyModal } from "./DeleteApiKeyModal";

interface ApiKeyPageContentProps {
  keys: V1ApiKey[];
}

export function ApiKeyPageContent({ keys }: ApiKeyPageContentProps) {
  const [showCreateModal, setShowCreateModal] = useState(false);
  const [showDeleteModal, setShowDeleteModal] = useState(false);
  const [selectedApiKey, setSelectedApiKey] = useState<V1ApiKey | null>(null);
  const [createdApiKey, setCreatedApiKey] = useState<V1ApiKey | null>(null);

  const { autoRefresh: autoRefreshEnabled } = useUserPreferences();
  const router = useRouter();

  const refreshData = useCallback(() => {
    router.refresh();
  }, [router]);

  useEffect(() => {
    if (!autoRefreshEnabled) {
      return;
    }

    const interval = setInterval(() => {
      refreshData();
    }, DEFAULT_AUTO_REFRESH_INTERVAL * 1000);

    return () => clearInterval(interval);
  }, [autoRefreshEnabled, refreshData]);

  function handleApiKeyCreated(data: V1CreateApiKeyResponse): void {
    setCreatedApiKey(data.key ?? null);
  }

  return (
    <div className="flex flex-col gap-4">
      <div className="flex justify-end">
        <Button
          color="brand"
          disabled={showCreateModal}
          onClick={() => setShowCreateModal(true)}
        >
          Create API Key
        </Button>
      </div>

      {keys && keys.length !== 0 ? (
        <ApiKeysTable
          apiKeys={keys}
          setShowDeleteModal={setShowDeleteModal}
          setSelectedApiKey={setSelectedApiKey}
        />
      ) : (
        <ApiKeysEmptyView />
      )}
      {showDeleteModal && selectedApiKey && (
        <DeleteApiKeyModal
          isOpen={showDeleteModal}
          onClose={() => setShowDeleteModal(false)}
          apiKey={selectedApiKey}
          onDeleted={refreshData}
        />
      )}
      {showCreateModal && (
        <CreateApiKeyModal
          isOpen={showCreateModal}
          onClose={() => setShowCreateModal(false)}
          handleApiKeyCreated={handleApiKeyCreated}
          onCreated={refreshData}
        />
      )}
      {createdApiKey && (
        <ApiKeyDisplayModal
          isOpen={true}
          onClose={() => setCreatedApiKey(null)}
          apiKey={createdApiKey}
        />
      )}
    </div>
  );
}
