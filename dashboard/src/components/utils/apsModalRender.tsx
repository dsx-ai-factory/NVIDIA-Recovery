// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

"use client";

import { Button, Modal, Tooltip } from "@nvidia/foundations-react-core";
import { CircleTick } from "@nvidia/react-gui-icons/icons";
import { type ReactNode, useState } from "react";

function DisabledTooltip({
  children,
  disabled,
  message,
}: {
  children: ReactNode;
  disabled: boolean;
  message: string;
}) {
  if (disabled) {
    return <Tooltip slotContent={message}>{children}</Tooltip>;
  } else {
    return children;
  }
}

export function ApsRenderModal({
  caption,
  title,
  content,
  onConfirm,
  enabled = true,
}: {
  caption: ReactNode;
  title: string;
  content: string;
  onConfirm: () => Promise<unknown>;
  enabled: boolean;
}) {
  const [modalOpen, setModalOpen] = useState(false);
  const [inProgress, setInProgress] = useState(false);

  return (
    <>
      <DisabledTooltip
        disabled={!enabled}
        message='To override disabled state you can enable "god mode" in the additional actions menu.'
      >
        <Button
          size="small"
          disabled={!enabled}
          onClick={() => setModalOpen(true)}
          kind="secondary"
        >
          {caption}
        </Button>
      </DisabledTooltip>
      <Modal
        onOpenChange={(open) => setModalOpen(open)}
        open={modalOpen}
        slotHeading={title}
        slotFooter={
          <>
            <Button kind="secondary" onClick={() => setModalOpen(false)}>
              Cancel
            </Button>
            <Button
              color="brand"
              disabled={inProgress}
              onClick={async () => {
                setInProgress(true);
                await onConfirm();
                setInProgress(false);
                setModalOpen(false);
              }}
            >
              {inProgress ? <CircleTick className="animate-spin" /> : "Confirm"}
            </Button>
          </>
        }
      >
        {content}
      </Modal>
    </>
  );
}
