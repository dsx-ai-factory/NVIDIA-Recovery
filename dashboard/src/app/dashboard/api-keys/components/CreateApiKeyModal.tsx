// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

"use client";

import {
  Button,
  DatePicker,
  ModalContent,
  ModalFooter,
  ModalHeading,
  ModalMain,
  ModalRoot,
  Text,
  TextInput,
} from "@nvidia/foundations-react-core";
import { useReducer, useState } from "react";
import Zod from "zod";
import type { V1CreateApiKeyResponse } from "~/gen/api-gateway";
import { createApiKeyAction } from "../actions";

const NINETY_DAYS_IN_MS = 90 * 24 * 60 * 60 * 1000;
const ONE_DAY_IN_MS = 24 * 60 * 60 * 1000;

const calcDaysDiff = (date1: Date, date2: Date) => {
  const diffTime = date1.getTime() - date2.getTime();
  return Math.ceil(diffTime / ONE_DAY_IN_MS);
};
interface CreateApiKeyModalProps {
  isOpen: boolean;
  onClose: () => void;
  handleApiKeyCreated: (apiKey: V1CreateApiKeyResponse) => void;
  onCreated: () => void;
}

const createApiKeySchema = Zod.object({
  description: Zod.string().min(1),
  expiresAt: Zod.string()
    .datetime()
    .refine(
      (date) => {
        return calcDaysDiff(new Date(date), new Date()) > 0;
      },
      {
        message: "Expiration date must be in the future",
      },
    )
    .refine(
      (date) => {
        return calcDaysDiff(new Date(date), new Date()) <= 90;
      },
      {
        message: "Expiration date must be within 90 days",
      },
    ),
});

type CreateApiKeyRequest = Zod.infer<typeof createApiKeySchema>;

type ModalStateAction =
  | { type: "setDescription"; payload: string }
  | { type: "setExpiresAt"; payload: string }
  | { type: "setRequestError"; payload: string }
  | { type: "reset" };

interface ModalState {
  request: CreateApiKeyRequest;
  state: {
    description: {
      error?: string;
      touched: boolean;
    };
    expiresAt: {
      error?: string;
      touched: boolean;
    };
    requestError?: string;
  };
}

const DEFAULT_MODAL_STATE: ModalState = {
  request: {
    description: "",
    expiresAt: "",
  },
  state: {
    description: { error: undefined, touched: false },
    expiresAt: { error: undefined, touched: false },
    requestError: undefined,
  },
};

function modalStateReducer(
  state: ModalState,
  action: ModalStateAction,
): ModalState {
  switch (action.type) {
    case "setDescription": {
      const parsed = createApiKeySchema.safeParse({
        ...state.request,
        description: action.payload,
      });
      if (parsed.success) {
        return {
          ...state,
          request: { ...parsed.data, description: action.payload },
          state: {
            ...state.state,
            description: { error: undefined, touched: true },
          },
        };
      } else {
        const errorObj: Record<string, string> = {};
        parsed.error.issues.forEach((err) => {
          if (err.path[0]) errorObj[String(err.path[0])] = err.message;
        });
        return {
          ...state,
          request: { ...state.request, description: action.payload },
          state: {
            ...state.state,
            description: { error: errorObj.description, touched: true },
          },
        };
      }
    }
    case "setExpiresAt": {
      const parsed = createApiKeySchema.safeParse({
        ...state.request,
        expiresAt: action.payload,
      });
      if (parsed.success) {
        return {
          ...state,
          request: { ...parsed.data, expiresAt: action.payload },
          state: {
            ...state.state,
            expiresAt: { error: undefined, touched: true },
          },
        };
      } else {
        const errorObj: Record<string, string> = {};
        parsed.error.issues.forEach((err) => {
          if (err.path[0]) errorObj[String(err.path[0])] = err.message;
        });
        return {
          ...state,
          request: { ...state.request, expiresAt: action.payload },
          state: {
            ...state.state,
            expiresAt: { error: errorObj.expiresAt, touched: true },
          },
        };
      }
    }
    case "reset":
      return { ...DEFAULT_MODAL_STATE };
    case "setRequestError": {
      return {
        ...state,
        state: { ...state.state, requestError: action.payload },
      };
    }
    default:
      return state;
  }
}

export function CreateApiKeyModal({
  isOpen,
  onClose,
  handleApiKeyCreated,
  onCreated,
}: CreateApiKeyModalProps) {
  const [isSubmitting, setIsSubmitting] = useState(false);

  async function handleCreateApiKey(
    description: string,
    expiresAt: string,
  ): Promise<void> {
    setIsSubmitting(true);
    try {
      const data = await createApiKeyAction({ description, expiresAt });
      handleApiKeyCreated(data);
      onCreated();
      onClose();
    } catch (error) {
      dispatchModalState({
        type: "setRequestError",
        payload:
          error instanceof Error ? error.message : "Failed to create API key",
      });
    } finally {
      setIsSubmitting(false);
    }
  }

  const [modalState, dispatchModalState] = useReducer(
    modalStateReducer,
    DEFAULT_MODAL_STATE,
  );

  // Calculate the minimum date (today) and maximum date (90 days from today)
  const minDate = new Date(Date.now() + ONE_DAY_IN_MS);
  const maxDate = new Date(Date.now() + NINETY_DAYS_IN_MS);

  const handleSubmit = () => {
    void handleCreateApiKey(
      modalState.request.description.trim(),
      modalState.request.expiresAt,
    );
  };

  const handleClose = () => {
    if (!isSubmitting) {
      dispatchModalState({
        type: "reset",
      });
      onClose();
    }
  };

  const handleDateChange = (selectedDate: Date | undefined) => {
    if (!selectedDate) {
      return;
    }

    const todayDate = new Date();
    setExpirationInDays(calcDaysDiff(selectedDate, todayDate));
  };

  const setExpirationInDays = (days: number): void => {
    const futureDate = new Date(Date.now() + days * ONE_DAY_IN_MS);
    dispatchModalState({
      type: "setExpiresAt",
      payload: futureDate.toISOString(),
    });
  };

  const formatDateForDisplay = (dateString: string) => {
    if (!dateString) return "";
    const date = new Date(dateString);
    return date.toLocaleString("en-US", {
      weekday: "long",
      year: "numeric",
      month: "long",
      day: "numeric",
      hour: "numeric",
      minute: "numeric",
      second: "numeric",
    });
  };

  return (
    <ModalRoot onOpenChange={handleClose} open={isOpen}>
      <ModalContent className="!w-[600px]">
        <ModalHeading>Create API Key</ModalHeading>
        <ModalMain>
          <div className="space-y-4">
            {modalState.state.requestError && (
              <div className="rounded-md border border-red-200 bg-red-50 p-3">
                <Text className="text-red-700">
                  {modalState.state.requestError}
                </Text>
              </div>
            )}
            <div className="flex flex-col gap-1">
              <div>Description</div>
              <TextInput
                value={modalState.request.description}
                onValueChange={(value) =>
                  dispatchModalState({
                    type: "setDescription",
                    payload: value,
                  })
                }
                placeholder="Enter a description for your API key"
                maxLength={64}
                disabled={isSubmitting}
                status={
                  modalState.state.description.touched
                    ? modalState.state.description.error
                      ? "error"
                      : "success"
                    : undefined
                }
              />
              <div
                className={
                  modalState.state?.description?.error
                    ? "text-feedback-danger"
                    : modalState.request.description
                      ? "text-feedback-success"
                      : undefined
                }
              >
                {modalState.state?.description?.error
                  ? modalState.state?.description?.error
                  : `${modalState.request.description.length}/64 characters`}
              </div>
            </div>

            <div className="flex flex-col gap-1">
              <div>Expiration Date</div>
              {/* TODO - Figure out how to enable the next year in the date picker */}
              <DatePicker
                value={new Date(modalState.request.expiresAt)}
                onValueChange={(value) => handleDateChange(value)}
                format="MM/dd/yyyy"
                disabledMatcher={{
                  before: minDate,
                  after: maxDate,
                }}
                disabled={isSubmitting}
                status={
                  modalState.state.expiresAt.touched
                    ? modalState.state?.expiresAt?.error
                      ? "error"
                      : "success"
                    : undefined
                }
              />
              <div
                className={
                  modalState.state?.expiresAt?.error
                    ? "text-feedback-danger"
                    : modalState.request.expiresAt
                      ? "text-feedback-success"
                      : undefined
                }
              >
                {modalState.state?.expiresAt?.error
                  ? modalState.state?.expiresAt?.error
                  : modalState.request.expiresAt
                    ? `Your API key will expire on ${formatDateForDisplay(modalState.request.expiresAt)}`
                    : "Select an expiration date (up to 90 days in the future)"}
              </div>
              <div className="mt-2 flex gap-2">
                <Button
                  kind="secondary"
                  size="small"
                  onClick={() => setExpirationInDays(7)}
                  disabled={isSubmitting}
                >
                  1 Week
                </Button>
                <Button
                  kind="secondary"
                  size="small"
                  onClick={() => setExpirationInDays(30)}
                  disabled={isSubmitting}
                >
                  1 Month
                </Button>
                <Button
                  kind="secondary"
                  size="small"
                  onClick={() => setExpirationInDays(90)}
                  disabled={isSubmitting}
                >
                  90 Days
                </Button>
              </div>
            </div>
          </div>
        </ModalMain>
        <ModalFooter>
          <Button
            kind="secondary"
            onClick={handleClose}
            disabled={isSubmitting}
          >
            Cancel
          </Button>
          <Button
            kind="primary"
            color="brand"
            onClick={handleSubmit}
            disabled={
              isSubmitting ||
              !(
                !!modalState.state.description.error ||
                (modalState.state.description.touched &&
                  !!modalState.state.expiresAt.error) ||
                modalState.state.expiresAt.touched
              )
            }
          >
            {isSubmitting ? "Creating..." : "Create API Key"}
          </Button>
        </ModalFooter>
      </ModalContent>
    </ModalRoot>
  );
}
