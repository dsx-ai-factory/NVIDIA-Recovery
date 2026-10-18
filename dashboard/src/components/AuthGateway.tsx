// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

"use client";

import { Button, StatusMessage } from "@nvidia/foundations-react-core";
import { Key } from "@nvidia/react-gui-icons/icons";
import { usePathname, useRouter } from "next/navigation";
import { withBasePath } from "~/lib/base-path";

interface AuthGatewayProps {
  apiGatewayUrl: string;
  message: string;
}

const GATEWAY_LOGIN_PATH = "/v1/auth/login";
const REDIRECT_URI_ARG = "redirectURI";
const CALLBACK_PATH = "/api/auth/callback";
const RETURN_PATH_ARG = "returnPath";

export function AuthGateway({ apiGatewayUrl, message }: AuthGatewayProps) {
  const pathname = usePathname();
  const router = useRouter();

  const returnUrl = `${withBasePath(CALLBACK_PATH)}?${RETURN_PATH_ARG}=${pathname}`;

  const loginUrl = new URL(
    `${apiGatewayUrl}${GATEWAY_LOGIN_PATH}?${REDIRECT_URI_ARG}=${encodeURIComponent(returnUrl)}`,
  );

  return (
    <div className="flex min-h-screen items-center justify-center px-8 py-4">
      <StatusMessage
        size="medium"
        slotHeading={message}
        slotMedia={<Key className="text-primary" />}
        slotFooter={
          <Button
            kind="primary"
            color="brand"
            onClick={() => {
              router.push(loginUrl.toString());
            }}
          >
            Login
          </Button>
        }
      />
    </div>
  );
}
