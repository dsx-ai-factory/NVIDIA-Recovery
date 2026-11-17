// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

"use client";
import { Button, Flex, TextInput } from "@nvidia/foundations-react-core";
import { CircleTick } from "@nvidia/react-gui-icons/icons";
import { useSearchParams } from "next/navigation";
import { signIn } from "next-auth/react";
import React, { useState } from "react";
import { withBasePath } from "~/lib/base-path";
import {
  buildGatewayLoginUrl,
  returnPathFromCallbackUrl,
} from "~/lib/gatewayLogin";

export function SignIn({
  useLdapAuth = false,
  apiGatewayLoginUrl,
}: {
  useLdapAuth: boolean;
  /** Set when the API Gateway owns login; the button then leaves for the gateway instead of calling NextAuth. */
  apiGatewayLoginUrl?: string;
}) {
  const searchParams = useSearchParams();
  const [clicked, setClicked] = React.useState(false);
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const onSignIn = (username: string, password: string) => {
    if (apiGatewayLoginUrl) {
      const returnPath = returnPathFromCallbackUrl(
        searchParams.get("callbackUrl"),
        window.location.origin,
      );
      window.location.assign(
        buildGatewayLoginUrl(apiGatewayLoginUrl, returnPath),
      );
      return;
    }
    const options = useLdapAuth
      ? {
          username,
          password,
          callbackUrl: withBasePath("/dashboard/workflow"),
        }
      : {
          callbackUrl: withBasePath("/dashboard/workflow"),
        };
    void signIn("nvlogin", options);
  };
  return (
    <Flex align="center" direction="col" className="w-[200px]" gap="2">
      {useLdapAuth && (
        <>
          <Flex direction="col" gap="1" className="w-full">
            <div className="text-left text-white">
              Username <span className="text-red-500">*</span>
            </div>
            <TextInput
              id="username"
              onValueChange={(value) => setUsername(value)}
              required
              value={username}
            />
          </Flex>
          <Flex direction="col" gap="1" className="w-full">
            <div className="text-left text-white">
              Password <span className="text-red-500">*</span>
            </div>
            <TextInput
              id="password"
              onValueChange={(value) => setPassword(value)}
              onKeyDown={(e) => {
                if (e.key === "Enter") {
                  setClicked(true);
                  onSignIn(username, password);
                  setClicked(false);
                }
              }}
              required
              value={password}
              type="password"
            />
          </Flex>
        </>
      )}
      <Button
        color="brand"
        disabled={clicked}
        className="!w-full"
        onClick={async () => {
          setClicked(true);
          onSignIn(username, password);
          setClicked(false);
        }}
      >
        {clicked ? <CircleTick className="animate-spin" /> : "Login"}
      </Button>
    </Flex>
  );
}
