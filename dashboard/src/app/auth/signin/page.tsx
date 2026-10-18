// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import Image from "next/image";
import { notFound } from "next/navigation";
import { env } from "~/env";
import { getAuthConfiguration } from "~/utils/auth";
import { SignIn } from "./components/SignIn";

function getRandomImage() {
  const seed = Math.floor(Math.random() * 1000000);
  return `https://picsum.photos/seed/${seed}/1920/1080`;
}

function getRandomBio() {
  const DESCRIPTIONS = [
    "We can help with your training jobs.",
    "Initiate your training, we'll ensure it completes with minimal downtime.",
    "Leave the heavy lifting to us - we babysit your training jobs while you innovate.",
    "We babysit your training jobs so you don't have to.",
    "Relax while we monitor your jobs - AI training made hassle-free!",
  ];
  return (
    DESCRIPTIONS[Math.floor(Math.random() * DESCRIPTIONS.length)] ??
    DESCRIPTIONS[0]
  );
}

export const dynamic = "force-dynamic";

export default async function SignInPage() {
  const authConfiguration = getAuthConfiguration();
  if (!authConfiguration.available) {
    notFound();
  }

  const cockpitName = env.COCKPIT_NAME ?? "NVIDIA Recovery";

  return (
    <div className="relative min-h-screen w-full bg-gray-700">
      <Image
        alt=""
        src={getRandomImage()}
        priority
        className="fixed top-0 left-0 z-0 h-full w-full object-cover brightness-50"
        height={1080}
        width={1920}
      />
      <div className="absolute inset-0 z-10 flex flex-col items-center justify-center">
        <h1 className="text-20 text-gray-000 mb-2 font-medium">
          Welcome to {cockpitName}
        </h1>
        <p className="leading-body text-gray-000 mb-6 text-center">
          {getRandomBio()}
        </p>
        <SignIn useLdapAuth={authConfiguration.provider === "ldap"} />
      </div>
    </div>
  );
}
