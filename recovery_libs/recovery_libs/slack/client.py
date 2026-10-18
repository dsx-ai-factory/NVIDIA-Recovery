# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import logging

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [thread_id: %(thread)d] - %(name)s - %(levelname)s - %(message)s",
    force=True,
)

logger = logging.getLogger(__name__)


class SlackClient:
    def __init__(self, slack_token: str, channel: str):
        self.slack_client = WebClient(token=slack_token)
        self.channel = channel

    def lookup_user(self, email: str):
        response = self.slack_client.users_lookupByEmail(email=email)
        if response.status_code >= 400:
            raise LookupError(f"the email {email} could not be found in slack.")
        return response.get("user")["id"]

    def post_message(self, text: str):
        """
        post message to Slack channel
        :param channel: the channel to post
        :param text: the text message to post
        """
        try:
            response = self.slack_client.chat_postMessage(channel=self.channel, text=f"{text}", link_names=True)
            logger.info(f"slack message posted to channel {self.channel}: {response}")
        except SlackApiError as ex:
            logger.error(f"slack message failed to post to channel {self.channel}: {ex}")
