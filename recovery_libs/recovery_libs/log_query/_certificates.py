# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""TLS certificate inspection shared by backends that report certificate expiry."""

from __future__ import annotations

import logging
from datetime import UTC, datetime

from cryptography import x509

logger = logging.getLogger(__name__)


def days_until_expiry(cert_path: str | None) -> int | None:
    """Return whole days until the PEM certificate at ``cert_path`` expires, negative once expired.

    Returns None when no path is configured or the certificate cannot be read, so a missing or
    unreadable file never fails the caller.
    """
    if not cert_path:
        return None
    try:
        with open(cert_path, "rb") as cert_file:
            certificate = x509.load_pem_x509_certificate(cert_file.read())
    except (OSError, ValueError):
        logger.warning("Failed to read certificate expiration", exc_info=True, extra={"cert_path": cert_path})
        return None
    return (certificate.not_valid_after_utc - datetime.now(UTC)).days
