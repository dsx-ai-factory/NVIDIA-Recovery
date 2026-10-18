#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Export the real Adapter OpenAPI without starting its optional provider."""

import argparse
import json
import sys
from pathlib import Path

from attribution_adapter.main import app


def main() -> None:
    """Write stable JSON, or fail if the checked-in public schema has drifted."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    if any(
        name == "nvidia_attribution_client" or name.startswith("nvidia_attribution_client.") for name in sys.modules
    ):
        raise SystemExit("Public schema export unexpectedly imported the provider client")
    output = Path(__file__).resolve().parents[2] / "fern/apis/attribution-adapter/openapi.json"
    schema = json.dumps(app.openapi(), indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    if args.check:
        if not output.exists() or output.read_text() != schema:
            raise SystemExit(
                "Adapter OpenAPI is stale; run scripts/docs/export-openapi.py with the locked public environment"
            )
        print("Adapter OpenAPI matches app.openapi()")
    else:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(schema)
        print(f"Wrote {output.relative_to(output.parents[3])}")


if __name__ == "__main__":
    main()
