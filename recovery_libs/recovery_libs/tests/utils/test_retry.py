# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import unittest
from unittest.mock import MagicMock

from recovery_libs.utils.retry import exponential_backoff_retry


class TestRetry(unittest.TestCase):
    def test_exponential_retry_success(self):
        mock_func = MagicMock()

        # Setting the base delay to be 0 to let the retry happen immediately
        @exponential_backoff_retry(max_retries=3, base_delay=0)
        def function_with_retries():
            mock_func()

        function_with_retries()

        self.assertEqual(mock_func.call_count, 1)

    def test_exponential_retry_exception(self):
        mock_func = MagicMock(side_effect=ValueError("Test"))

        # Setting the base delay to be 0 to let the retry happen immediately
        @exponential_backoff_retry(max_retries=3, base_delay=0)
        def function_with_retries():
            mock_func()

        with self.assertRaises(ValueError):
            function_with_retries()

        self.assertEqual(mock_func.call_count, 4)

    def test_exponential_retry_function(self):
        self.attempts = 0

        def retry_func(ret) -> tuple[bool, str]:
            if ret == "upstream failed":
                return True, ret
            return False, ""

        # Setting the base delay to be 0 to let the retry happen immediately
        @exponential_backoff_retry(max_retries=2, base_delay=0, retry_func=retry_func)
        def function_with_retries():
            self.attempts += 1
            if self.attempts < 3:
                return "upstream failed"
            return "success"

        self.assertEqual(function_with_retries(), "success")
        self.assertEqual(self.attempts, 3)


if __name__ == "__main__":
    unittest.main()
