# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


import unittest

from recovery_libs.utils.nodeset.topology import get_rack, group_by_rack


class TestNodeUtils(unittest.TestCase):
    """Test cases for node utility functions from recovery_libs."""

    def test_get_rack_nvl72_pattern(self):
        """Test extraction with nvl72[dD]<number>-T<number> pattern (uses recovery_libs)."""
        # Standard pattern without d/D - returns full rack name
        self.assertEqual(get_rack("nvl72010-T12"), "nvl72010")
        self.assertEqual(get_rack("nvl72001-T01"), "nvl72001")
        self.assertEqual(get_rack("nvl72999-T99"), "nvl72999")

        # Pattern with lowercase d
        self.assertEqual(get_rack("nvl72d010-T01"), "nvl72d010")
        self.assertEqual(get_rack("nvl72d001-T12"), "nvl72d001")

        # Pattern with uppercase D
        self.assertEqual(get_rack("nvl72D010-T12"), "nvl72D010")
        self.assertEqual(get_rack("nvl72D020-T05"), "nvl72D020")

    def test_get_rack_no_match(self):
        """Test extraction when no pattern matches."""
        self.assertEqual(get_rack("node001"), None)
        self.assertEqual(get_rack("compute-001"), None)
        self.assertEqual(get_rack(""), None)
        self.assertEqual(get_rack("pool0-00123"), None)

    def test_get_rack_custom_pattern(self):
        """Test extraction with custom regex pattern to extract just rack number."""
        # Custom pattern to extract just the rack number (010 instead of nvl72010)
        custom_pattern = r"nvl72[dD]?(\d+)-T\d+"
        self.assertEqual(get_rack("nvl72010-T12", custom_pattern), "010")
        self.assertEqual(get_rack("nvl72d010-T01", custom_pattern), "010")
        # No match with custom pattern
        self.assertEqual(get_rack("pool0-00123", custom_pattern), None)

    def test_group_by_rack_nvl72_pattern(self):
        """Test grouping nodes by rack with nvl72 pattern."""
        nodes = [
            "nvl72010-T12",
            "nvl72010-T13",
            "nvl72d010-T01",  # Different naming variant, different rack
            "nvl72D020-T05",
            "nvl72020-T06",  # Different naming, different rack from D020
        ]

        result = group_by_rack(nodes)

        # Note: nvl72d010 and nvl72010 are considered different racks by default
        self.assertGreaterEqual(len(result), 2)
        self.assertIn("nvl72010", result)

    def test_group_by_rack_with_unknown(self):
        """Test grouping nodes where some don't match any pattern."""
        nodes = [
            "nvl72010-T01",
            "nvl72010-T02",
            "node999",  # No rack pattern
        ]

        result = group_by_rack(nodes, include_unknown=True)

        self.assertEqual(len(result), 2)
        self.assertIn("nvl72010", result)
        self.assertIn("unknown", result)
        self.assertEqual(len(result["nvl72010"]), 2)
        self.assertEqual(len(result["unknown"]), 1)
        self.assertIn("node999", result["unknown"])

    def test_group_by_rack_custom_pattern(self):
        """Test grouping nodes with custom pattern to extract just rack number."""
        nodes = [
            "nvl72010-T01",
            "nvl72010-T02",
            "nvl72d010-T03",  # Same rack number
        ]

        # Custom pattern extracts just the rack number
        custom_pattern = r"nvl72[dD]?(\d+)-T\d+"
        result = group_by_rack(nodes, custom_pattern=custom_pattern)

        self.assertEqual(len(result), 1)
        self.assertIn("010", result)
        self.assertEqual(len(result["010"]), 3)

    def test_group_by_rack_empty_list(self):
        """Test grouping with empty node list."""
        result = group_by_rack([])
        self.assertEqual(len(result), 0)

    def test_group_by_rack_single_rack(self):
        """Test grouping when all nodes are in same rack."""
        nodes = [
            "nvl72010-T01",
            "nvl72010-T02",
            "nvl72010-T03",
        ]

        result = group_by_rack(nodes)

        self.assertEqual(len(result), 1)
        self.assertIn("nvl72010", result)
        self.assertEqual(len(result["nvl72010"]), 3)


if __name__ == "__main__":
    unittest.main()
