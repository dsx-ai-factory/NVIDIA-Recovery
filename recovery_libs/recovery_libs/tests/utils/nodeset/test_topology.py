# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for nodeset.topology module."""

from recovery_libs.utils.nodeset.topology import (
    can_group_by_rack,
    get_rack,
    group_by_rack,
    parse_node,
)


class TestParseNode:
    """Tests for parse_node function."""

    def test_oci_hsg_gb200_format(self):
        """Test OCI-HSG GB200 format: nvl{config}{rack}-T{tray}"""
        result = parse_node("nvl72010-T10")
        assert result.node_name == "nvl72010-T10"
        assert result.rack == "nvl72010"
        assert result.tray == "T10"
        assert result.has_rack is True

    def test_oci_hsg_gb200_various_trays(self):
        """Test various tray numbers."""
        for tray_num in ["01", "05", "10", "16", "18"]:
            result = parse_node(f"nvl72013-T{tray_num}")
            assert result.rack == "nvl72013"
            assert result.tray == f"T{tray_num}"

    def test_aws_dfw_gb200_format(self):
        """Test AWS-DFW GB200 format: nvl{config}d{rack}-T{tray}"""
        result = parse_node("nvl72d002-T05")
        assert result.node_name == "nvl72d002-T05"
        assert result.rack == "nvl72d002"
        assert result.tray == "T05"
        assert result.has_rack is True

    def test_gcp_iad_gb200_format(self):
        """Test GCP-IAD GB200 format: nvl{config}D{rack}-T{tray}"""
        result = parse_node("nvl72D127-T17")
        assert result.node_name == "nvl72D127-T17"
        assert result.rack == "nvl72D127"
        assert result.tray == "T17"
        assert result.has_rack is True

    def test_h100_pool_format(self):
        """Test H100 pool format: pool{id}-{node_id}"""
        result = parse_node("pool0-00123")
        assert result.node_name == "pool0-00123"
        assert result.rack is None
        assert result.tray is None
        assert result.has_rack is False

    def test_h100_pool_various(self):
        """Test various H100 pool formats."""
        test_cases = ["pool0-00123", "pool0-00144", "pool0-0212"]
        for node_name in test_cases:
            result = parse_node(node_name)
            assert result.rack is None
            assert result.tray is None

    def test_unknown_format(self):
        """Test unknown node format returns no rack info."""
        result = parse_node("unknown-node-format")
        assert result.node_name == "unknown-node-format"
        assert result.rack is None
        assert result.tray is None
        assert result.has_rack is False


class TestGroupByRack:
    """Tests for group_by_rack function."""

    def test_single_rack(self):
        """Test grouping nodes from a single rack."""
        nodes = ["nvl72010-T01", "nvl72010-T02", "nvl72010-T03"]
        result = group_by_rack(nodes)
        assert result == {"nvl72010": ["nvl72010-T01", "nvl72010-T02", "nvl72010-T03"]}

    def test_multiple_racks(self):
        """Test grouping nodes from multiple racks."""
        nodes = [
            "nvl72010-T01",
            "nvl72010-T02",
            "nvl72015-T01",
            "nvl72015-T02",
            "nvl72020-T05",
        ]
        result = group_by_rack(nodes)
        assert result == {
            "nvl72010": ["nvl72010-T01", "nvl72010-T02"],
            "nvl72015": ["nvl72015-T01", "nvl72015-T02"],
            "nvl72020": ["nvl72020-T05"],
        }

    def test_pool_nodes_excluded(self):
        """Test that pool nodes (H100/B200) are excluded from grouping."""
        nodes = ["pool0-00123", "pool0-00124", "pool0-00125"]
        result = group_by_rack(nodes)
        assert result == {}

    def test_mixed_nodes(self):
        """Test mixed GB200 and pool nodes."""
        nodes = [
            "nvl72010-T01",
            "pool0-00123",
            "nvl72010-T02",
            "pool0-00124",
        ]
        result = group_by_rack(nodes)
        # Only GB200 nodes should be grouped
        assert result == {"nvl72010": ["nvl72010-T01", "nvl72010-T02"]}

    def test_empty_list(self):
        """Test empty node list."""
        result = group_by_rack([])
        assert result == {}

    def test_nodes_sorted_within_rack(self):
        """Test that nodes are sorted within each rack."""
        nodes = ["nvl72010-T05", "nvl72010-T01", "nvl72010-T03"]
        result = group_by_rack(nodes)
        assert result["nvl72010"] == ["nvl72010-T01", "nvl72010-T03", "nvl72010-T05"]


class TestCanGroupByRack:
    """Tests for can_group_by_rack function."""

    def test_gb200_nodes(self):
        """Test GB200 nodes can be grouped."""
        assert can_group_by_rack(["nvl72010-T01", "nvl72010-T02"]) is True

    def test_pool_nodes(self):
        """Test pool nodes cannot be grouped."""
        assert can_group_by_rack(["pool0-00123", "pool0-00124"]) is False

    def test_samples_first_node_only(self):
        """Test that only the first node is sampled (nodes are homogeneous)."""
        # First node is GB200 - should return True
        assert can_group_by_rack(["nvl72010-T01", "pool0-00123"]) is True
        # First node is pool - should return False
        assert can_group_by_rack(["pool0-00123", "nvl72010-T01"]) is False

    def test_empty_list(self):
        """Test empty list."""
        assert can_group_by_rack([]) is False


class TestGetRack:
    """Tests for get_rack function."""

    def test_gb200_node(self):
        """Test getting rack from GB200 node."""
        assert get_rack("nvl72010-T05") == "nvl72010"
        assert get_rack("nvl72d002-T05") == "nvl72d002"
        assert get_rack("nvl72D127-T17") == "nvl72D127"

    def test_pool_node(self):
        """Test getting rack from pool node returns None."""
        assert get_rack("pool0-00123") is None

    def test_unknown_node(self):
        """Test getting rack from unknown node returns None."""
        assert get_rack("unknown-format") is None
