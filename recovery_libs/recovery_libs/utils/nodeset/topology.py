# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Node topology utilities for parsing node names and grouping by rack.

Supported node naming conventions:

GB200 clusters (rack structure):
  - nvl72010-T10   (OCI-HSG: nvl{config}{rack}-T{tray})
  - nvl72d002-T05  (AWS-DFW: nvl{config}d{rack}-T{tray})
  - nvl72D127-T17  (GCP-IAD: nvl{config}D{rack}-T{tray})

H100/B200 clusters (no rack structure):
  - pool0-00123    (flat pool naming)

Custom patterns:
  - Supports custom regex patterns for extracting rack identifiers
"""

import re
from dataclasses import dataclass

from recovery_libs.utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class NodeTopology:
    """Parsed node topology information."""

    node_name: str
    rack: str | None = None
    tray: str | None = None

    @property
    def has_rack(self) -> bool:
        """Returns True if node has rack information."""
        return self.rack is not None


# Regex patterns for different node naming conventions
# GB200: nvl{config}[dD]?{rack}-T{tray}
GB200_PATTERN = re.compile(r"^(nvl\d+[dD]?\d+)-T(\d+)$")

# H100/B200: pool{id}-{node_id}
POOL_PATTERN = re.compile(r"^pool\d+-\d+$")


def parse_node(node_name: str) -> NodeTopology:
    """
    Parse node name to extract rack and tray information.

    Args:
        node_name: Node name string (e.g., 'nvl72010-T10', 'pool0-00123')

    Returns:
        NodeTopology with rack and tray if extractable, None otherwise.

    Examples:
        >>> parse_node('nvl72010-T10')
        NodeTopology(node_name='nvl72010-T10', rack='nvl72010', tray='T10')

        >>> parse_node('nvl72d002-T05')
        NodeTopology(node_name='nvl72d002-T05', rack='nvl72d002', tray='T05')

        >>> parse_node('pool0-00123')
        NodeTopology(node_name='pool0-00123', rack=None, tray=None)
    """
    # Try GB200 pattern first
    gb200_match = GB200_PATTERN.match(node_name)
    if gb200_match:
        return NodeTopology(
            node_name=node_name,
            rack=gb200_match.group(1),
            tray=f"T{gb200_match.group(2)}",
        )

    # Try pool pattern (H100/B200)
    pool_match = POOL_PATTERN.match(node_name)
    if pool_match:
        return NodeTopology(
            node_name=node_name,
            rack=None,
            tray=None,
        )

    # Unknown format - return node as-is without rack info
    return NodeTopology(node_name=node_name, rack=None, tray=None)


def group_by_rack(
    nodes: list[str], custom_pattern: str | None = None, include_unknown: bool = False
) -> dict[str, list[str]]:
    """
    Group nodes by rack.

    Nodes without rack information (H100/B200 pool nodes) are excluded by default,
    but can be included under "unknown" key if include_unknown=True.

    Args:
        nodes: List of node names
        custom_pattern: Optional regex pattern to extract rack identifier.
                       The pattern should contain a single capture group.
        include_unknown: If True, nodes without rack info are grouped under "unknown" key.
                        If False (default), nodes without rack info are excluded.

    Returns:
        Dictionary mapping rack names to list of node names.
        Empty dict if no nodes have rack information.

    Examples:
        >>> group_by_rack(['nvl72010-T01', 'nvl72010-T02', 'nvl72015-T01'])
        {'nvl72010': ['nvl72010-T01', 'nvl72010-T02'], 'nvl72015': ['nvl72015-T01']}

        >>> group_by_rack(['pool0-00123', 'pool0-00124'])
        {}

        >>> group_by_rack(['pool0-00123', 'pool0-00124'], include_unknown=True)
        {'unknown': ['pool0-00123', 'pool0-00124']}

        >>> group_by_rack(['nvl72010-T01', 'nvl72010-T02'], custom_pattern=r'nvl72[dD]?(\\d+)-T\\d+')
        {'010': ['nvl72010-T01', 'nvl72010-T02']}
    """
    rack_groups: dict[str, list[str]] = {}
    unknown_nodes: list[str] = []

    # Compile custom pattern once if provided
    pattern = re.compile(custom_pattern) if custom_pattern else None

    for node_name in nodes:
        rack: str | None = None

        if pattern:
            match = pattern.search(node_name)
            if match and match.groups():
                rack = match.group(1)
        else:
            topology = parse_node(node_name)
            if topology.has_rack:
                rack = topology.rack

        if rack:
            rack_groups.setdefault(rack, []).append(node_name)
        elif include_unknown:
            unknown_nodes.append(node_name)

    # Add unknown nodes if requested and any exist
    if include_unknown and unknown_nodes:
        rack_groups["unknown"] = unknown_nodes

    # Sort nodes within each rack for consistency
    for rack_nodes in rack_groups.values():
        rack_nodes.sort()

    logger.debug(f"Grouped {len(nodes)} nodes into {len(rack_groups)} rack(s)")
    return rack_groups


def can_group_by_rack(nodes: list[str]) -> bool:
    """
    Check if nodes support rack grouping by sampling the first node.

    Since nodes within a cluster are homogeneous (same naming convention),
    we only need to check the first node to determine if rack grouping applies.

    Args:
        nodes: List of node names

    Returns:
        True if the first node has rack information.
    """
    if not nodes:
        return False
    return parse_node(nodes[0]).has_rack


def get_rack(node_name: str, custom_pattern: str | None = None) -> str | None:
    """
    Get rack name for a node.

    Args:
        node_name: Node name string
        custom_pattern: Optional regex pattern to extract rack identifier.
                       The pattern should contain a single capture group
                       for the rack identifier. If None, uses default parsing.

    Returns:
        Rack name if extractable, None otherwise.

    Examples:
        >>> get_rack("nvl72010-T12")
        "nvl72010"
        >>> get_rack("nvl72010-T12", r'nvl72[dD]?(\\d+)-T\\d+')
        "010"
        >>> get_rack("pool0-00123")
        None
    """
    if custom_pattern:
        match = re.search(custom_pattern, node_name)
        if match and match.groups():
            return match.group(1)
        return None

    return parse_node(node_name).rack
