# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


"""Slurm utilities, including the parsing of slurm's compacted nodelists to iterable strings of valid hostnames"""

import itertools as it
import json
import logging
import re
from collections.abc import Iterable

logger = logging.getLogger(__name__)


def comment_is_APS(json_text: str | None) -> dict[str, dict | str] | None:
    """
    Try to interpret comment string as a json object.
    If there is a top-level key 'APS' or 'HEIMDALL' and it is not null, return parsed json.
    Otherwise, return None.
    """
    if json_text is None:
        return None
    if not isinstance(json_text, str):
        return None

    json_text = json_text.strip()
    if not json_text:
        return None
    if not json_text.startswith("{"):
        return None

    try:
        obj = json.loads(json_text)
        if obj.get("APS", None) is not None or obj.get("HEIMDALL", None) is not None:
            return obj
    except json.decoder.JSONDecodeError as e:
        logger.error(f"JSONDecodeError encountered while decoding <{json_text}> Slurm comment: {e}")
    return None


class ParseError(Exception):
    """Generic parser exception class."""

    def __init__(self, what: str):
        self.what = what

    def __what__(self):
        return self.what

    def __repr__(self):
        return f"ParseError({self.what})"


class ParseBase:
    """Basic skeleton for parsing."""

    def __init__(self, str: str, pos: int):
        """Parser contains a string and its current position in that string (rather than actually split up strings and risk string-interning hell."""
        self.str = str
        self.pos = pos

    def comp_char(self, char: str) -> bool:
        """Compare self.string at self.pos and return if it is char; return false if we've run off the end of str. Perhaps we should accept an "end" param instead of relying on string end."""
        if self.pos >= len(self.str):
            return False
        return self.str[self.pos] == char


class HostPhrase(ParseBase):
    r"""Parses HostPhrase := ([A-Z,a-z,_,-,0-9]*\[ExpanderExp\])*"""

    __hostexp__ = re.compile(r"[\w-]+")

    def parse(self) -> tuple[Iterable[str], int]:
        """Tries to parse the above expression, and throws an exception if it fails.
        Returns result and new position if it suceeds."""
        prod = []
        while self.pos < len(self.str):
            if self.comp_char("["):
                self.pos += 1
                exp1, self.pos = ExpanderExp(self.str, self.pos).parse()
                assert self.comp_char("]")
                self.pos += 1
                prod.append(exp1)
            else:
                m = self.__hostexp__.match(self.str, self.pos)
                if m is None:
                    break
                prod.append(it.repeat(m.group(0), 1))
                self.pos = m.end()
        if len(prod) == 0:
            raise ParseError(f"Could not find expected hostphrase {self.str[self.pos :]}")
        return ("".join(x) for x in it.product(*prod)), self.pos


class ExpanderExp(ParseBase):
    """Parses ExpanderExp := (ExpanderSub)+(,ExpanderSub)*"""

    def parse(self) -> tuple[Iterable[str], int]:
        """Tries to parse the above expression, and throws an exception if it fails.
        Returns result and new position if it suceeds."""
        subs = []
        sub, self.pos = ExpanderSub(self.str, self.pos).parse()
        subs.append(sub)
        while self.comp_char(","):
            self.pos += 1
            sub, self.pos = ExpanderSub(self.str, self.pos).parse()
            subs.append(sub)
        return it.chain(*subs), self.pos


def prefix_count(numstr: str) -> int:
    """Return the estimated prefix length to use; if just '0' or we don't start with 0, that's 0. Otherwise, length of string."""
    if numstr == "0" or numstr[0] != "0":
        return 0
    else:
        return len(numstr)


class ExpanderSub(ParseBase):
    """Parses ExpanderSub := <NON-NEGATIVE INT>|<NON-NEGATIVE INT>-<NON-NEGATIVE INT>"""

    __rangeexp__ = re.compile(r"(\d+)-(\d+)")
    __digitsexp__ = re.compile(r"(\d+)")

    def parse(self) -> tuple[Iterable[str], int]:
        """Tries to parse the above expression, and throws an exception if it fails.
        Returns prefix width and result and new position if it suceeds."""
        if (m := self.__rangeexp__.match(self.str, self.pos)) is not None:
            low_str, high_str = [x for x in m.groups()]
            low_prefix, high_prefix = prefix_count(low_str), prefix_count(high_str)
            low, high = int(low_str), int(high_str)
            if low >= high:
                raise ParseError(f"Range expression ({low}, {high}) covers 0 items!")
            prefix = max(low_prefix, high_prefix)
            return (str(x).zfill(prefix) for x in range(low, high + 1)), m.end()
        elif (m := self.__digitsexp__.match(self.str, self.pos)) is not None:
            return (
                it.repeat(m.group(1), 1),
                m.end(),
            )
        else:
            raise ParseError("Expected expander sub-expression!")


class HostExpr(ParseBase):
    """Parses HostExpr := HostPhrase[.HostPhrase]*"""

    def parse(self) -> tuple:
        """Tries to parse the above expression, and throws an exception if it fails.
        Returns expanded result and new position if it suceeds."""
        prod = []
        host, self.pos = HostPhrase(self.str, self.pos).parse()
        prod.append(host)
        while self.comp_char("."):
            prod.append(".")
            self.pos += 1
            host1, self.pos = HostPhrase(self.str, self.pos).parse()
            prod.append(host1)
        return ("".join(x) for x in it.product(*prod)), self.pos


class HostList(ParseBase):
    """Parses comma-delimited HostExpr, expands, and checks for duplicates"""

    def parse(self):
        """Tries to parse comma-delimnted HostExpr; returns result and updated position."""
        prod = []
        hosts, self.pos = HostExpr(self.str, self.pos).parse()
        prod.append(hosts)
        while self.comp_char(","):
            self.pos += 1
            hosts, self.pos = HostExpr(self.str, self.pos).parse()
            prod.append(hosts)
        all_hosts = list(it.chain(*prod))
        if len(all_hosts) != len(frozenset(all_hosts)):
            logger.warning("Warning: duplicate hosts found in hostlist expansion.")
        return all_hosts, self.pos


def parse_nodelist(string: str) -> Iterable[str]:
    """Convenience wrapper for hostlist"""
    if len(string.strip()) == 0:
        return []

    # A special case that can be returned by sacct when no nodes are assigned.
    if string == "None assigned":
        return []

    try:
        return HostList(string, 0).parse()[0]
    except ParseError as pe:
        logger.error(f"{string} caused {pe}")
        return []


def set_filter_if_any(val: str, filter: frozenset[str] | None) -> bool:
    "If a filter is provided, the value must be in the filter to return True. If no filter is provided, always return False."
    if filter is None or len(filter) == 0:
        return False
    return val in filter


def splitlist(string: str | None) -> frozenset[str] | None:
    """Split a comma-delimited string into a list of strings. Filter out empty/whitespace entries.
    Return None if no nonzero entries were found."""
    if string is None:
        return None
    result = [val for val in (entry.strip() for entry in string.split(",")) if len(val) > 0]
    if len(result) > 0:
        return frozenset(result)
    else:
        return None
