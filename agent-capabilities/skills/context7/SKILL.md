---
name: context7
description: Use when code, tests, configuration, or review depends on current external library, framework, SDK, API, CLI, or cloud-service documentation.
---

# Context7

## Goal

Ground version-sensitive external dependency work in current documentation
without adding retrieval to tasks that can be answered from repository evidence.

Use Context7 when correctness depends on an exact library version, API,
configuration option, migration path, or recently changing service behavior.
Do not use it for stable repository-local behavior that the checked-in code and
tests establish directly.

## Route

1. Identify the dependency, version, and narrow question that affects the task.
2. Resolve and fetch the smallest relevant documentation section with the
   repo-configured Context7 MCP tools.
3. Apply the documented behavior and retain the source needed to explain the
   decision.
4. Stop retrieving when the required fact is supported.

If Context7 is unavailable or returns incomplete results, use the dependency's
official documentation through available research tools. Report the evidence
gap rather than guessing from memory.
