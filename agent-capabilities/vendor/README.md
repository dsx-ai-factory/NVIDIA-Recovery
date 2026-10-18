# Agent Capability Vendor Notes

This directory records source and license context for vendored agent
capabilities projected into Claude Code and Codex.

| Capability | Source | License note |
|------------|--------|--------------|
| Context7 | Local installed Claude plugin cache, `claude-plugins-official/context7/local`; upstream Context7 repository | Upstream MIT license recorded in `vendor/context7/LICENSE`; plugin metadata and the original stdio MCP config are recorded for provenance. |
| code-simplifier | Local installed Claude plugin cache, `claude-plugins-official/code-simplifier/1.0.0` | `vendor/code-simplifier/LICENSE` |
| feature-dev | Local installed Claude plugin cache, `claude-plugins-official/feature-dev/local` | `vendor/feature-dev/LICENSE` |

The repository projections are generated from `agent-capabilities/` with:

```bash
python3 scripts/sync-agent-capabilities.py --write
```
