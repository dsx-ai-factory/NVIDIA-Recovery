# Portable Agent Capabilities

This repository keeps shared agent workflows in `agent-capabilities/` and
projects them into each supported client.

## Source And Projections

Edit only the canonical source:

- `agent-capabilities/skills/` for `SKILL.md` workflows.
- `agent-capabilities/agents/` for portable agent prompt specs.
- `agent-capabilities/mcp/context7.json` for the shared Context7 MCP endpoint.
- `agent-capabilities/vendor/` for source and license notes.

Generated projections are committed so a fresh clone works without setup:

- Codex skills: `.agents/skills/`
- Claude Code skills: `.claude/skills/`
- Codex agents: `.codex/agents/`
- Claude Code agents: `.claude/agents/`
- Codex MCP config: `.codex/config.toml`
- Claude Code MCP config: `.mcp.json`

## Commands

Regenerate projections after source edits:

```bash
python3 scripts/sync-agent-capabilities.py --write
```

Check for drift:

```bash
python3 scripts/sync-agent-capabilities.py --check
```

Run the generator regression test:

```bash
python3 scripts/sync-agent-capabilities-self-test.py
```

## Operational Notes

Claude Code and Codex still require normal project trust and MCP approval. The
repository provides the shared configuration, but users and organizations retain
control over whether those project files are trusted and which MCP servers are
enabled.
