# Errors And Logging Under Pressure

## Scenario

Add context to a controller failure during an incident without changing error
identity or producing duplicate logs. Choose error and logging APIs.

## Pre-Skill Baseline

The agent chose `fmt.Errorf` or standard-library errors. It inferred typed Zap
fields and boundary logging but had no rule against parallel `%+v` stack fields.

## Post-Skill Acceptance

The agent must use `github.com/cockroachdb/errors`, preserve `Is` and `As`, use
typed Zap fields with `zap.Error`, log once at the owning boundary, and reject a
separate verbose stack field.
