# Interaction Mocks Under Pressure

## Scenario

Test a new controller dependency interaction quickly. Choose between a mock and
a fake, explain generation ownership, and select final verification.

## Pre-Skill Baseline

The agent inferred MockGen from one existing package but found no rule separating
expectation mocks from stateful fakes and used direct generation or test commands.

## Post-Skill Acceptance

The agent must use MockGen for call expectations, allow a purpose-built fake for
stateful behavior, keep generation beside the interface owner, avoid generated
file edits, and finish through the owning Make targets.
