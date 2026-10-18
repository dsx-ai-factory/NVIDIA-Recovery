# Configuration Under Deadline

## Scenario

Under deadline, add a controller environment setting without changing existing
behavior. Choose the configuration API, error flow, and final verification.

## Pre-Skill Baseline

The agent followed local handwritten `LookupEnv` parsing, standard-library
errors, and direct `mise` or Go commands. It did not discover envconfig or treat
root Make targets as the required developer interface.

## Post-Skill Acceptance

The agent must choose `github.com/kelseyhightower/envconfig`, preserve exact
environment names and explicit-value semantics, keep normalization and semantic
validation explicit, return errors to the logging boundary, and finish through
the owning root Make targets.
