# Contributing to NVIDIA Recovery

External contributions are welcome: issues, documentation and code pull requests.
Read the [Code of Conduct](CODE_OF_CONDUCT.md), search existing issues, and discuss
substantial changes with maintainers before implementing them. Report suspected
vulnerabilities privately through [NVIDIA Product Security](SECURITY.md).

## CI approval

NVIDIA copy-pr-bot copies approved PR heads to `pull-request/*` branches for CI.
A PR from anyone outside the CODEOWNER maintainer team, including other NVIDIA
employees, does not run CI automatically. A maintainer reviews the current diff
and comments on the PR with its full current head commit:

```text
/ok to test <full-current-head-sha>
```

The placeholder must be replaced with the actual SHA. A new push requires a new
approval; approval of a previous head does not authorize additional code. An
unauthorized commenter or mismatched SHA cannot authorize the run. This approves
CI execution only; code review, DCO and required checks still govern merging.
Trusted, signed maintainer PRs may be copied automatically by the bot.

## Issues

Use the repository issue forms and include enough detail for the NVIDIA
Recovery team to reproduce or evaluate the request. Bug reports should include
the NVIDIA Recovery revision or version, deployment mode, relevant
configuration with secrets removed, reproduction steps, expected behavior,
actual behavior, and sanitized logs.

Do not include credentials, private hostnames, customer data, or other
sensitive information.

Maintainers triage issues using reproducibility, user impact and supported scope.
They may request missing evidence, link duplicates or explain an out-of-scope
closure. Provide impact and reproduction details to help prioritize the work;
triage and responses are best effort, with no guaranteed response time.

## Development

The repository contains Python, Go, TypeScript, Helm, and shell components.
Install only the toolchain required by the area you are changing. CI records
the authoritative pinned versions; the root Python workspace requires Python
3.12 or later.

Create a branch in your fork and install the relevant dependencies. For a full
development workflow, follow [Start developing](docs/development/getting-started.md)
for component toolchains, dependency setup, and image builds. Use the
[local tutorial](docs/getting-started.md) to run a Slurm/Slinky environment and
the [testing guide](docs/development/testing.md) to choose between component
checks, the scheduler harness, and disposable E2E.

Prefer the smallest formatter, lint, test, or `make verify-<area>` target that
proves the changed behavior. Repository-wide verification is not the default
gate.

### Nightly Tests

Use the nightly designation for tests that are too expensive for pull-request
and ordinary main-branch CI. They remain part of scheduled nightly and tagged
release suites. Do not use it to hide a flaky test.

Mark a Python test with the registered pytest marker:

```python
@pytest.mark.nightly
def test_large_recovery_matrix(): ...
```

Give a top-level Go test a `TestNightly` prefix:

```go
func TestNightlyLargeRecoveryMatrix(t *testing.T) {
	// ...
}
```

GitHub pull-request and main-branch workflows pass explicit test
selection flags that exclude these tests. Scheduled nightlies and tagged
releases run the full suite, including nightly tests. Ordinary local runs remain
unfiltered unless the caller supplies its own selection flag.

## Change Expectations

- Keep each pull request focused on one reviewable outcome.
- Preserve supported end-user recovery behavior behind production interfaces.
- Add or update tests for changed behavior; do not add test-only ingress.
- Update user-facing and operator documentation when behavior or configuration
  changes.
- Add or supersede an architecture decision record only for a durable,
  cross-cutting, or expensive-to-reverse decision.

Changes involving authentication, authorization, scheduler actions, secret
loading, migrations, release automation, or behavior that can kill, release,
stop, or modify a workload require human review.

## Pull Requests

1. Create a branch from the current `main`.
2. Make the focused change and run relevant validation.
3. Review your diff for accidental credentials, private surfaces, generated
   artifacts, and unrelated edits.
4. Push the branch and open a pull request using the repository template.
5. Address maintainer feedback and keep the branch current when requested.

All submissions require review. Approval and green required checks do not
guarantee acceptance; maintainers also evaluate scope, product direction,
security, maintainability, and license compatibility.

## Signing Off Your Work

* We require that all contributors "sign-off" on their commits. This certifies that the contribution is your original work, or you have rights to submit it under the same license, or a compatible license.

  * Any contribution which contains commits that are not Signed-Off will not be accepted.

* To sign off on a commit you simply use the `--signoff` (or `-s`) option when committing your changes:
  ```bash
  $ git commit -s -m "Add cool feature."
  ```
  This will append the following to your commit message:
  ```
  Signed-off-by: Your Name <your@email.com>
  ```

* Full text of the DCO (https://developercertificate.org/):

  ```
    Developer Certificate of Origin
    Version 1.1

    Copyright (C) 2004, 2006 The Linux Foundation and its contributors.

    Everyone is permitted to copy and distribute verbatim copies of this
    license document, but changing it is not allowed.


    Developer's Certificate of Origin 1.1

    By making a contribution to this project, I certify that:

    (a) The contribution was created in whole or in part by me and I
        have the right to submit it under the open source license
        indicated in the file; or

    (b) The contribution is based upon previous work that, to the best
        of my knowledge, is covered under an appropriate open source
        license and I have the right under that license to submit that
        work with modifications, whether created in whole or in part
        by me, under the same open source license (unless I am
        permitted to submit under a different license), as indicated
        in the file; or

    (c) The contribution was provided directly to me by some other
        person who certified (a), (b) or (c) and I have not modified
        it.

    (d) I understand and agree that this project and the contribution
        are public and that a record of the contribution (including all
        personal information I submit with it, including my sign-off) is
        maintained indefinitely and may be redistributed consistent with
        this project or the open source license(s) involved.
  ```

## License

Unless explicitly stated otherwise, accepted contributions are licensed
under the [Apache License 2.0](LICENSE).
