# NVIDIA Recovery releases

NVIDIA Recovery distributes a source archive and the `nvidia-recovery` Helm chart
through GitHub Releases, and the same chart through
`oci://ghcr.io/dsx-ai-factory/nvidia-recovery/charts/nvidia-recovery`. The chart name
does not affect the names of deployed resources, which derive from the Helm
release name. Operators build service images from the matching source using
`scripts/build-images.sh`, choose their registry, and configure the chart with
those images. No executable or container image is published here. Advice Operator
and private Attribution source are excluded. Attribution is enabled by default;
public source/client publication and native amd64 end-to-end qualification remain
release prerequisites. See [Attribution](docs/public-api/optional-attribution.md).

## Version and support policy

Maintainers release when a reviewed change set is ready; there is no fixed
cadence. Use SemVer `vMAJOR.MINOR.PATCH` tags (or explicit prerelease suffixes).
While versions are `0.x`, minor releases may break documented APIs/configuration;
patch releases should preserve compatibility. Notes must describe Gateway
HTTP/gRPC and Adapter contract changes, migrations, deployment inputs and limits.
The most recent released minor line receives fixes; older-line backports require
a maintainer decision based on impact. Report security issues through
[SECURITY.md](SECURITY.md).

## Prepare a release

1. Review PRs since the previous release and curate `docs/releases/v<VERSION>.md`
   from their user impact and CHANGELOG. GitHub's draft release-note generator can
   help enumerate PRs; maintainers review the result. Commit notes and chart
   `version`/`appVersion` together. Never overwrite a published version.
2. Review dependency changes, vulnerabilities and licenses using committed Go
   modules, `uv.lock`, JavaScript locks, image inputs and the chart lock. Dependabot
   is the single updater; its PRs follow ordinary SHA approval, CI and human review.
   Minor/patch updates are grouped per ecosystem. Charts and image inputs remain
   manually reviewed to preserve deliberate pins, especially MySQL 13.0.4. After
   changing dependencies, run `helm dependency update deploy/helm/nvidia-recovery`, review
   and commit `Chart.lock`, and refresh the license inventory. Packaging requires
   the lock and uses `helm dependency build`.
3. Inspect actual chart dependencies, including disabled and nested charts. Retain
   license/NOTICE material and update [the chart inventory](deploy/helm/nvidia-recovery/THIRD_PARTY_NOTICES.md).
   Bundled MinIO has AGPLv3 material; license-owner clearance remains required.
   Source headers, locks and successful packaging do not establish compatibility
   or absence of vulnerable dependencies. Record findings, fixes or accepted
   exceptions in versioned notes before approval.
4. Confirm mandatory public code checks passed for the selected commit and review
   build-input availability. Run this local gate from a committed checkout with
   Python 3.12+ and Helm 3.17.3:

   ```bash
   python3 -m unittest discover -s scripts/release -p 'test_*.py'
   python3 scripts/release/package.py package v0.2.2 /tmp/release-first
   python3 scripts/release/package.py package v0.2.2 /tmp/release-second
   diff -r /tmp/release-first /tmp/release-second
   ```

   Output directories must be empty. Source comes from `git archive` of one commit;
   uncommitted files are not inputs. Committed chart version/appVersion must match
   the requested release. Locked Helm dependencies are built inside that
   extracted archive with isolated Helm configuration. Exact normalized archives
   are inspected; the chart is linted and rendered locally. Tar/gzip metadata is
   normalized and two builds must match byte for byte. This proves reproducibility
   with the same tools and fetched dependencies; `Chart.lock` locks resolution,
   not upstream archive bytes. Investigate changed upstream content. The gate
   invokes no scheduler, cluster, provider, image build or runtime recovery scenario.
5. Keep runtime qualification explicitly unverified without separately authorized
   evidence. Live recovery qualification is excluded from this work. Secret
   scanning/removal, cleanup, rotation and history rewriting are explicitly
   skipped; do not report them as passed or make them acceptance gates.

## Maintainer publication

Publication and settings require a separate maintainer decision. Protect `v*`
tags and reviewed main, configure required reviewers on `helm-release`, prevent
self-review, and restrict that environment to protected release tags. Prevent
tag updates/deletion for all actors using the separate no-bypass rule in the
[settings runbook](docs/readiness/2026-09-09-github-ruleset-commands.md#5-verify-release-tag-protection).
An environment name in YAML does not configure these controls.

An authorized protected tag triggers `tag-release.yml`. It requires the source
commit to be reachable from main and the latest public main push CI run for that
exact SHA to have completed successfully. It runs the archive gate and uploads
its artifacts. After environment approval it rechecks that exact-SHA CI result
and requires the current remote tag to resolve to the qualified source commit.
Both `pull-request-and-main.yml` and the public schema/link/render workflow
`docs.yml` must succeed for that main push; a missing run blocks publication.
The environment-gated job downloads and verifies the same bytes/source SHA,
pushes that chart to OCI and creates a GitHub Release containing versioned notes,
both archives, `release.json`, `version` and `SHA256SUMS`. Publication never
repackages. The tagged public E2E job runs independently and reports its result
without blocking publication. Private-provider qualification is separate and is
not a public release prerequisite.
No release workflow or live recovery qualification is executed as part of this
implementation. Public code checks and reviewer clearance remain prerequisites.

Existing nightly charts remain development artifacts after their existing E2E
job, not qualified tagged source releases. Tagged publication refuses duplicate
chart versions. If OCI succeeds but GitHub publication fails, inspect remote
digests and qualified assets before manually completing the GitHub release;
do not repackage or overwrite OCI content on a blind retry.

After authorized publication, download every listed asset anonymously and run
`sha256sum -c SHA256SUMS`. Compare an OCI-pulled chart with the release chart and
record its OCI digest and URLs. Internal visibility/private package permissions
do not prove anonymous access. SHA-256 checksums detect byte changes, not signer
identity. Chart signatures/provenance remain deferred; executable-malware scanning
is inapplicable to this source/chart distribution. No DSX waiver or public-launch
readiness is implied by these applicability decisions.
