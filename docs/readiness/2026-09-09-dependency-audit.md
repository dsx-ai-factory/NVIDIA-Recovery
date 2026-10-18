# NVIDIA Recovery dependency audit — 2026-09-09

## Outcome

**Completed with open findings; this is not dependency clearance.** OSV-Scanner v2.5.1 (osv-scalibr 0.5.2) queried the current OSV database against exact committed inputs from `b736d1a46826929884a521f27730e782eab29640` in `/private/tmp/nvidia-recovery-dsx-release-impl-20260909`. The vulnerability run completed in approximately 2.2 seconds with exit code 1 and structured findings, without extraction/query errors in its captured log. The same-input license metadata run completed in approximately 4.3 seconds. No source or dependency was changed.

Primary result: **948 reported package occurrences, 39 affected source/package/version tuples, 118 package/advisory-group occurrences, 104 distinct selected advisory IDs**. Counts use OSV alias groups, so GHSA/GO/PYSEC aliases of the same advisory are not counted twice for a package. A package in two locks or at two versions can contribute multiple occurrences. Scores are scanner-provided values; blank or zero is not proof of low risk.

| Input | Reported packages | Affected package/version tuples | Advisory groups |
| --- | ---: | ---: | ---: |
| `api-gateway/go.mod` | 111 | 6 | 12 |
| `dashboard/pnpm-lock.yaml` | 651 | 20 | 52 |
| `scripts/docs/package-lock.json` | 12 | 1 | 26 |
| `slurm-advisor-controller/go.mod` | 33 | 3 | 5 |
| `uv.lock` | 141 | 9 | 23 |

The scanner reported 950 extracted packages before its normalization/filtering, 948 package occurrences in JSON, and one local/unscannable-package filter message. Its filesystem log confirms **0 directories visited and exactly 5 input inodes visited**. The generic `Starting filesystem walk for root: /` log line does not indicate a whole-filesystem scan: only explicit lockfile arguments were used.

## Prioritized disposition

These priorities concern follow-up review/remediation, not demonstrated exploitation. No advisory below has an exploit path proven by this audit.

1. **P1 — adjudicate the dashboard `fs@0.0.2` dependency before clearance.** [MAL-2025-21003](https://osv.dev/vulnerability/MAL-2025-21003) flags npm `fs` as malicious across its reported range; no fixed version is supplied. It is a direct `devDependency` in `dashboard/package.json`. The scanner did not download, execute, or inspect this package, so the exact tarball content and execution are unverified. Review whether this dependency should be removed in a focused follow-up commit within this stack; do not infer compromise from the database match. The source's `import fs from "fs"` alone is not evidence that the npm dependency executed.
2. **P1 — qualify and update the Next.js/sharp image stack.** `next@16.3.0-canary.7` matches [GHSA-2xp9-vwfh-vxw4](https://osv.dev/vulnerability/GHSA-2xp9-vwfh-vxw4), score 9.5, with an advisory fixed boundary at 16.3.3 (15.5.24 on the other supported line). `sharp@0.34.5` additionally matches [GHSA-rgj7-g3m4-5g8c](https://osv.dev/vulnerability/GHSA-rgj7-g3m4-5g8c), fixed at 0.35.4, and GHSA-f88m-g3jw-g9cj. The checked-in sign-in page uses `next/image`, and `next.config.mjs` permits `picsum.photos/seed/**`; this establishes an image-optimization surface, **not that attacker-controlled AVIF reaches vulnerable code**. The other critical Next.js match, GHSA-p293-qw3h-jr36, requires a Windows-hosted server; treat it as platform-conditional rather than evidence of a Linux deployment exploit.
3. **P1 — review the active gRPC transport dependency.** Both Go modules select `google.golang.org/grpc@1.83.0`; [GHSA-vp52-pcj8-j9qc](https://osv.dev/vulnerability/GHSA-vp52-pcj8-j9qc) describes HTTP/2 DATA-fragment heap exhaustion and is fixed at 1.83.1. The Gateway constructs `grpc.NewServer` and the controller constructs a gRPC client, making this a relevant runtime dependency. No affected stream/exploit call chain was tested. The two additional gRPC matches concern xDS and require separate applicability checks; 1.83.2 is an advisory boundary on the 1.83 line for the missing-authority crash.
4. **P1 — qualify the build-tool trust boundary.** Docs pin `pnpm@10.8.0`, with **26 advisory groups**, including [GHSA-379q-355j-w6rj](https://osv.dev/vulnerability/GHSA-379q-355j-w6rj) (script-execution policy bypass, fixed at 10.26.0) and [GHSA-2phv-j68v-wwqx](https://osv.dev/vulnerability/GHSA-2phv-j68v-wwqx) (command injection, fixed at 10.27.0). Later listed issues have 10.x fixed boundaries through 10.34.5. These are package-manager/build risks, not a deployed HTTP service finding. Review one compatible maintained tool version against **all** listed advisories and update related pins together. This audit did not inspect credentials or run a secret scan.
5. **P2 — address Python workspace and authentication findings with compatibility review.** `mako@1.3.10` matches [GHSA-v92g-xgxw-vvmm](https://osv.dev/vulnerability/GHSA-v92g-xgxw-vvmm) (fixed 1.3.11) and Windows-specific GHSA-2h4p-vjrc-8xpq (fixed 1.3.12). The lock also flags `starlette@0.52.1`, `python-multipart@0.0.22`, `pyjwt@2.12.1`, `cryptography@49.0.0`, `requests@2.32.5`, `idna@3.11`, Click and pydantic-settings. The repository caps Starlette below 0.53 and cryptography below 50; the reported fixes cross those bounds, so a blanket patch-only upgrade cannot be assumed. `next-auth@4.24.13` has three matches with structured fixed boundaries at 4.24.15. Its critical email-normalizer advisory requires the email/magic-link provider; the inspected source selects Azure AD or Credentials instead, so **that specific provider prerequisite is not present in the checked source**. Other NextAuth issues remain open for applicability review.
6. **P2 — disposition remaining tool/transitive findings.** `kin-openapi@0.132.0` has four matches, including GHSA-r277-6w6q-xmqw (9.1; fixed 0.144.0), and `oapi-codegen/v2@2.5.0` matches GHSA-rjwr-m7qx-3fjr (fixed 2.7.1). The module declares oapi-codegen as a Go tool. This audit did not establish kin-openapi's vulnerable validation/authentication function as a serving path. Dashboard transitive/dev matches include fast-uri, js-yaml, undici, PostCSS, nanoid, ws, vitest, and others; all exact versions, IDs and fixed boundaries are in `findings.csv`.

**Disposition for every finding remains open:** owner to remediate and validate, or record a specific reviewed applicability/exception decision. No exception is accepted by this report. No automatic fixes, dependency resolution, builds, source execution, or package lifecycle scripts were run. Suggested version boundaries come from advisories and are not compatibility-tested recommendations.

## Go checksum and toolchain supplement

The official scanner supports `go.mod`, not native `go.sum` input. Primary Go results therefore use declared module graphs rather than counting all historical checksum entries as runtime dependencies. To honor checksum coverage, a separate documented OSV custom inventory queried **269 API Gateway checksum package/version pairs, 84 controller pairs, and `stdlib@1.25.9`**. `/go.mod` checksum suffixes were deduplicated; no module source was downloaded.

The supplemental scan succeeded with findings: **273 deduplicated reported package entries, 375 package/advisory groups**, including 20 for stdlib and 74 additional selected non-stdlib advisory IDs absent from primary findings. They are intentionally excluded from the primary count: older `go.sum` versions need not be selected by the build. Original per-module provenance remains in `go-checksums.osv-scanner.json`; OSV collapses output provenance to the custom input file.

The primary scanner did not emit a stdlib package, so Go 1.25.9 was queried explicitly. It is both the modules' declared minimum and the checked-in main CI `GO_VERSION`, **not a verified release compiler**. Matches include GO-2026-4918 (HTTP/2 transport loop, fixed 1.25.10), GO-2026-5037 (X.509 hostname parsing, fixed 1.25.11), and GO-2026-6089 (HTTP/2 ReadHeaderTimeout handling, fixed 1.25.13); the full 20-entry set is retained. Review an appropriate maintained patch baseline; no actual binary/toolchain was scanned.

## License metadata and distribution boundary

The same five inputs were scanned with OSV's `--licenses` metadata option using deps.dev, with no license allowlist or policy waiver. The metadata contains **14 UNKNOWN expressions, 9 non-standard expressions and 1 package entry without license metadata**, plus LGPL-3.0-or-later expressions for nine Sharp/libvips platform-package entries and MPL expressions. Expressions can coexist on one package; these are metadata occurrence counts, not legal conclusions. All review items are in `license-review-items.json`; raw data and count summary are retained.

Some UNKNOWN entries are this repository's local Python workspace packages rather than external dependencies; `fern-api@5.119.0` is also UNKNOWN in deps.dev. Do not treat missing metadata as an absent license. Review the actual upstream text and package identity where needed. This run did not download or inspect license texts from package archives.

PR4's tracked Helm notices identify bundled MinIO chart 5.4.0 under AGPLv3 and retain its license/NOTICE. That already-recorded license-owner clearance remains open. Sharp/libvips lock entries describe dependencies operators may install/build; they do **not** establish that NVIDIA's source/Helm release publishes those binaries. This audit grants no distribution clearance and performs no legal compatibility determination.

## Coverage and exact limitations

- Inputs are snapshots of `b736d1a46826929884a521f27730e782eab29640`, with SHA-256 hashes in `input-manifest.json`. Results become stale after lock/toolchain changes; no later commit is covered.
- Scanner selection was explicit, with Go/Rust call analysis and transitive manifest resolution disabled. There was no recursive repository scan, no installed-package scan, no secret detection, and no image or executable scan. `node_modules`, `.venv`, caches, removed Advice Operator, private provider source, and unrelated worktrees were excluded by never supplying them as scanner inputs.
- **Zero exploit paths are confirmed reachable.** Static usage above distinguishes relevant service/tool declarations and explicit platform/provider prerequisites. It does not replace call-graph, bundle, configuration, platform or deployment validation. No live qualification was performed.
- OSV data includes build/test and platform-optional lock entries. No platform filter, per-service Python install graph, bundler tree-shaking analysis or selected Go binary graph was computed. Third-party database coverage and advisory precision can be incomplete; no-match packages are not certified safe.
- `deploy/helm/nvidia-recovery/Chart.lock` and `api-gateway/buf.lock` were fingerprinted but are not supported vulnerability inputs in this workflow. Helm chart templates/transitive archives, Buf modules/plugins, external container base images, GitHub Actions and the separately downloaded Fern preview renderer bundle were **not** vulnerability-audited. The bundled implementation inside the `fern-api` npm tarball is not expanded by its twelve-entry package-lock scan.
- Grafana tooling `requirements.txt` contains lower bounds (`pyyaml>=6.0`, `requests>=2.31`) rather than an installed-version lock; it was fingerprinted but not resolved or represented as a successful package-version scan.
- Go checksum results are historical-presence evidence; Go stdlib results apply to the declared 1.25.9 baseline. The local replacement of the API Gateway module is first-party code and has no independent third-party package advisory assessment here.
- Vulnerability and license queries required no authentication and completed without an access blocker. Results and scripts were written only under this audit directory. The reviewer made no worktree edits. Concurrent parent documentation edits appeared by completion; every audited input still matches the recorded snapshot hash (see `final-input-check.json`).

## Reproduction and retained evidence

The checked-in evidence includes [all primary findings](dependency-audit-2026-09-09/findings.csv),
[input hashes](dependency-audit-2026-09-09/input-manifest.json), and
[license metadata review items](dependency-audit-2026-09-09/license-review-items.json).
Full raw scanner outputs, commands and the checksum supplement remain local at
`/private/tmp/dsx-dependency-audit-20260909/`; the inventory below refers to that directory.
Secret scanning/removal remains explicitly skipped, independently of this dependency review.

- `scanner-release.json`, `osv-scanner_SHA256SUMS`, `scanner-version.txt`: official v2.5.1 selection and binary hash verification. SLSA provenance was not separately verified.
- `command.json`, `scan-status.json`, `scan.stdout.txt`, `scan.stderr.txt`, `osv-results.json`: exact primary invocation, timing/status, coverage diagnostics and complete advisory payloads.
- `findings.csv`, `grouped-findings.json`: all 118 primary package/advisory groups with IDs, aliases, package-specific fixed boundaries and disposition.
- `go-checksums-command.json`, `go-checksums-status.json`, `go-checksums-results.json`, `go-checksums-findings.json`: separate historical-checksum and declared-toolchain supplement.
- `license-command.json`, `license-status.json`, `osv-license-results.json`, `license-summary.json`, `license-review-items.json`: metadata-only license run.
- `source-evidence-manifest.json`, `evidence/`: bounded source/manifest evidence used for applicability notes.

Documentation consulted before execution: [official explicit-input/call-analysis usage](https://google.github.io/osv-scanner/usage/scan-source), [supported formats/custom inventory](https://google.github.io/osv-scanner/supported-languages-and-lockfiles/), [license metadata behavior](https://google.github.io/osv-scanner/usage/license-scanning/), [official release v2.5.1](https://github.com/google/osv-scanner/releases/tag/v2.5.1). Context7 was queried first; its older examples were supplemented by current official docs and the downloaded version's `scan source --help`.
