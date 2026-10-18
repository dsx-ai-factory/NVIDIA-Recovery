# NVIDIA Recovery documentation

The Fern site publishes the curated Markdown in `pages.json`, the API Gateway
HTTP reference, and the Attribution Adapter HTTP reference. The Gateway guide
also covers its gRPC contract. Job Manager's internal generated client is not a
third public API.

Edit the source Markdown in the repository root and `docs/`. The root `README.md`
is the site overview, `ARCHITECTURE.md` is the architecture guide, and
`docs/README.md` is the documentation hub. The same files are readable on GitHub;
there is no separate Fern copy to maintain. `prepare.py` stages ignored MDX files
and resolves curated links to site routes, and other repository links to GitHub.
No internal plans or readiness reports are in the site navigation.

Navigation in `fern/docs.yml` groups the pages into Start, Use, Understand,
Operate, Develop, and Reference. Every section uses `skip-slug: true` so grouping
does not change page URLs or the `gateway-reference` and `attribution-reference`
API prefixes. Keep each page's source path, slug, and meaningful rendered-text
marker in `pages.json` aligned with its navigation entry. Standard Markdown,
relative repository links, and fenced Mermaid diagrams work in both GitHub and
Fern. Do not edit `fern/generated/` directly.

## Local checks

Use Node.js 22 or newer and Python 3.12 with the repository's locked public
Adapter environment. Fern 5.152.0 and pnpm 10.8.0 are pinned in this directory's
npm lockfile; `fern.config.json` pins the same CLI version.

From the repository root:

```bash
npm ci --prefix scripts/docs --ignore-scripts --no-audit --no-fund
uv run --locked --package attribution-adapter python scripts/docs/export-openapi.py --check
npm --prefix scripts/docs run check
npm --prefix scripts/docs test
npm --prefix scripts/docs run render
```

The publication guard uses PyYAML from the Adapter's existing `dev` dependency
group, included by default by `uv run --package attribution-adapter`. It is a
documentation tool dependency, not a runtime requirement of the Adapter. Do not
run `publish-check.py` with `--no-dev`; a clean invocation with the CI-pinned uv
version must reach the hosting guard without installing extra dependencies.

The schema export depends on the provider-free `attribution_adapter.main:app`
import boundary. It uses `app.openapi()` and refuses private FACT-client imports;
it does not start the server, execute lifespan, or construct Settings/provider
services. After public API changes, run the same command without `--check` and
review the generated JSON. The Gateway uses its existing authoritative Swagger
file directly. Regenerate and verify it through the owning targets with Buf
1.54.0 and the pinned remote plugins/`buf.lock`:

```bash
make GO_PACKAGES=api-gateway generate-go
make GO_PACKAGES=api-gateway check-generated-go
```

The existing Go generated-artifact gate owns that check. No independent Gateway
schema copy or unpinned schema converter is introduced here.

`check.mjs` runs `fern check --local --json` and treats diagnostics as failures,
except Fern's known warning that the Gateway input is Swagger 2.0. Fern itself
can warn and exit zero on unresolved OpenAPI references, so using plain
`fern check` is insufficient. The negative tests introduce broken links, a bad
schema reference and invalid MDX in disposable fixtures to prove rejection.

The render command starts a real local `fern docs dev`, fetches every page in
`pages.json` and Gateway workload/audit operations plus the Adapter operation,
strips scripts/styles, and requires the rendered text/schema content. HTML and
the renderer log are saved under `scripts/docs/render-output/`. This is
server-rendered markup verification,
not a browser screenshot or visual review. Search, SEO and hosted authentication
are outside local-preview qualification. No live API or scheduler is called.

The preview child explicitly omits `FERN_TOKEN`; the renderer rejects even an
empty token variable with HTTP 500. It selects free local frontend/backend
ports and terminates only its own process group. The first preview downloads
Fern's renderer bundle and dependencies.
Fern can refresh that cached bundle independently of the pinned CLI: the local
render is a smoke test, not a hermetic or byte-reproducible site build. The
public Adapter JSON export and curated page preparation are deterministic.

For interactive editing, run `npm --prefix scripts/docs run dev`. Re-run page
preparation after changing an authoritative Markdown guide.

## Fern theme and hosted previews

`fern/docs.yml` selects NVIDIA's shared Fern theme with `global-theme: nvidia`
and `fern/fern.config.json` names the `nvidia` Fern organization. The files in
`fern/components/` and `fern/main.css` are NVIDIA's standard
theme sources, copied unmodified for reference; the site does not load them
directly. Do not restyle the site locally. Changes to the shared theme belong in
the NVIDIA Fern configuration.

Pull requests copied to `pull-request/<n>` by copy-pr-bot get a hosted preview
through two workflows:

1. `fern-docs-preview-build.yml` runs `prepare.py` and uploads `fern/`, the
   Gateway Swagger file and pull request metadata. It has no secrets.
2. `fern-docs-preview-comment.yml` downloads that artifact, runs
   `fern generate --docs --preview` with the `DOCS_FERN_TOKEN` secret, and
   posts or updates one comment with the preview and the pages whose source
   Markdown changed. It never checks out the pull request.

Preparation happens in the first workflow because the curated guides are staged
from the repository root and `docs/`, not authored under `fern/`. The comment
workflow only runs once it exists on the default branch, and it needs a
`DOCS_FERN_TOKEN` secret from `fern token` for the `nvidia` organization. Both
workflows are preview-only; the `Documentation` workflow remains the
validation gate.

## Hosting is pending

There is no allocated Fern instance, docs.nvidia.com path, or publication
token for this site. `instances: []` is deliberate, and the publication guard
refuses to run until exactly one instance is configured. Do not add an invented
live URL to the site or README. The scaffold does not configure GitHub
environment protection.

Before activating publication, the maintainer must obtain the actual Fern
origin, docs.nvidia.com subpath, reverse-proxy integration and Fern access.
Fern's [subpath hosting instructions](https://buildwithfern.com/learn/docs/preview-publish/setting-up-your-domain)
require matching base paths on the instance `url` and `custom-domain`, plus the
owning infrastructure's reverse proxy. Confirm any shared-domain/multi-source
and theme requirements with that owner; DNS alone does not allocate a subpath.

Then add exactly one approved instance, following NVIDIA's standard shape (a
`*.docs.buildwithfern.com` `url`, a `docs.nvidia.com` `custom-domain` with the
same subpath, and `multi-source: true`), and set the `DOCS_NVIDIA_URL` Actions
variable to the allocated HTTPS URL.
Configure the `docs-publish` environment with required reviewers, prevention of
self-review and protected-main deployment restrictions. Put `DOCS_FERN_TOKEN` in
that environment, and enable the `DOCS_PUBLISH_ENABLED` repository variable
only after those controls and hosting inputs are reviewed.

Publication is a manual workflow dispatch from protected `main`, with the
publish input explicitly selected, after config/schema/MDX/negative/render checks
pass. `publish-check.py` rejects the unallocated configuration, mismatched
subpaths and unprotected refs. Tokens are supplied only to the final publish
step. After publication is
authorized, the owner must verify the actual hosted URL and reverse-proxy
behavior; local rendering does not establish that readiness.

Research: [Fern local preview](https://buildwithfern.com/learn/docs/preview-publish/preview-changes),
[Fern CLI commands](https://buildwithfern.com/learn/cli-api-reference/cli-reference/docs-commands),
[FastAPI OpenAPI export](https://fastapi.tiangolo.com/reference/fastapi/#fastapi.FastAPI.openapi).
