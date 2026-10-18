# NVIDIA Recovery Dashboard

The web dashboard for monitoring workloads and recovery in NVIDIA Recovery.

## Setting up

* Install [NodeJS](https://nodejs.org/en), v20.17.0
* Install pnpm by following [this](https://pnpm.io/installation).
* Install dependency packages with `pnpm install`.

## Running in local dev mode

If you don't use the authentication then you can ignore this part
* start docker, if you don't use the authentication
* run `./start-database.sh`, which will run a postgres database in your local docker environment.
* run `pnpm run db:generate` or `pnpm run db:migrate` to build tables in your database. This is only needed for the first time you run this project or there is any database schema updates.

Start your dev server
* Run `pnpm run dev` to start your local server on port `3000`, once up visit `http://localhost:3000`, by default the
workflow overview page is hosted at http://localhost:3000/dashboard/workflow

## Optional base path

The dashboard is built for root-relative routes by default. To build it under a
prefix, set one build-time variable:

```bash
BASE_PATH=/mission-control/recovery-engine corepack pnpm build
```

The value must start with `/` and must not include a trailing slash. Docker
builds accept the same value through
`--build-arg BASE_PATH=/mission-control/recovery-engine`.

For Helm deployments, leave `app.base_path` empty for the default root routes.
When deploying a prefixed image, set `app.base_path` to the exact `BASE_PATH`
used for that image. The chart routes ingress traffic and constructs NextAuth
URLs from that value; it cannot change the base path already compiled into an
image. If authentication is configured outside the chart, set `NEXTAUTH_URL` to
the site origin for a root build or to the full
`https://host.example/<base-path>/api/auth` endpoint for a prefixed build.
* Changes to the default configurations like the job manager URL should be made to the `.env` file.

## Optional Grafana trace links

To show trace links, set `GRAFANA_TRACE_URL_ENABLED=true`, `GRAFANA_URL` to
an operator-provided HTTP(S) Grafana base URL (including any subpath), and
`GRAFANA_DATA_SOURCE_ID` to the Tempo datasource UID in that installation.
Optionally set `GRAFANA_ORG_ID` to select an organization; when omitted, Grafana
uses the signed-in user's organization. The Helm equivalents are the same names
under `app`. There is no default endpoint, datasource UID, or organization.
Links are omitted when the endpoint or datasource is missing.

The shipped Grafana dashboards require selecting datasources from the local
Grafana installation. Query dropdowns populate from those datasources rather
than shipping saved infrastructure values. The service-tracing dashboard also
requires entering cluster and environment filters for the deployment.
