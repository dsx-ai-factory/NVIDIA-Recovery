# Grafana Dashboard Update Tool

Push local dashboard JSON files to a Grafana instance via the
[Dashboard HTTP API](https://grafana.com/docs/grafana/latest/http_api/dashboard/).

## Prerequisites

```bash
pip install -r tools/requirements.txt
```

## Authentication

The script reads a Grafana API token from the `GRAFANA_TOKEN` environment
variable. The token should be a **Service Account** token (or legacy API key)
with `Editor` or `Admin` permissions on the target folder.

```bash
export GRAFANA_TOKEN="<grafana-service-account-token>"
```

Dry-run validation does not connect to Grafana and does not require a token.

## Configuration

All persistent settings live in `tools/config.yaml`:

```yaml
grafana:
  url: ""              # or set GRAFANA_URL / --grafana-url
  org_id:               # or set GRAFANA_ORG_ID / --org-id
  folder_uid: ""       # target folder UID (empty = General folder)

defaults:
  overwrite: true                                       # overwrite existing dashboards
  message: "Updated via grafana-dashboard-update tool"  # version history message
  dashboards_dir: "../"                                 # default directory for --dir
```

Deployment-specific Grafana URLs, organization IDs, and folder UIDs are
operator configuration and are deliberately empty in the checked-in file.
`GRAFANA_FOLDER_UID` or `--folder-uid` may supply the target folder.

## Usage

### Update a single dashboard

```bash
python tools/update_dashboard.py -f heimdall-watcher-service.json
```

### Update all dashboards in a directory

```bash
python tools/update_dashboard.py -d .
```

Files prefixed with `dd-` (Datadog exports) are automatically skipped.

### Dry run (validate without pushing)

```bash
python tools/update_dashboard.py -d . --dry-run
```

### Custom commit message

```bash
python tools/update_dashboard.py -f heimdall-watcher-service.json \
  -m "Fix poller metric names"
```

### Override folder or config path

```bash
# Push to a specific Grafana folder
python tools/update_dashboard.py -f dashboard.json --folder-uid abc123

# Use alternate config
python tools/update_dashboard.py -f dashboard.json -c /path/to/config.yaml
```

### Prevent accidental overwrites

```bash
python tools/update_dashboard.py -f dashboard.json --no-overwrite
```

## CLI Reference

| Flag | Description |
|------|-------------|
| `-f, --file PATH` | Push a single JSON dashboard file |
| `-d, --dir PATH` | Push all `*.json` files in directory (excludes `dd-*`) |
| `-c, --config PATH` | YAML config file (default: `tools/config.yaml`) |
| `-m, --message TEXT` | Version history commit message |
| `--grafana-url URL` | Override the Grafana URL from environment or config |
| `--org-id ID` | Override the organization ID from environment or config |
| `--folder-uid UID` | Override target folder UID from environment or config |
| `--no-overwrite` | Fail if dashboard already exists |
| `--dry-run` | List files without pushing |

## How It Works

1. Reads connection settings from `tools/config.yaml`
2. Reads the API token from `$GRAFANA_TOKEN`
3. Loads each dashboard JSON file
4. Sets `dashboard.id` to `null` so Grafana resolves identity by `uid`
5. POSTs to `POST /api/dashboards/db` with `overwrite: true`
6. Reports status, version number, and URL for each dashboard

## CI/CD Integration

Run the same commands from your CI provider, with `GRAFANA_TOKEN` supplied
through its environment configuration. From the repository root:

```bash
pip install -r deploy/dashboards/grafana/tools/requirements.txt
python deploy/dashboards/grafana/tools/update_dashboard.py -d deploy/dashboards/grafana/
```
