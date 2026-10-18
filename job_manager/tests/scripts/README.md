## Overview
This directory contains scripts that publish artificial events bus messages for job manager to consume to faciliate e2e
testing.

`job_update_event_bus_publish.py` publishes synthetic job updates to exercise the
legacy event-bus consumer. The Slurm Advisor Controller (SAC), which replaces the
former workflow controller, sends production job updates through API Gateway;
this script does not exercise that API path.
`anomaly_event_bus_publish.py` simulates the case where anomaly watcher sends job anomalies.

The event bus deployment can be the one running on local machine (e.g. with docker compose) or a remote deployment in a
k8s cluster. The connection spec is in `event_bus_connection_spec.yaml`, please update the corresponding section to point
to different event bus deployment.

To run them:
1. Install dependencies
```bash
# This goes back to the job_manager root directory where pyproject.toml is located.
cd ../..

uv sync
```
2. Execute the script in the virtual env
```bash
uv run --locked python3 tests/scripts/job_update_event_bus_publish.py <job-id> <job-attempt-index>

uv run --locked python3 tests/scripts/anomaly_event_bus_publish.py <job-id>
```

If details of the job event or anomaly event need to be changed, just change the file, save it and re-run the script.
