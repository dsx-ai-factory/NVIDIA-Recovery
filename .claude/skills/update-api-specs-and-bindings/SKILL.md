---
name: update-api-specs-and-bindings
description: Update the job manager http endpoint openapi spec and api-gateway Go bindings
---

# Update API Specs and Bindings

## Goals
This skill defines the steps to turn job manager's interface changes (i.e., data classes, http endpoints) into openapi
spec. Then use the openapi spec to generate Go bindings in api-gateway service. 

After these steps, the interface changes will be available for external consumption.

## Steps

**Step 1**: Start job manager locally and make sure the http endpoints are availables

Run this step to check if job manager is already running. It should return http status code 200 if the job manager is 
running ok locally.

```bash
curl -s --noproxy '*' -i http://localhost:8080/status
```

If this command fails, start the job manager dependency services with docker compose. In `job-manager` directory, start
the dependency services with:

```bash
docker compose -f docker-compose.yaml up -d
```

Then start job manager itself:
```bash
uv run python -m job_manager.main
```
Note that after every interface change, local job manager needs to be restarted to pick up the changes.

**Step 2**: Query job manager's /openapi.json for the updated openapi spec and use it to overwrite the existing one

When in job-manager directory
```bash
curl --noproxy '*' http://localhost:8080/openapi.json | jq > job-manager-openapi-3-1-schema.json
```

**Step 3**: Generate api-gateway Go bindings

Change to api-gateway directory, then run
```bash
./scripts/generate-job-manager-bindings.sh
```