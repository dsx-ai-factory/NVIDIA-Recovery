# Loki Query Performance Comparison

`loki_query_comparison.py` compares two operator-supplied LogQL queries over the
same time range. It reports timing statistics and can export results as JSON.
Run it from the `anomaly_detection_isolator` directory using the project environment.

## Configuration

The tool has no default deployment, kubeconfig, Kubernetes context, credential
secret, query, or time range. Supply these explicitly on each invocation. Secret
lookups use the namespace configured in the selected Kubernetes context.
Authenticated requests require an HTTPS Loki endpoint without embedded URL credentials;
invalid endpoints are rejected before Kubernetes secrets are fetched.

Set the shell variables below to your environment's values before running:

```bash
uv run --locked python scripts/loki_query_comparison.py \
  --kubeconfig "$KUBECONFIG" \
  --context "$KUBE_CONTEXT" \
  --loki-url "$LOKI_URL" \
  --secret-name "$LOKI_TOKEN_SECRET" \
  --secret-token-key "$LOKI_TOKEN_KEY" \
  --query1 "$QUERY1" \
  --query2 "$QUERY2" \
  --start-time "$START_TIME" \
  --end-time "$END_TIME" \
  --output comparison_results.json
```

Use ISO timestamps including a timezone; the start must precede the end. Choose
queries appropriate to your deployment's labels and schema. Queries need not
return the same data, so faster results alone do not establish equivalence.

For certificate authentication, replace the two bearer-token arguments with all
six arguments below. Certificate authentication and bearer-token authentication
cannot be selected together.

```bash
--cert-secret "$LOKI_CERT_SECRET" --cert-secret-key "$LOKI_CERT_KEY" \
--key-secret "$LOKI_PRIVATE_KEY_SECRET" --key-secret-key "$LOKI_PRIVATE_KEY_KEY" \
--ca-cert-secret "$LOKI_CA_SECRET" --ca-cert-secret-key "$LOKI_CA_KEY"
```

For multi-tenant Loki, supply `--x-scope-org-id` explicitly. The tool does not
choose a tenant on your behalf. TLS certificate verification remains enabled.

## Comparison options

- `--iterations`: runs per query, default 3.
- `--sleep-between`: seconds between runs, default 10.
- `--limit`: request limit for the simple client, default 10000.
- `--query1-name` and `--query2-name`: optional display labels.
- `--direction`: `forward` (default) or `backward`.
- `--use-adi-client`: use the production `LokiLogFetcher` with pagination. This
  client fetches pages across the time range rather than using `--limit` as a
  total result cap.
- `--output`: export timing statistics and query metadata as JSON.

`--compare-directions --use-adi-client` compares the first query in both
directions; omit `--query2` in this mode. It alternates forward and backward
runs, compares `(timestamp, log line)` multiplicities, and reports order and
performance differences. Pagination is required because a capped response can
return different subsets in the two directions. Pagination still has a finite
page limit, so check for truncation before concluding that all logs match.

## Interpreting results

The tool reports mean, median, minimum, maximum, standard deviation, and line
counts. It excludes timing samples whose z-score exceeds 2, but this method can
miss outliers in small samples. Inspect raw timings in the JSON output and
repeat comparisons to account for caching and changing backend load.

For empty results, check the supplied label selectors and time range. For
credential errors, check the selected context, namespace, secret names, and
keys. `--help` lists all supported arguments without contacting Kubernetes or
Loki.
