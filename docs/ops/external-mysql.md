# Connect external MySQL with TLS

This guide assumes an external MySQL service with TLS already configured as
part of production readiness. It explains how to connect Job Manager and
Notifications to that service when replacing the bundled evaluation database.
For database TLS setup, see MySQL's
[encrypted-connections guide](https://dev.mysql.com/doc/mysql-security-excerpt/8.0/en/using-encrypted-connections.html)
for server and client configuration.

Both Job Manager and Notifications need their own connection
settings, password Secret references, and CA mounts. Retain the source revision,
images, shell variables, and site values from [backend installation](../operators/installation.md).

The current clients use **CA verification** (`VERIFY_CA`): they verify the
server certificate chain against the configured PEM bundle, with hostname
checking disabled. This procedure does not enable hostname verification or
client-certificate mTLS. Both clients default to TLS disabled, so set the TLS
options explicitly and require secure transport on the database server.

## Prepare the database and credentials

Use a MySQL endpoint reachable from both application Pods, with service accounts
that can initialize schemas and perform normal reads/writes. The example uses
separate `nvidia_recovery_jobs` and `nvidia_recovery_notifications` schemas and accounts.
Configure MySQL with `require_secure_transport=ON` and obtain its trusted CA
bundle. Application startup initializes fresh schemas; unsupported or
unversioned existing schemas are rejected.

The example references these existing password Secrets in the release namespace:

| Secret | Key | Value |
| --- | --- | --- |
| `nvidia-recovery-jobs-db` | `password` | Password for the `nvidia_recovery_jobs` account |
| `nvidia-recovery-notifications-db` | `password` | Password for the `nvidia_recovery_notifications` account |

Mount the public CA bundle through a ConfigMap. Replace the local certificate
path with the bundle supplied by your database operator:

```bash
kubectl --context "$HEIMDALL_KUBE_CONTEXT" -n "$RELEASE_NAMESPACE" \
  create configmap nvidia-recovery-mysql-ca --from-file=ca.pem=/path/to/mysql-ca.pem
```

## Configure both clients

Save the following as `external-mysql-values.yaml`, replacing the example
endpoint, accounts, schemas, and Secret names with your site values:

```yaml
mysql:
  enabled: false
global:
  mysql:
    secret:
      create: false

job-manager:
  credentials:
    mysql_secret_name: nvidia-recovery-jobs-db
    mysql_secret_key_name: password
  storage:
    mode: MYSQL
    sql:
      host: mysql.example.com
      port: 3306
      username: nvidia_recovery_jobs
      database_name: nvidia_recovery_jobs
      ssl_enabled: true
      ssl_ca_path: /etc/mysql-ca/ca.pem
  volumes:
    - name: mysql-ca
      configMap:
        name: nvidia-recovery-mysql-ca
  volumeMounts:
    - name: mysql-ca
      mountPath: /etc/mysql-ca
      readOnly: true

notifications:
  db:
    protocol: mysql+pymysql
    host: mysql.example.com
    port: "3306"
    user: nvidia_recovery_notifications
    database: nvidia_recovery_notifications
    mysql_secret_name: nvidia-recovery-notifications-db
    mysql_secret_key: password
    ssl_enabled: true
    ssl_ca_path: /etc/mysql-ca/ca.pem
  volumes:
    - name: mysql-ca
      configMap:
        name: nvidia-recovery-mysql-ca
  volumeMounts:
    - name: mysql-ca
      mountPath: /etc/mysql-ca
      readOnly: true
```

`mysql.enabled=false` disables the bundled database. Separately,
`global.mysql.secret.create=false` stops the umbrella chart from creating a
database Secret; it does **not** set either client's password reference. The
Job Manager key is `mysql_secret_key_name`, while Notifications uses
`mysql_secret_key`. A common database/account/Secret is also possible, but both
references must still be configured explicitly.

Apply this overlay last, after the quickstart and site values, on every render,
install, and upgrade. Helm replaces `volumes` and `volumeMounts` lists; merge
any existing custom entries into these lists first. The overlay retains the
application enablement and images from the site values. If the optional
dependency checker is enabled, configure its external endpoint separately;
its checks do not qualify the two clients' TLS connections.

After preparing chart dependencies as in the installation guide, render and lint:

```bash
helm lint deploy/helm/nvidia-recovery \
  --values deploy/helm/nvidia-recovery/values-quickstart.yaml \
  --values nvidia-recovery-values.yaml \
  --values external-mysql-values.yaml
helm template "$RELEASE_NAME" deploy/helm/nvidia-recovery \
  --namespace "$RELEASE_NAMESPACE" \
  --values deploy/helm/nvidia-recovery/values-quickstart.yaml \
  --values nvidia-recovery-values.yaml \
  --values external-mysql-values.yaml > /tmp/nvidia-recovery-external-mysql.yaml
```

Inspect the rendered resources: no bundled MySQL StatefulSet or umbrella
database Secret; both client Deployments reference the intended password key
and mount the CA read-only; both ConfigMaps enable TLS and point to that exact
mounted path and the intended endpoint/schema.

Install or upgrade the release using the same values order:

```bash
helm upgrade --install "$RELEASE_NAME" deploy/helm/nvidia-recovery \
  --kube-context "$HEIMDALL_KUBE_CONTEXT" --namespace "$RELEASE_NAMESPACE" \
  --values deploy/helm/nvidia-recovery/values-quickstart.yaml \
  --values nvidia-recovery-values.yaml \
  --values external-mysql-values.yaml --wait --timeout 15m
```

## Verify connections

Check both applications' startup logs for successful schema initialization and
any certificate-verification or database connection errors:

```bash
for client in job-manager notifications; do
  kubectl --context "$HEIMDALL_KUBE_CONTEXT" -n "$RELEASE_NAMESPACE" \
    logs deployment/"$RELEASE_NAME-$client" -c "$client" --since=10m
done
```

These names match the installation guide; substitute the rendered Deployment
names if you customized them. Use database connection monitoring to confirm
that both service accounts connect to the intended database over TLS. A running
Pod or a shallow health response alone does not establish database connectivity.

Verify that Job Manager can persist and retrieve a fresh job and that
Notifications can persist and query its audit events using the
[operations guide](../operators/operations.md). This checks normal application
reads and writes, including Job Manager's separate write pool when enabled.

For an optional CA-rejection check in a disposable test deployment, mount a
valid but unrelated CA for both clients and restart them. Confirm that both
startup logs report certificate-verification failures. Restore the trusted CA,
restart both clients, and repeat the connection checks above.
