# Bundled Helm chart notices

The NVIDIA Recovery umbrella chart bundles the six external dependencies in
`Chart.lock` and their transitive charts. Disabling a dependency at runtime does
not remove its files from the archive. The chart's own [LICENSE](LICENSE) applies
to NVIDIA Recovery material; it does not replace the upstream terms below.

These notices describe the inspected chart source, including bundled templates,
scripts and CRDs. They do not describe the licenses of container images or the
applications installed by those charts. The standalone NVIDIA Recovery Slurm
controller chart has no external chart dependency.

## Source and retained material

Paths are relative to the umbrella chart archive. Source links select immutable
upstream revisions inspected on 2026-09-09. Identical upstream license bytes share
one retained file; copyright notices already present in chart files remain there.

| Bundled chart path | Version | Pinned upstream source | Retained upstream material |
| --- | --- | --- | --- |
| `charts/rabbitmq` | 16.0.5 | [Bitnami rabbitmq/16.0.5](https://github.com/bitnami/charts/tree/8e3fe0b70144eb8c11df19e525c53d1b63c0c52c/bitnami/rabbitmq) | [Apache 2.0 and Broadcom notice](licenses/bitnami-charts.LICENSE) |
| `charts/rabbitmq/charts/common` | 2.31.1 | [Bitnami common/2.31.1](https://github.com/bitnami/charts/tree/d96e16aa1e2f1d42de829330f5c0dbc1c4666493/bitnami/common) | [Apache 2.0 and Broadcom notice](licenses/bitnami-charts.LICENSE) |
| `charts/mysql` | 13.0.4 | [Bitnami mysql/13.0.4](https://github.com/bitnami/charts/tree/96ecfce7c2fea20642cfc02c2bc661697022bf9e/bitnami/mysql) | [Apache 2.0 and Broadcom notice](licenses/bitnami-charts.LICENSE) |
| `charts/mysql/charts/common` | 2.31.0 | [Bitnami common/2.31.0](https://github.com/bitnami/charts/tree/3826a9e1488c12545f11cf8cb1a11d23daf90602/bitnami/common) | [Apache 2.0 and Broadcom notice](licenses/bitnami-charts.LICENSE) |
| `charts/grafana` | 9.0.0 | [Grafana grafana-9.0.0](https://github.com/grafana/helm-charts/tree/cc4a3063b6aafd3fdf94ab9662a69a1616f8edd5/charts/grafana) | [Apache 2.0](licenses/grafana-charts.LICENSE) |
| `charts/alloy` | 1.10.1 | [Alloy helm-chart/1.10.1](https://github.com/grafana/alloy/tree/3c53e3c93eba268417bcdcce4918c0bba6c572e3/operations/helm/charts/alloy) | [Apache 2.0](licenses/alloy.LICENSE) |
| `charts/alloy/charts/crds` | 0.0.0 | [Alloy CRDs at the same revision](https://github.com/grafana/alloy/tree/3c53e3c93eba268417bcdcce4918c0bba6c572e3/operations/helm/charts/alloy/charts/crds) | [Apache 2.0](licenses/alloy.LICENSE) |
| `charts/loki` | 17.2.0 | [Grafana Community loki-17.2.0](https://github.com/grafana-community/helm-charts/tree/5e343f50c0362f9fcadb08f54e1ca0a4a307953d/charts/loki) | [Apache 2.0](licenses/grafana-charts.LICENSE) |
| `charts/loki/charts/rollout-operator` | 0.49.0 | [Grafana rollout-operator-0.49.0](https://github.com/grafana/helm-charts/tree/003eea1e3684e7c49b74e8e198ca777ea5e6a594/charts/rollout-operator) | [Apache 2.0](licenses/grafana-charts.LICENSE) |
| `charts/loki/charts/rollout-operator/charts/crds` | 0.0.0 | [Rollout Operator CRDs at the same revision](https://github.com/grafana/helm-charts/tree/003eea1e3684e7c49b74e8e198ca777ea5e6a594/charts/rollout-operator/charts/crds) | [Apache 2.0](licenses/grafana-charts.LICENSE) |
| `charts/loki/charts/minio` | 5.4.0 | [MinIO chart version revision](https://github.com/minio/minio/tree/43a74029685512ce9b1b76c053d48b43fc8d64fc/helm/minio) | [GNU AGPLv3](licenses/minio.LICENSE), [MinIO NOTICE](licenses/minio.NOTICE) |
| `charts/opentelemetry-collector` | 0.124.0 | [OpenTelemetry opentelemetry-collector-0.124.0](https://github.com/open-telemetry/opentelemetry-helm-charts/tree/fac18b4a27203ab08aa028d42e428086cdd29ae7/charts/opentelemetry-collector) | [Apache 2.0](licenses/opentelemetry-charts.LICENSE) |

The Alloy source is in `grafana/alloy`, although its chart is distributed through
the Grafana chart repository. The two `crds` charts have the same name/version
but different parent paths and source content.

MinIO's upstream root license is GNU AGPLv3 and its root NOTICE is retained
verbatim. Its chart README also identifies GNU AGPLv3. The license owner must
assess the bundled MinIO source and distribution obligations before publication.
These notices neither relicense it under Apache 2.0 nor approve redistribution.

## License text provenance

The retained files are byte-for-byte copies of the following upstream files.
The four Bitnami revisions share identical `LICENSE.md` bytes. The Grafana,
Rollout Operator and Grafana Community Loki revisions share identical `LICENSE`
bytes. No separate LICENSE, NOTICE or COPYING file was found inside the inspected
upstream chart subtrees or the previous packaged chart trees; MinIO's root NOTICE
is therefore supplied alongside its root license here.

| Local file | Upstream file | SHA-256 |
| --- | --- | --- |
| `licenses/bitnami-charts.LICENSE` | [Bitnami LICENSE.md](https://github.com/bitnami/charts/blob/8e3fe0b70144eb8c11df19e525c53d1b63c0c52c/LICENSE.md) | `e455871ddb1a8b185a03bc65094a4fc917238174e101b3ed07079255ea988415` |
| `licenses/grafana-charts.LICENSE` | [Grafana LICENSE](https://github.com/grafana/helm-charts/blob/cc4a3063b6aafd3fdf94ab9662a69a1616f8edd5/LICENSE) | `75bd47da88044ab35f277475359fc2c1bcc3171ab94d721a5c1a0d33ac61deb2` |
| `licenses/alloy.LICENSE` | [Alloy LICENSE](https://github.com/grafana/alloy/blob/3c53e3c93eba268417bcdcce4918c0bba6c572e3/LICENSE) | `cfc7749b96f63bd31c3c42b5c471bf756814053e847c10f3eb003417bc523d30` |
| `licenses/opentelemetry-charts.LICENSE` | [OpenTelemetry LICENSE](https://github.com/open-telemetry/opentelemetry-helm-charts/blob/fac18b4a27203ab08aa028d42e428086cdd29ae7/LICENSE) | `43070e2d4e532684de521b885f385d0841030efa2b1a20bafb76133a5e1379c1` |
| `licenses/minio.LICENSE` | [MinIO LICENSE](https://github.com/minio/minio/blob/43a74029685512ce9b1b76c053d48b43fc8d64fc/LICENSE) | `0d96a4ff68ad6d4b6f1f30f713b18d5184912ba8dd389f86aa7710db079abcb0` |
| `licenses/minio.NOTICE` | [MinIO NOTICE](https://github.com/minio/minio/blob/43a74029685512ce9b1b76c053d48b43fc8d64fc/NOTICE) | `e74f1fdf2e9de7b4efc317bee3333ed26454f24fd147527707fe4185b706703a` |

## Maintaining this record

For each dependency change, inspect the actual chart archive and all nested
chart sources, resolve their upstream revisions, check file-specific terms and
retain any applicable license and notice texts. Refresh this table and review
any changed or unresolved terms with the license owner. A matching version or
unsigned digest alone does not authenticate upstream bytes.

Release packaging and archive validation require these tracked files to be
included unchanged. Helm's [chart structure](https://helm.sh/docs/topics/charts/)
and [.helmignore rules](https://helm.sh/docs/chart_template_guide/helm_ignore_file/)
explain their packaging location. Do not remove license material through ignore
rules. Retention checks do not decide license compatibility or replace the final
source, dependency and artifact clearance review.
