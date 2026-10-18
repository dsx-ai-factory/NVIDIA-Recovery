# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Runs Heimdall locally with Slurm on Slinky.
# CI will eventually use tilt ci; the scenario scripts will continue to run tests.

E2E_DIR = 'scripts/ci/e2e'
TILT_DIR = 'deploy/tilt'

load('ext://helm_resource', 'helm_resource', 'helm_repo')

INFRA_LABEL = 'infra'
STACK_LABEL = 'stack'

cluster_name = os.environ.get('CLUSTER_NAME', 'heimdall-e2e-dev')
helm_release = os.environ.get('HELM_RELEASE', 'nvidia-recovery')

def stack_workload_name(component):
    """Match the local charts' default fullname, including the provider suffix."""
    if component == 'attribution-service':
        prefix = helm_release[:43]
        return (prefix[:-1] if prefix.endswith('-') else prefix) + '-attribution-service'
    name = helm_release if component in helm_release else helm_release + '-' + component
    name = name[:63]
    return name[:-1] if name.endswith('-') else name

log_backend = os.environ.get('E2E_LOG_BACKEND', 'loki')
if log_backend not in ['loki', 'victorialogs']:
    fail('invalid E2E_LOG_BACKEND=' + log_backend + '; expected loki or victorialogs')

e2e_profile = os.environ.get('E2E_PROFILE', 'advisor-act')
if e2e_profile not in ['advisor-observe', 'advisor-act']:
    fail('invalid E2E_PROFILE=' + e2e_profile + '; expected one of: advisor-observe, advisor-act')

kubernetes_service_auth_smoke = os.environ.get('E2E_KUBERNETES_SERVICE_AUTH_SMOKE', '') != ''
if kubernetes_service_auth_smoke and e2e_profile != 'advisor-observe':
    fail('E2E_KUBERNETES_SERVICE_AUTH_SMOKE requires E2E_PROFILE=advisor-observe')

render_dir = TILT_DIR + '/tmp/' + cluster_name
slinky_slurm_tag = os.environ.get('E2E_SLINKY_SLURM_TAG', '') or '25.05-ubuntu24.04'
if '@' in slinky_slurm_tag:
    fail('E2E_SLINKY_SLURM_TAG must be a tag; use E2E_SLINKY_SACKD_IMAGE for a digest-pinned donor')
slinky_sackd_image = os.environ.get('E2E_SLINKY_SACKD_IMAGE', '')
if not slinky_sackd_image:
    if slinky_slurm_tag == '25.05-ubuntu24.04':
        slinky_sackd_image = 'ghcr.io/slinkyproject/sackd:25.05-ubuntu24.04@sha256:dd1f0b6030229b3d602e3d80e6d50cc4f9fd17b325312521dfa07376fffc7569'
    else:
        slinky_sackd_image = 'ghcr.io/slinkyproject/sackd:' + slinky_slurm_tag
slurm_shared_home = os.environ.get('SLURM_SHARED_HOME', '/var/lib/heimdall-e2e/slurm-home')

# Helm installs can take several minutes on a new Kind cluster.
service_wait_timeout = os.environ.get('SERVICE_WAIT_TIMEOUT', '')

# Attribution is enabled by default; unavailable explicitly selects degraded mode.
attribution_mode = os.environ.get('E2E_ATTRIBUTION_MODE', 'available')
if attribution_mode not in ['unavailable', 'available']:
    fail(
        'invalid E2E_ATTRIBUTION_MODE=' + attribution_mode +
        '; expected unavailable or available'
    )

attribution_source_url = os.environ.get(
    'ATTRIBUTION_SOURCE_URL',
    'https://github.com/dsx-ai-factory/attribution.git',
)
attribution_source_ref = os.environ.get(
    'ATTRIBUTION_SOURCE_REF',
    '7d6dff168bc282f471828fc8468e02de0df17480',
)
attribution_source_dir = os.environ.get(
    'ATTRIBUTION_SOURCE_DIR',
    '/tmp/heimdall-attribution-source-' + attribution_source_ref[:12],
)

tilt_upsert_timeout = os.environ.get('TILT_K8S_UPSERT_TIMEOUT_SECS', '') or service_wait_timeout or '2400'
tilt_max_parallel_updates = os.environ.get('TILT_MAX_PARALLEL_UPDATES', '') or '2'
update_settings(
    k8s_upsert_timeout_secs=int(tilt_upsert_timeout),
    max_parallel_updates=int(tilt_max_parallel_updates),
)

# An empty DEPLOY_INFRA or DEPLOY_STACK skips that part of the environment.
if os.environ.get('DEPLOY_INFRA', '1') != '':
    slurm_helm_wait_flag = '--wait'
    if 'WaitStrategy' in str(local(['helm', 'upgrade', '--help'], quiet=True)):
        slurm_helm_wait_flag = '--wait=legacy'

    cluster_label = os.environ.get('CLUSTER_LABEL', 'e2e')
    prometheus_values = render_dir + '/prometheus.yaml'
    prometheus_gpu_values = render_dir + '/prometheus-gpu.yaml'
    alloy_values = render_dir + '/alloy.yaml'
    alloy_config = render_dir + '/alloy-config.river'
    alloy_application_config = render_dir + '/alloy-application-config.river'
    slurm_values = render_dir + '/slurm.yaml'
    slurm_shared_storage = render_dir + '/slurm-shared-storage.yaml'

    cert_manager_timeout = os.environ.get('CERT_MANAGER_WAIT_TIMEOUT', '') or service_wait_timeout or '600'
    prometheus_timeout = os.environ.get('PROMETHEUS_WAIT_TIMEOUT', '') or service_wait_timeout or '1800'
    loki_timeout = os.environ.get('LOKI_WAIT_TIMEOUT', '') or service_wait_timeout or '1800'
    alloy_timeout = os.environ.get('ALLOY_WAIT_TIMEOUT', '') or service_wait_timeout or '900'
    slurm_operator_timeout = os.environ.get('SLURM_OPERATOR_WAIT_TIMEOUT', '') or service_wait_timeout or '1200'
    slurm_timeout = os.environ.get('SLURM_WAIT_TIMEOUT', '') or service_wait_timeout or '2400'

    helm_repo('jetstack', 'https://charts.jetstack.io', labels=[INFRA_LABEL])
    helm_repo('prometheus-community', 'https://prometheus-community.github.io/helm-charts', labels=[INFRA_LABEL])
    helm_repo('grafana', 'https://grafana.github.io/helm-charts', labels=[INFRA_LABEL])

    helm_resource(
        'cert-manager',
        'jetstack/cert-manager',
        namespace='cert-manager',
        flags=[
            '--version',
            os.environ.get('CERT_MANAGER_CHART_VERSION', 'v1.16.2'),
            '--set',
            'crds.enabled=true',
            '--create-namespace',
            '--cleanup-on-fail',
            '--wait',
            '--timeout',
            cert_manager_timeout + 's',
        ],
        resource_deps=['jetstack'],
        labels=[INFRA_LABEL],
    )

    # These charts need values that refer to this cluster and its local paths.
    local_resource(
        'prometheus-values',
        cmd=[
            TILT_DIR + '/render.sh',
            E2E_DIR + '/services/values/prometheus.yaml',
            prometheus_values,
            '__CLUSTER_LABEL__',
            cluster_label,
        ],
        deps=[E2E_DIR + '/services/values/prometheus.yaml', TILT_DIR + '/render.sh'],
        labels=[INFRA_LABEL],
    )
    local_resource(
        'prometheus-gpu-values',
        cmd=[
            TILT_DIR + '/render.sh',
            E2E_DIR + '/services/values/prometheus-gpu.yaml',
            prometheus_gpu_values,
            '__CLUSTER_LABEL__',
            cluster_label,
        ],
        deps=[E2E_DIR + '/services/values/prometheus-gpu.yaml', TILT_DIR + '/render.sh'],
        labels=[INFRA_LABEL],
    )

    prometheus_flags = [
        '--version',
        os.environ.get('PROMETHEUS_CHART_VERSION', '77.13.0'),
        '--values',
        prometheus_values,
        '--create-namespace',
        '--cleanup-on-fail',
        '--wait',
        '--timeout',
        prometheus_timeout + 's',
    ]
    if os.environ.get('DEPLOY_GPU_SCRAPE', '') != '':
        prometheus_flags += ['--values', prometheus_gpu_values]
    helm_resource(
        'prometheus',
        'prometheus-community/kube-prometheus-stack',
        namespace='monitoring',
        flags=prometheus_flags,
        resource_deps=['prometheus-community', 'prometheus-values', 'prometheus-gpu-values'],
        labels=[INFRA_LABEL],
        port_forwards=['3000:3000', '9090:9090'],
    )

    if log_backend == 'loki':
        helm_resource(
            'loki',
            'grafana/loki',
            namespace='monitoring',
            flags=[
                '--version',
                os.environ.get('LOKI_CHART_VERSION', '6.34.0'),
                '--values',
                E2E_DIR + '/services/values/loki.yaml',
                '--create-namespace',
                '--cleanup-on-fail',
                '--wait',
                '--timeout',
                loki_timeout + 's',
            ],
            deps=[E2E_DIR + '/services/values/loki.yaml'],
            resource_deps=['grafana'],
            labels=[INFRA_LABEL],
            port_forwards=['3100:3100'],
        )

    else:
        k8s_yaml(TILT_DIR + '/services/victorialogs.yaml')
        k8s_resource(
            workload='victorialogs',
            port_forwards=['9428:9428'],
            labels=[INFRA_LABEL],
        )

    local_resource(
        'slurm-shared-storage',
        cmd=[
            TILT_DIR + '/apply-slurm-shared-storage.sh',
            E2E_DIR + '/services/values/slurm-shared-storage.yaml',
            slurm_shared_storage,
            slurm_shared_home,
        ],
        deps=[
            E2E_DIR + '/services/values/slurm-shared-storage.yaml',
            TILT_DIR + '/apply-slurm-shared-storage.sh',
            TILT_DIR + '/render.sh',
        ],
        labels=[INFRA_LABEL],
    )
    local_resource(
        'alloy-values',
        cmd=[
            TILT_DIR + '/render.sh',
            E2E_DIR + '/services/values/alloy.yaml',
            alloy_values,
            '__SLURM_SHARED_HOME__',
            slurm_shared_home,
        ],
        deps=[E2E_DIR + '/services/values/alloy.yaml', TILT_DIR + '/render.sh'],
        labels=[INFRA_LABEL],
    )
    local_resource(
        'alloy-config',
        cmd=[
            TILT_DIR + '/apply-alloy-config.sh',
            E2E_DIR + '/services/values/alloy-config.river',
            alloy_config,
            log_backend,
        ],
        deps=[
            E2E_DIR + '/services/values/alloy-config.river',
            TILT_DIR + '/apply-alloy-config.sh',
            TILT_DIR + '/render.sh',
        ],
        resource_deps=['alloy-values', 'slurm-shared-storage'],
        labels=[INFRA_LABEL],
    )
    helm_resource(
        'alloy',
        'grafana/alloy',
        namespace='slurm',
        flags=[
            '--version',
            os.environ.get('ALLOY_CHART_VERSION', '1.10.1'),
            '--values',
            alloy_values,
            '--create-namespace',
            '--cleanup-on-fail',
            '--wait',
            '--timeout',
            alloy_timeout + 's',
        ],
        resource_deps=[log_backend, 'grafana', 'alloy-config', 'slurm-log-directories'],
        labels=[INFRA_LABEL],
    )

    local_resource(
        'alloy-application-config',
        cmd=[
            TILT_DIR + '/apply-alloy-config.sh',
            E2E_DIR + '/services/values/alloy-application-config.river',
            alloy_application_config,
            log_backend,
            'alloy-application-config',
        ],
        deps=[
            E2E_DIR + '/services/values/alloy-application-config.river',
            TILT_DIR + '/apply-alloy-config.sh',
            TILT_DIR + '/render.sh',
        ],
        resource_deps=['slurm-shared-storage'],
        labels=[INFRA_LABEL],
    )
    helm_resource(
        'alloy-application',
        'grafana/alloy',
        namespace='slurm',
        flags=[
            '--version',
            os.environ.get('ALLOY_CHART_VERSION', '1.10.1'),
            '--values',
            alloy_values,
            '--values',
            E2E_DIR + '/services/values/alloy-application.yaml',
            '--cleanup-on-fail',
            '--wait',
            '--timeout',
            alloy_timeout + 's',
        ],
        deps=[E2E_DIR + '/services/values/alloy-application.yaml'],
        # Complete the node-only rollout before starting the shared reader.
        resource_deps=['alloy', 'alloy-application-config', 'slurm-log-directories'],
        labels=[INFRA_LABEL],
    )

    helm_resource(
        'slurm-operator',
        'oci://ghcr.io/slinkyproject/charts/slurm-operator',
        namespace='slurm-system',
        flags=[
            '--version',
            os.environ.get('SLURM_OPERATOR_CHART_VERSION', '0.3.1'),
            '--create-namespace',
            '--cleanup-on-fail',
            '--wait',
            '--timeout',
            slurm_operator_timeout + 's',
        ],
        resource_deps=['cert-manager'],
        labels=[INFRA_LABEL],
    )
    local_resource(
        'slurm-priorityclass',
        cmd=['kubectl', 'apply', '-f', E2E_DIR + '/services/values/slurm-priorityclass.yaml'],
        deps=[E2E_DIR + '/services/values/slurm-priorityclass.yaml'],
        labels=[INFRA_LABEL],
    )
    local_resource(
        'slurm-values',
        cmd=[
            TILT_DIR + '/render.sh',
            E2E_DIR + '/services/values/slurm.yaml',
            slurm_values,
            '__SLURM_SHARED_HOME__',
            slurm_shared_home,
            '__SLINKY_SLURM_TAG__',
            slinky_slurm_tag,
        ],
        deps=[E2E_DIR + '/services/values/slurm.yaml', TILT_DIR + '/render.sh'],
        labels=[INFRA_LABEL],
    )

    helm_resource(
        'slurm',
        'oci://ghcr.io/slinkyproject/charts/slurm',
        namespace='slurm',
        flags=[
            '--version',
            os.environ.get('SLURM_CHART_VERSION', '0.3.1'),
            '--values',
            slurm_values,
            '--create-namespace',
            '--cleanup-on-fail',
            slurm_helm_wait_flag,
            '--timeout',
            slurm_timeout + 's',
        ],
        deps=[E2E_DIR + '/services/values/slurm.yaml'],
        resource_deps=['slurm-operator', 'slurm-priorityclass', 'slurm-shared-storage', 'slurm-values'],
        labels=[INFRA_LABEL],
        port_forwards=['2222:22', '6820:6820'],
    )
    local_resource(
        'slurm-log-directories',
        cmd=[E2E_DIR + '/provision-slurm-log-directories.sh', 'slurm'],
        deps=[E2E_DIR + '/provision-slurm-log-directories.sh'],
        resource_deps=['slurm'],
        labels=[INFRA_LABEL],
    )
    local_resource(
        'slurm-fixtures',
        cmd=[E2E_DIR + '/provision-slurm-fixtures.sh', 'slurm'],
        deps=[E2E_DIR + '/provision-slurm-fixtures.sh', 'tests/e2e'],
        ignore=['tests/e2e/**/*.py', 'tests/e2e/**/__pycache__', 'tests/e2e/**/job.sh'],
        resource_deps=['slurm'],
        labels=[INFRA_LABEL],
    )

if os.environ.get('DEPLOY_STACK', '1') != '':
    stack_deps = []
    if os.environ.get('DEPLOY_INFRA', '1') != '':
        # Wait for the services that Heimdall uses before installing it.
        stack_deps = ['prometheus', 'alloy', 'alloy-application', 'slurm-log-directories']

    stack_namespace = 'slurm'
    k8s_yaml(TILT_DIR + '/namespace.yaml')

    attribution_prepare_script = TILT_DIR + '/prepare-attribution-source.sh'
    attribution_client_build_script = TILT_DIR + '/build-attribution-adapter.sh'
    attribution_service_build_script = TILT_DIR + '/build-attribution-service.sh'
    attribution_values = TILT_DIR + '/services/attribution/stack-values.yaml'
    for path in [
        attribution_prepare_script,
        attribution_client_build_script,
        attribution_service_build_script,
        attribution_values,
    ]:
        watch_file(path)

    if attribution_mode == 'available':
        # Missing enabled source fails here before deploying a partial stack.
        local([
            attribution_prepare_script,
            attribution_source_dir,
            attribution_source_url,
            attribution_source_ref,
        ])
        custom_build(
            'attribution-adapter',
            command=[attribution_client_build_script, attribution_source_dir],
            deps=[
                'attribution_adapter',
                'recovery_libs',
                'pyproject.toml',
                'uv.lock',
                attribution_client_build_script,
                attribution_source_dir + '/service/service-app/nvidia_attribution_client',
            ],
        )
        custom_build(
            'attribution-service',
            command=[attribution_service_build_script, attribution_source_dir],
            deps=[
                attribution_service_build_script,
                attribution_source_dir + '/LICENSE',
                attribution_source_dir + '/service/service-app/Dockerfile',
                attribution_source_dir + '/service/service-app/Dockerfile.dockerignore',
                attribution_source_dir + '/service/attribution-lib',
                attribution_source_dir + '/service/service-app',
            ],
        )

    # Refresh local chart dependencies before helm() renders the NVIDIA Recovery chart.
    local(TILT_DIR + '/refresh-nvidia-recovery-deps.sh')

    # helm() does not watch every local dependency of the NVIDIA Recovery chart.
    for path in [
        'deploy/helm/nvidia-recovery/.helmignore',
        'deploy/helm/nvidia-recovery/Chart.yaml',
        'deploy/helm/nvidia-recovery/files',
        'deploy/helm/nvidia-recovery/templates',
        'deploy/helm/nvidia-recovery/values.yaml',
        'deploy/helm/anomaly-detection-isolator',
        'deploy/helm/api-gateway',
        'deploy/helm/dashboard',
        'deploy/helm/job-manager',
        'deploy/helm/loki-mcp',
        'deploy/helm/mysql',
        'deploy/helm/notifications',
        'deploy/helm/attribution-adapter',
        'deploy/helm/attribution-service',
        'deploy/helm/watcher',
        TILT_DIR + '/refresh-nvidia-recovery-deps.sh',
    ]:
        watch_file(path)

    docker_build('job-manager', '.', dockerfile='job_manager/Dockerfile')
    docker_build('watcher', '.', dockerfile='watcher/Dockerfile')
    docker_build('anomaly-detection-isolator', '.', dockerfile='anomaly_detection_isolator/Dockerfile')
    docker_build('api-gateway', 'api-gateway', dockerfile='api-gateway/Dockerfile')
    docker_build('dashboard', 'dashboard', dockerfile='dashboard/Dockerfile')
    docker_build('notifications', '.', dockerfile='notifications/Dockerfile')

    stack_values = [E2E_DIR + '/values/stack-local.yaml']
    if attribution_mode == 'available':
        stack_values += [attribution_values]
    attribution_set_values = []
    if attribution_mode == 'unavailable':
        attribution_set_values = [
            'attribution-service.enabled=false',
            'attribution-adapter.enabled=false',
            'anomaly-detection-isolator.attribution_enabled=false',
        ]
    if log_backend == 'victorialogs':
        # Apply last so the attribution overlay cannot reset the selected backend.
        stack_values += [E2E_DIR + '/values/stack-local-victorialogs.yaml']
    stack_workloads = [stack_workload_name(component) for component in [
        'anomaly-detection-isolator',
        'api-gateway',
        'dashboard',
        'job-manager',
        'notifications',
        'watcher',
        'rabbitmq',
    ]] + ['nvidia-recovery-mysql']
    if attribution_mode == 'available':
        stack_workloads += [stack_workload_name('attribution-adapter'), stack_workload_name('attribution-service')]
    # These chart values are literal strings, so wire the local sibling services
    # here for both interactive development and the E2E runner.
    rabbitmq_host = stack_workload_name('rabbitmq')
    stack_set_values = attribution_set_values + [
        'job-manager.event_bus.host=' + rabbitmq_host,
        'watcher.config.datasources.rabbitmq.publisher.host=' + rabbitmq_host,
        'anomaly-detection-isolator.event_bus.host=' + rabbitmq_host,
        'api-gateway.audit.endpoint=http://' + stack_workload_name('notifications') + ':8080',
        'dashboard.app.apiGatewayURL=http://' + stack_workload_name('api-gateway') + ':8080',
    ]
    docker_build(
        'slurm-advisor-controller',
        '.',
        dockerfile='slurm-advisor-controller/Dockerfile',
        build_args={
            'SLURM_IMAGE': slinky_sackd_image,
            'VERSION': os.environ.get('BUILD_IMAGE_TAG', '') or 'latest',
        },
    )
    docker_build(
        'slurm-watchdog',
        '.',
        dockerfile='slurm-advisor-controller/Dockerfile.watchdog',
        build_args={'BASE_IMAGE': slinky_sackd_image},
    )

    # Scheduler changes require act mode and explicit acknowledgement.
    if e2e_profile == 'advisor-observe':
        advisor_set_values = [
            'controller.mode=observe',
            'watchdog.mode=observe',
            'controller.acknowledgeSchedulerActions=false',
            'watchdog.acknowledgeSchedulerActions=false',
        ]
    else:
        advisor_set_values = [
            'controller.mode=act',
            'watchdog.mode=act',
            'controller.acknowledgeSchedulerActions=true',
            'watchdog.acknowledgeSchedulerActions=true',
        ]
    advisor_set_values += [
        'controller.heimdallEndpoint=' + stack_workload_name('api-gateway') + '-internal:9090',
        'controller.auditTrail.destination=gateway',
        'controller.auditTrail.maxBatchSize=1',
        'controller.auditTrail.flushInterval=1s',
        'watchdog.auditTrail.destination=gateway',
        'watchdog.auditTrail.maxBatchSize=1',
        'watchdog.auditTrail.flushInterval=1s',
        'controller.logLinks.enabled=true',
        'controller.logLinks.destination.mountPath=/home/log-links',
        'controller.logLinks.destination.claimName=slurm-log-links',
        'controller.logLinks.sources[0].mountPath=/home/logs',
        'controller.logLinks.sources[0].claimName=slurm-job-logs',
    ]

    # A separate opt-in run exercises eager recovery without toggling it during tests.
    if os.environ.get('E2E_ENABLE_EAGER_REQUEUE', '') != '':
        advisor_set_values += ['controller.enableEagerRequeue=true']

    advisor_values = [TILT_DIR + '/services/slurm-advisor-controller/values.yaml']
    if kubernetes_service_auth_smoke:
        advisor_values += [TILT_DIR + '/services/slurm-advisor-controller/kubernetes-service-auth.yaml']

    k8s_yaml(helm(
        'deploy/helm/slurm-advisor-controller',
        name=os.environ.get('SLURM_ADVISOR_RELEASE', 'slurm-advisor-controller'),
        namespace=stack_namespace,
        values=advisor_values,
        set=advisor_set_values,
    ))
    advisor_workloads = ['slurm-advisor-controller', 'slurm-advisor-controller-watchdog']

    if kubernetes_service_auth_smoke:
        stack_values += [E2E_DIR + '/values/stack-local-kubernetes-service-auth.yaml']

    for path in stack_values:
        watch_file(path)

    k8s_yaml(helm(
        'deploy/helm/nvidia-recovery',
        name=helm_release,
        namespace=stack_namespace,
        values=stack_values,
        set=stack_set_values,
    ))

    # Local ports for reaching the stack from this machine, including from E2E tests (see E2E_*_URL).
    stack_port_forwards = {
        stack_workload_name('dashboard'): ['3001:3001'],
        stack_workload_name('job-manager'): ['8081:8080'],
        stack_workload_name('notifications'): ['8082:8080'],
        stack_workload_name('api-gateway'): ['8083:8080'],
        stack_workload_name('watcher'): ['8084:8080'],
        stack_workload_name('anomaly-detection-isolator'): ['8085:8080'],
        stack_workload_name('rabbitmq'): ['15672:15672'],
        'nvidia-recovery-mysql': ['13306:3306'],
        'slurm-advisor-controller': ['9190:9090'],
        'slurm-advisor-controller-watchdog': ['9191:9091'],
    }

    for workload in stack_workloads:
        k8s_resource(
            workload=workload,
            resource_deps=stack_deps,
            labels=[STACK_LABEL],
            port_forwards=stack_port_forwards.get(workload, []),
        )

    for workload in advisor_workloads:
        k8s_resource(
            workload=workload,
            resource_deps=stack_workloads,
            labels=[STACK_LABEL],
            port_forwards=stack_port_forwards.get(workload, []),
        )
