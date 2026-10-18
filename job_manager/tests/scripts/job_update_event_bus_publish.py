#!/usr/bin/env python3

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Script to publish a synthetic job update message to the local RabbitMQ instance.

Usage:
  # Basic job update (unchanged from original)
  python3 job_update_event_bus_publish.py 12345 0 RUNNING

  # Job update with DAG dependencies
  python3 job_update_event_bus_publish.py 99999 0 PENDING \
      --dag-dep name:preprocess_job \
      --dag-dep id:12345 \
      --dag-action release
"""

import argparse
import json
import re
from datetime import UTC, datetime, timedelta

import pika
import recovery_libs.event_bus.rabbit_eventing as rabbitmq
import yaml
from recovery_libs.schema.aps_spec.spec import (
    ApsSlurmJobSpec,
    AutoResumeModeEnum,
    DagActionEnum,
    DagDependencyStep,
    JobReferenceById,
    JobReferenceByName,
)
from recovery_libs.storage.dataclass import SlurmJobExecutionRecord, SlurmJobFinalStates

FINAL_STATES = {s.value for s in SlurmJobFinalStates}


def parse_args():
    parser = argparse.ArgumentParser(
        description="Publish a synthetic job update message to local RabbitMQ.",
    )
    parser.add_argument("job_id", help="Slurm job ID (e.g., 12345 or 12345_0 for job arrays)")
    parser.add_argument("job_attempt_index", type=int, help="Job attempt index (e.g., 0)")
    parser.add_argument("state", help="Job state (e.g., RUNNING, COMPLETED, FAILED, PENDING)")
    parser.add_argument(
        "--dag-dep",
        action="append",
        metavar="TYPE:VALUE",
        help="Add a DAG dependency in the format 'name:job_name' or 'id:job_id'. Can be specified multiple times.",
    )
    parser.add_argument(
        "--dag-action",
        choices=["release"],
        default="release",
        help="DAG action to take when all dependencies are met (default: release)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the message that would be sent without actually publishing it.",
    )
    return parser.parse_args()


def build_dag_dependencies(dag_dep_args: list[str]) -> list[DagDependencyStep]:
    """Parse --dag-dep arguments into DagDependencyStep objects."""
    deps = []
    for dep_str in dag_dep_args:
        if ":" not in dep_str:
            raise ValueError(f"Invalid --dag-dep format '{dep_str}', expected 'name:<job_name>' or 'id:<job_id>'")
        ref_type, ref_value = dep_str.split(":", 1)
        if ref_type == "name":
            target = JobReferenceByName(name=ref_value)
        elif ref_type == "id":
            target = JobReferenceById(id=ref_value)
        else:
            raise ValueError(f"Invalid reference type '{ref_type}' in --dag-dep, expected 'name' or 'id'")
        deps.append(DagDependencyStep(target=target))
    return deps


def main():
    args = parse_args()

    job_id = args.job_id
    state = args.state.upper()

    # Check if job_id matches pattern <integer>_<integer>
    # If so, extract the prefix as job_group_id
    job_group_id = None
    match = re.match(r"^(\d+)_\d+$", job_id)
    if match:
        job_group_id = match.group(1)
        print(f"Detected job array pattern: job_id={job_id}, job_group_id={job_group_id}")

    # Build APS spec
    dag_dependencies = None
    dag_action = None
    if args.dag_dep:
        dag_dependencies = build_dag_dependencies(args.dag_dep)
        dag_action = DagActionEnum(args.dag_action)
        print(f"DAG dependencies: {[str(d) for d in dag_dependencies]}")
        print(f"DAG action: {dag_action.value}")

    aps_spec = ApsSlurmJobSpec(
        workflow_name="test_workflow",
        auto_resume_mode=AutoResumeModeEnum.REQUEUE,
        dag_dependencies=dag_dependencies,
        dag_action=dag_action,
    )

    job_record_event = SlurmJobExecutionRecord(
        job_id=job_id,
        job_attempt_index=args.job_attempt_index,
        workflow_id="workflow-test",
        job_group_id=job_group_id,
        job_name="test_job",
        state=state,
        cluster="test_cluster",
        partition="batch",
        reservation="recovery_prod",
        namespace="test-k8s-namespace",
        user="John",
        command="sbatch test.sh",
        total_task_count=16,
        submit_time=datetime.now(UTC) - timedelta(minutes=30),
        start_time=datetime.now(UTC) - timedelta(minutes=20),
        end_time=datetime.now(UTC) - timedelta(seconds=20) if state in FINAL_STATES else None,
        last_updated_at=datetime.now(UTC),
        node_count=2,
        allocated_node_list=["node_0", "node_1"],
        inclusion_node_list=["node_0"],
        exclusion_node_list=["node_2"],
        total_gpu_count=16,
        aps_spec=aps_spec,
    )

    message_body = job_record_event.to_json()
    print(f"Job update message body:\n{json.dumps(json.loads(message_body), indent=2)}")

    if args.dry_run:
        print("\n[dry-run] Message not sent.")
        return

    connection_spec = None
    with open("./configs/event_bus_connection_spec.yaml", "r") as config:
        connection_spec = yaml.safe_load(config)["job_update"]

    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=connection_spec["host"],
            port=connection_spec["port"],
            virtual_host=connection_spec["virtual_host"],
            credentials=pika.PlainCredentials(
                connection_spec["username"],
                connection_spec["password"],
            ),
        )
    )
    channel = connection.channel()
    rabbitmq.declare_exchange(channel=channel, exchange_name=connection_spec["exchange"])
    rabbitmq.declare_queue(channel=channel, queue_name=connection_spec["queue"])
    rabbitmq.bind_queue(
        channel=channel,
        queue_name=connection_spec["queue"],
        exchange_name=connection_spec["exchange"],
        routing_key=connection_spec["routing_key"],
    )

    print("Publishing an event to the exchange.")
    channel.basic_publish(
        exchange=connection_spec["exchange"],
        routing_key=connection_spec["routing_key"],
        body=message_body,
    )
    print("Done.")


if __name__ == "__main__":
    main()
