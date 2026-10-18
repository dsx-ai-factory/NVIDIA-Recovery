#!/usr/bin/env python3

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


import pika
import recovery_libs.event_bus.rabbit_eventing as rabbitmq
import yaml
from recovery_libs.schema.watcher.model import Event
from recovery_libs.storage.dataclass import SlurmJobExecutionRecord

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

# Enqueue job event
job_record = SlurmJobExecutionRecord.from_json(
    {
        "job_id": 710695,
        "job_attempt_index": 0,
        "workflow_id": "workflow-test",
        "job_name": "mcore_fp8:27b_fullcs_first_last_bf16_4xLR_3t",
        "state": "RUNNING",
        "state_reason": "None",
        "submit_time": "2024-10-11T05:27:15+00:00",
        "start_time": "2024-10-11T18:46:23+00:00",
        "end_time": None,
        "cluster": "test-cluster-02",
        "partition": "high_prio",
        "reservation": "",
        "namespace": "test-k8s-namespace",
        "user": "test-user-04",
        "command": "/home/test-user-04/megatron-lm-5/experiment_27b_fullcs_first_last_bf16_4xLR_3t.sh",
        "comment": '{"APS":{}}',
        "dependency": "",
        "total_task_count": None,
        "node_count": 64,
        "allocated_node_list": [
            "test-node-005",
            "test-node-006",
            "test-node-007",
            "test-node-008",
            "test-node-009",
            "test-node-010",
            "test-node-011",
            "test-node-012",
            "test-node-013",
            "test-node-014",
            "test-node-015",
            "test-node-016",
            "test-node-017",
            "test-node-018",
            "test-node-019",
            "test-node-020",
            "test-node-021",
            "test-node-022",
            "test-node-023",
            "test-node-024",
            "test-node-025",
            "test-node-026",
            "test-node-027",
            "test-node-028",
            "test-node-029",
            "test-node-030",
            "test-node-031",
            "test-node-032",
            "test-node-033",
            "test-node-034",
            "test-node-035",
            "test-node-036",
            "test-node-037",
            "test-node-038",
            "test-node-039",
            "test-node-040",
            "test-node-041",
            "test-node-042",
            "test-node-043",
            "test-node-044",
            "test-node-045",
            "test-node-046",
            "test-node-047",
            "test-node-048",
            "test-node-049",
            "test-node-050",
            "test-node-051",
            "test-node-052",
            "test-node-053",
            "test-node-054",
            "test-node-055",
            "test-node-056",
            "test-node-057",
            "test-node-058",
            "test-node-059",
            "test-node-060",
            "test-node-061",
            "test-node-062",
            "test-node-063",
            "test-node-064",
            "test-node-065",
            "test-node-066",
            "test-node-067",
            "test-node-068",
        ],
        "inclusion_node_list": None,
        "exclusion_node_list": None,
        "total_cpu_count": 64,
        "total_memory_mb": 128937500,
        "total_gpu_count": 512,
        "anomalies": None,
        "checkpoints": None,
        "aps_spec": None,
        "last_state_transition_time": None,
        "last_updated_at": "2024-10-11T19:35:54.858591+00:00",
        "retry_of": None,
    }
)


channel.basic_publish(
    exchange=connection_spec["exchange"],
    routing_key=connection_spec["routing_key"],
    body=job_record.to_json(),
)

# Enqueue checkpoint load event
checkpoint_load_event = Event.from_json(
    {
        "body": "0: [2024-10-06 14:34:58,714] [INFO] [engine.py:3536:_save_zero_checkpoint] bf16_zero checkpoint loaded /home/test-user-03/savanna/evo2_checkpionts/regression_1/global_step50/bf16_zero_pp_rank_3_mp_rank_00_optim_states.pt",
        "type": "CHECKPOINTLOAD",
        "timestamp": "1728250521663627361",
        "attributes": {
            "job_id": "690531",
            "log_file_name": "test-user-03_regression_1-_690531_date_24-10-06_time_14-32-08.log",
            "log_file_path": "/home/test-service-user/otelfilelog_paths/dev/test-user-03-aps-logs-dev/test-user-03_regression_1-_690531_date_24-10-06_time_14-32-08.log",
            "uuid": "019263c3-2172-74a7-b28f-0077ef0a2556",
        },
    }
)

print("Publishing checkpoint_load to the exchange.")
channel.basic_publish(
    exchange=connection_spec["exchange"],
    routing_key=connection_spec["routing_key"],
    body=checkpoint_load_event.model_dump_json(),
)

# Enqueue checkpoint save event
checkpoint_save_event = Event.from_json(
    {
        "body": "0: [2024-10-06 14:34:58,714] [INFO] [engine.py:3536:_save_zero_checkpoint] bf16_zero checkpoint saved /home/test-user-03/savanna/evo2_checkpionts/regression_1/global_step50/bf16_zero_pp_rank_3_mp_rank_00_optim_states.pt",
        "type": "CHECKPOINTSAVE",
        "timestamp": "1728250521663627361",
        "attributes": {
            "job_id": "690531",
            "log_file_name": "test-user-03_regression_1-_690531_date_24-10-06_time_14-32-08.log",
            "log_file_path": "/home/test-service-user/otelfilelog_paths/dev/test-user-03-aps-logs-dev/test-user-03_regression_1-_690531_date_24-10-06_time_14-32-08.log",
            "uuid": "019263c3-2172-74a7-b28f-0077ef0a2556",
        },
    }
)

print("Publishing checkpoint_save to the exchange.")
channel.basic_publish(
    exchange=connection_spec["exchange"],
    routing_key=connection_spec["routing_key"],
    body=checkpoint_save_event.model_dump_json(),
)


# print(f"Checkpoint: {checkpoint_save_event.model_dump_json()}")
