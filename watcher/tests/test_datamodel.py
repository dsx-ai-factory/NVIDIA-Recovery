# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# flake8: noqa

test_data = [
    {
        "body": "31:  [2024-04-17 11:11:46] iteration       10/15258789 | consumed samples:         1280 | elapsed time per iteration (ms): 5233.2 | throughput per GPU (TFLOP/s/GPU): 540.3 | learning rate: 1.310719E-07 | global batch size:   128 | lm loss: 1.262446E+01 | loss scale: 1.0 | grad norm: 73.991 | num zeros: 1261107328.0 | number of skipped iterations:   0 | number of nan iterations:   0 |",
        "attributes": {
            "job_id": "816175",
            "log.file.name": "gpt3-15b-8t-fault-inject-crash-segfault-t8p1-n4_816175_date_24-04-17_time_11-09-50.log",
            "log.file.path": "/home/test-user/recovery/fault-injectors/megatron-lm/logs/gpt3-15b-8t-fault-inject-crash-segfault-t8p1-n4_816175_date_24-04-17_time_11-09-50.log",
            "start_time": "24-04-17_time_11-09-50",
        },
        "resources": {"service.id": "test-service-id", "service.name": "test-cluster-02"},
        "timestamp": "1713911447800002970",
    },
    {
        "body": "31:  [2024-04-17 11:11:46] iteration       10/15258789 | consumed samples:         1280 | elapsed time per iteration (ms): 5233.2 | throughput per GPU (TFLOP/s/GPU): 294.3 | learning rate: 1.310719E-07 | global batch size:   128 | lm loss: 1.262446E+01 | loss scale: 1.0 | grad norm: 73.991 | num zeros: 1261107328.0 | number of skipped iterations:   0 | number of nan iterations:   0 |",
        "attributes": {
            "job_id": "816175",
            "log.file.name": "gpt3-15b-8t-fault-inject-crash-segfault-t8p1-n4_816175_date_24-04-17_time_11-09-50.log",
            "log.file.path": "/home/test-user/recovery/fault-injectors/megatron-lm/logs/gpt3-15b-8t-fault-inject-crash-segfault-t8p1-n4_816175_date_24-04-17_time_11-09-50.log",
            "start_time": "24-04-17_time_11-09-50",
        },
        "resources": {"service.id": "test-service-id", "service.name": "test-cluster-02"},
        "timestamp": "1713911447958502970",
    },
]
