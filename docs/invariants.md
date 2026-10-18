Important system design invariants and rules.

1. An instance of NVIDIA Recovery currently operates on a single cluster at a time. Code may assume all jobs are from the same cluster.
2.  `heimdall_job_id` is always a uuidv5 of job_id+attempt_index
