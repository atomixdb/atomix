<!-- Workload Generator -->
# Workload Generator

A tool for generating and executing configurable workloads against the database system.

## Overview

The Workload Generator creates synthetic workloads based on configuration parameters to test and benchmark the database system. It supports a variety of workload patterns and provides performance metrics after execution.

## Configuration

The workload generator is configured using a JSON file. Here's an example configuration:
```json
{
    "num-keys": 1, 
    "max-concurrency": 1,
    "num-queries": 1000,
    "zipf-exponent": 1.0, 
    "namespace": "mr", 
    "name": "bubbles",
    "background-runtime-core-ids": [2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31]
}
```


### Configuration Parameters

- **num-keys**: The number of unique keys to use in the workload. Higher values create a more distributed workload.
- **max-concurrency**: Maximum number of concurrent transactions that can be executed. Controls the parallelism level of the workload.
- **num-queries**: Total number of queries to execute during the workload run.
- **zipf-exponent**: Controls the skew of the key access distribution. A higher value (e.g., 1.0) creates more skewed access patterns where some keys are accessed much more frequently than others. A value of 0 would create a uniform distribution. We use this parameter to control contention.
- **namespace**: The namespace to use for the workload operations.
- **name**: The name of the table to use for the workload.
- **background-runtime-core-ids**: List of CPU core IDs that the workload generator can use for its background processing. This allows for control over CPU affinity.


## Execution

If executing for the first time pass the `--create-keyspace` flag to create the keyspace. Omit the flag in subsequent runs to avoid getting an error.
```bash
cargo run --release --bin workload-generator -- --config configs/config.json --workload-config workload-generator/configs/config.json --create-keyspace
```

