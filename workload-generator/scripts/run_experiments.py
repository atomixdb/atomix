import os
import json
import subprocess
import ray
from ray import tune
from pathlib import Path
import psutil
import re
import uuid
import signal
from coolname import generate_slug
from functools import partial

RAY_LOGS_DIR = Path(__file__).parent.parent / "experiments" / "ray_logs"


def parse_metrics(output):
    metrics = {}
    # Find the metrics section between METRICS_START and METRICS_END
    metrics_section = re.search(r"METRICS_START\n(.*?)\nMETRICS_END", output, re.DOTALL)
    if not metrics_section:
        print("Warning: No metrics section found in output")
        return {"throughput": 0.0}

    metrics_text = metrics_section.group(1)

    # Regular expressions to match the metrics
    patterns = {
        "total_duration": r"Total Duration: ([\d\.]+[µnm]?s)",
        "total_transactions": r"Total Transactions: (\d+)",
        "avg_latency": r"Average Latency: ([\d\.]+[µnm]?s)",
        "p50_latency": r"P50 Latency: ([\d\.]+[µnm]?s)",
        "p95_latency": r"P95 Latency: ([\d\.]+[µnm]?s)",
        "p99_latency": r"P99 Latency: ([\d\.]+[µnm]?s)",
        "throughput": r"Throughput: ([\d\.]+) transactions/second",
    }

    for metric, pattern in patterns.items():
        match = re.search(pattern, metrics_text)
        if match:
            value = match.group(1)
            # Convert duration strings to seconds
            if "s" in value:
                if "µs" in value:
                    metrics[metric] = float(value.replace("µs", "")) / 1_000_000
                elif "ms" in value:
                    metrics[metric] = float(value.replace("ms", "")) / 1_000
                elif "ns" in value:
                    metrics[metric] = float(value.replace("ns", "")) / 1_000_000_000
                else:
                    metrics[metric] = float(value.replace("s", ""))
            else:
                metrics[metric] = float(value)
    # print(metrics)
    return metrics


def run_workload(config):
    #  Overwrite the namespace and name with a random UUID
    uuid_str = uuid.uuid4().hex[:8]
    # config["namespace"] += f"_{uuid_str}"
    config["name"] += f"_{uuid_str}"

    # Create a temporary config file with the current parameters
    root_dir = Path(__file__).parent.parent.parent
    config_path = root_dir / "configs" / "config.json"
    workload_config_path = root_dir / "workload-generator" / "configs" / f"config-ray.json"
    os.makedirs(os.path.dirname(workload_config_path), exist_ok=True)
    with open(workload_config_path, "w") as f:
        json.dump(config, f)

    # Run the workload generator with timeout
    try:
        process = subprocess.Popen(
            [
                "cargo",
                "run",
                "--release",
                "--bin",
                "workload-generator",
                "--",
                "--workload-config",
                str(workload_config_path),
                "--config",
                str(config_path),
                "--create-keyspace",
            ],
            cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        try:
            stdout, stderr = process.communicate(timeout=60 * 10)  # 10 minutes timeout
            metrics = parse_metrics(stdout)
            if not metrics:
                print("Warning: No metrics found in output")
                return {"throughput": 0.0}
            return metrics

        except subprocess.TimeoutExpired:
            process.kill()
            print(f"Timeout exceeded for config: {config}")
            return {"throughput": 0.0}

    except Exception as e:
        print(f"Error running workload: {e}")
        return {"throughput": 0.0}


def main():
    # Initialize Ray
    ray.init()

    ray_logs_dir = Path(RAY_LOGS_DIR)
    ray_logs_dir.mkdir(parents=True, exist_ok=True)

    namespace, name = generate_slug(2).split("-")

    # Define the search space
    # config = {
    #     "num-keys": tune.grid_search([1, 10, 100]),
    #     "max-concurrency": tune.grid_search([1, 5, 10]),
    #     "num-queries": tune.grid_search([1000]),
    #     "zipf-exponent": tune.grid_search([0, 0.5, 1.2]),
    #     "namespace": namespace,
    #     "name": name,
    #     "background-runtime-core-ids": [3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31]
    # }

    config = {
        "num-keys": tune.grid_search([1]),
        "max-concurrency": tune.grid_search([1]),
        "num-queries": tune.grid_search([10000]),
        "zipf-exponent": tune.grid_search([0]),
        "namespace": namespace,
        "name": name,
        "background-runtime-core-ids": [3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31]
    }

    experiment_name = f"{namespace}_{name}_{uuid.uuid4().hex[:8]}"

    # Run the experiment
    analysis = tune.run(
        run_workload,
        config=config,
        num_samples=1,
        resources_per_trial={"cpu": psutil.cpu_count()},
        storage_path=ray_logs_dir,
        name=experiment_name,
        # progress_reporter=tune.CLIReporter(
        #     metric_columns=["throughput"],
        #     parameter_columns=[
        #         "max-concurrency",
        #         "num-queries",
        #         "num-keys",
        #         "zipf-exponent",
        #     ],
        # ),
    )

    # Save results to a file
    results = analysis.results_df
    results.to_csv(ray_logs_dir / experiment_name / "results.csv")

    # Shutdown Ray
    ray.shutdown()


if __name__ == "__main__":
    main()
