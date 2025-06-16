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
import time
import requests
from coolname import generate_slug
from functools import partial

from datetime import timedelta
from threading import Event, Thread
from run_experiments import run_workload, parse_metrics

RAY_LOGS_DIR = Path(__file__).parent.parent / "experiments" / "ray_logs"


def warden_debug_metrics(metrics_addr: str):
    # Get the metrics from the metrics server.
    response = requests.get(f"http://{metrics_addr}/debug")
    if response.status_code != 200:
        return None
    return response.json()

"""
{
  "primary_range_statuses": [
    {
      "range_id": {
        "keyspace_id": "59000c7c-200f-4c56-9775-302b75c98ee8",
        "range_id": "75f229bb-ab9c-4e9c-be8a-0609b6016435"
      },
      "leader_sequence_number": 1,
      "host_info": {
        "identity": {
          "name": "127.0.0.1",
          "zone": "/a"
        },
        "address": "0.0.0.0:0",
        "warden_connection_epoch": 3342
      }
    }
  ],
  "secondary_range_statuses": [
    {
      "range_id": {
        "keyspace_id": "59000c7c-200f-4c56-9775-302b75c98ee8",
        "range_id": "8b21de02-efbb-4a42-a474-70fda7ef32b6"
      },
      "leader_sequence_number": 1,
      "host_info": {
        "identity": {
          "name": "127.0.0.1",
          "zone": "/a"
        },
        "address": "0.0.0.0:0",
        "warden_connection_epoch": 3342
      },
      "wal_epoch": 17780,
      "applied_epoch": 17780
    }
  ]
}
"""

def watch_replication_drift(interval: timedelta, stop_event: Event, return_metrics):
    metrics_addr = "127.0.0.1:7777"
    return_metrics["primary_ranges"] = {}
    return_metrics["secondary_ranges"] = {}
    time.sleep(4)
    while not stop_event.is_set():
        # Get the metrics from the metrics server.
        time.sleep(interval.total_seconds())
        metrics = warden_debug_metrics(metrics_addr)
        if metrics is None:
            continue
        # TODO: Update the timeseries with the metrics.
        for range_status in metrics["secondary_range_statuses"]:
            range_id = range_status["range_id"]["range_id"]
            wal_epoch = range_status["wal_epoch"]
            applied_epoch = range_status["applied_epoch"]
            if range_id not in return_metrics["secondary_ranges"]:
                return_metrics["secondary_ranges"][range_id] = {
                    "wal_epoch": [] ,
                    "applied_epoch": [],
                }
            return_metrics["secondary_ranges"][range_id]["wal_epoch"].append(wal_epoch)
            return_metrics["secondary_ranges"][range_id]["applied_epoch"].append(applied_epoch)
        for range_status in metrics["primary_range_statuses"]:
            range_id = range_status["range_id"]["range_id"]
            latest_wal_epoch = range_status["latest_wal_epoch"]
            if range_id not in return_metrics["primary_ranges"]:
                return_metrics["primary_ranges"][range_id] = {
                    "latest_wal_epoch": [],
                }
            return_metrics["primary_ranges"][range_id]["latest_wal_epoch"].append(latest_wal_epoch)

def run_replication_experiment(config):
    reset_cassandra()
    build_chardonnay()
    service_to_process = start_chardonnay()

    print("Starting replication drift watcher")
    stop_event = Event()
    replication_metrics = {}
    thread = Thread(target=watch_replication_drift, args=(timedelta(seconds=1), stop_event, replication_metrics))
    thread.start()

    print("Running workload")
    metrics = run_workload(config)

    time.sleep(5)

    print("Stopping replication drift watcher")
    stop_event.set()
    thread.join()

    for service, process in service_to_process.items():
        process.terminate()
        process.wait()

    return {"metrics": metrics, "replication_metrics": replication_metrics}


def reset_cassandra():
    root_dir = Path(__file__).parent.parent.parent
    reset_script = root_dir / "scripts" / "reset_cassandra.sh"
    if not reset_script.exists():
        raise FileNotFoundError(f"Reset script not found at {reset_script}")
    subprocess.check_output([reset_script], timeout=10, cwd=root_dir)


def run_and_log(cmd, log_file, working_dir):
    with open(log_file, "w") as f:
        process = subprocess.Popen(cmd, stdout=f, stderr=f, cwd=working_dir)
        return process

def build_chardonnay():
    root_dir = Path(__file__).parent.parent.parent
    subprocess.check_output(["cargo", "build", "--release"], cwd=root_dir)

def start_chardonnay():
    root_dir = Path(__file__).parent.parent.parent
    universe_dir = root_dir / "universe"
    warden_dir = root_dir / "warden"
    epoch_dir = root_dir / "epoch"
    epoch_publisher_dir = root_dir / "epoch_publisher"
    frontend_dir = root_dir / "frontend"
    rangeserver_dir = root_dir / "rangeserver"
    log_dir = root_dir / "logs"
    config_path = root_dir / "configs" / "config.json"

    # Create the log directory.
    log_dir.mkdir(parents=True, exist_ok=True)

    service_to_dir = {
        "universe": universe_dir,
        "warden": warden_dir,
        "epoch": epoch_dir,
        "epoch_publisher": epoch_publisher_dir,
        "frontend": frontend_dir,
        "rangeserver": rangeserver_dir,
    }

    service_to_process = {}

    for service, _ in service_to_dir.items():
        log_file = log_dir / f"{service}.log"
        binary = root_dir / "target" / "release" / service
        process = run_and_log([binary, "--config", config_path], log_file, root_dir)
        service_to_process[service] = process
        time.sleep(0.2)

    time.sleep(1)
    # Check that all processes are running.
    for service, process in service_to_process.items():
        if process.poll() is not None:
            raise RuntimeError(f"Service {service} crashed")

    return service_to_process



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
        "num-queries": tune.grid_search([30000]),
        "zipf-exponent": tune.grid_search([0]),
        "namespace": namespace,
        "name": name,
        "background-runtime-core-ids": [3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31]
    }

    experiment_name = f"{namespace}_{name}_{uuid.uuid4().hex[:8]}"

    # Run the experiment
    analysis = tune.run(
        run_replication_experiment,
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
    results = analysis.results
    with open(ray_logs_dir / experiment_name / "results.json", "w") as f:
        json.dump(results, f)

    # Shutdown Ray
    ray.shutdown()


if __name__ == "__main__":
    main()
