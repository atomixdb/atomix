#!/usr/bin/env python3
import sys
import time
import logging
import argparse
from subprocess import run, check_output
from pathlib import Path

REPO_ROOT = str(Path(__file__).parent.parent)

log = logging.getLogger(__name__)


ATOMIX_NAMESPACE = "atomix"
ATOMIX_KEYSPACE = "atomix"
CASSANDRA_KEYSPACE_CQL = REPO_ROOT + "/schema/cassandra/atomix/keyspace.cql"
CASSANDRA_SCHEMA_CQL = REPO_ROOT + "/schema/cassandra/atomix/schema.cql"


def parse_args():
    parser = argparse.ArgumentParser(description="Deploy atomix to K8s")
    return parser.parse_args()

def kubectl_create_config_map(name: str, namespace: str, config_file: str):
    run(["kubectl", "create", "configmap", name, "-n", namespace, "--from-file=" + config_file], check=True)

def kubectl_list_namespaces():
    """List all namespaces in the k8s cluster."""
    output = check_output(
        [
            "kubectl",
            "get",
            "namespaces",
            "-o",
            "jsonpath='{.items[*].metadata.name}'",
        ],
        text=True,
    )
    namespaces = output.strip("'").split()
    return namespaces


def check_command_exists(command):
    """Check if a command exists in the system's PATH."""
    try:
        check_output(["which", command], text=True)
        return True
    except:
        return False


def read_file_contents(file_path):
    with open(file_path, "r") as f:
        return f.read()


def preflight_checks():
    # Check prerequisites:
    # - kubectl
    # - connection to k8s cluster
    # - there's no existing deployment
    if not check_command_exists("kubectl"):
        log.fatal("kubectl not found in PATH")
        sys.exit(1)
    try:
        check_output(["kubectl", "get", "nodes"])
    except:
        log.fatal("Unable to connect to k8s cluster")
        sys.exit(1)
    if ATOMIX_NAMESPACE in kubectl_list_namespaces():
        log.fatal(
            "Atomix already deployed, namespace {} already exists".format(
                ATOMIX_NAMESPACE
            )
        )
        sys.exit(1)


def kubectl_apply_and_wait_for_sts(yaml_file: str, statefulset: str, namespace: str):
    run(["kubectl", "apply", "-f", yaml_file], check=True)
    run(
        [
            "kubectl",
            "rollout",
            "status",
            "--watch",
            "--timeout=120s",
            "statefulset",
            statefulset,
            "-n",
            namespace,
        ],
        check=True,
    )


def wait_until_cassandra_cql_ready(
    pod_name: str, namespace: str, retries: int = 20, time_between_retries: int = 15
):
    fails = 0
    while True:
        try:
            run(
                [
                    "kubectl",
                    "exec",
                    "-n",
                    namespace,
                    pod_name,
                    "--",
                    "cqlsh",
                    "-e",
                    "DESCRIBE KEYSPACES",
                ],
                check=True,
            )
            break
        except:
            fails += 1
            if fails > retries:
                log.fatal("Cassandra not ready after 10 tries")
                sys.exit(1)
            log.info(
                "Cassandra not ready, retrying in %d seconds", time_between_retries
            )
            time.sleep(time_between_retries)


def main():
    # Very simple script to deploy the application to Kubernetes
    # Mainly for CI tests

    # Steps:
    # 1. Create namespace.
    # 2. Deploy Cassandra.
    # 3. Create keyspace and schema in Cassandra
    # 4. Deploy the universe manager.
    # 5. Deploy the frontend.
    # 6. Deploy the epoch service.
    # 7. Deploy the epoch publisher.
    # 8. Deploy the warden service.
    # 9. Deploy the rangemanager.

    global log
    logging.basicConfig(level=logging.INFO)
    args = parse_args()
    preflight_checks()

    log.info("Deploying Atomix to Kubernetes")

    log.info("Creating namespace")
    run(["kubectl", "apply", "-f", "namespace.yaml"], check=True)

    log.info("Deploying Cassandra")
    kubectl_apply_and_wait_for_sts("cassandra.yaml", "cassandra", ATOMIX_NAMESPACE)
    wait_until_cassandra_cql_ready("cassandra-0", ATOMIX_NAMESPACE)

    log.info("Creating Cassandra keyspace")
    keyspace_cql = read_file_contents(CASSANDRA_KEYSPACE_CQL).strip()
    run(
        [
            "kubectl",
            "exec",
            "-n",
            ATOMIX_NAMESPACE,
            "cassandra-0",
            "--",
            "cqlsh",
            "-e",
            keyspace_cql,
        ],
        check=True,
    )
    log.info("Creating Cassandra schema")
    schema_cql = read_file_contents(CASSANDRA_SCHEMA_CQL).strip()
    run(
        [
            "kubectl",
            "exec",
            "-n",
            ATOMIX_NAMESPACE,
            "cassandra-0",
            "--",
            "cqlsh",
            "-k",
            ATOMIX_KEYSPACE,
            "-e",
            schema_cql,
        ],
        check=True,
    )

    log.info("Deploying Universe Manager")
    kubectl_apply_and_wait_for_sts(
        "universe.yaml", "atomix-universe", ATOMIX_NAMESPACE
    )
    time.sleep(3)

    log.info("Deploying Frontend")
    kubectl_apply_and_wait_for_sts(
        "frontend.yaml", "atomix-frontend", ATOMIX_NAMESPACE
    )
    time.sleep(3)

    log.info("Deploying Epoch Service")
    kubectl_apply_and_wait_for_sts(
        "epoch_service.yaml", "atomix-epoch", ATOMIX_NAMESPACE
    )
    time.sleep(3)

    log.info("Deploying Epoch Publisher")
    kubectl_apply_and_wait_for_sts(
        "epoch_publisher.yaml", "atomix-epoch-publisher", ATOMIX_NAMESPACE
    )
    time.sleep(3)

    log.info("Deploying Warden Service")
    kubectl_apply_and_wait_for_sts(
        "warden.yaml", "atomix-warden", ATOMIX_NAMESPACE
    )
    time.sleep(3)

    log.info("Deploying RangeServer")
    kubectl_apply_and_wait_for_sts(
        "rangeserver.yaml", "atomix-rangeserver", ATOMIX_NAMESPACE
    )


if __name__ == "__main__":
    sys.exit(main())
