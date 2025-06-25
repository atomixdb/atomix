import os
import re
import json
import time
import uuid
import signal
import logging
import argparse
import psycopg2
import random

from subprocess import Popen
from yanniszark_common.cmdutils import check_output

log = logging.getLogger(__name__)


TEARDOWN_TASKS = []

def parse_args():
    parser = argparse.ArgumentParser()
    return parser.parse_args()

def connect_to_yugabyte(host="localhost"):
    conn = psycopg2.connect(
        host=host,
        user="yugabyte",
        port=5433,
        password="yugabyte",
        database="yugabyte"
    )
    conn.autocommit = True
    return conn.cursor()

def docker_check_output(container, cmd):
    return check_output(["docker", "exec", container, "bash", "-c", cmd], text=True)


class YugabyteDB:

    DB = "kv_store"
    PRIMARY_MASTER = "172.18.0.2"
    SECONDARY_MASTER = "172.18.0.3"
    PRIMARY_CONTAINER = "yb-primary"
    SECONDARY_CONTAINERS = ["yb-secondary-1", "yb-secondary-2"]

    def __init__(self):
        self.__compose_process = None

    def start(self):
        self.__start_docker_compose()

    def setup(self):
        # Create table on primary
        docker_check_output(
            self.PRIMARY_CONTAINER,
            f"""ysqlsh -h {self.PRIMARY_MASTER} -c "
                CREATE TABLE {self.DB} (
                    k TEXT,
                    v TEXT,
                    PRIMARY KEY (k ASC)
                ) SPLIT AT VALUES (('d'), ('m'), ('t'));"
            """
        )

        # Create table on secondary
        docker_check_output(
            self.SECONDARY_CONTAINERS[0],
            f"""ysqlsh -h {self.SECONDARY_MASTER} -c "
                CREATE TABLE {self.DB} (
                    k TEXT,
                    v TEXT,
                    PRIMARY KEY (k ASC)
                ) SPLIT AT VALUES (('d'), ('m'), ('t'));"
            """
        )

        # Create snapshot schedule on secondary
        docker_check_output(
            self.SECONDARY_CONTAINERS[0],
            f"./bin/yb-admin --master_addresses {self.SECONDARY_MASTER} create_snapshot_schedule 1 10 ysql.yugabyte"
        )

        # Get table ID
        # Example output:
        # yugabyte.kv_store [ysql_schema=public] [000034cb000030008000000000004000]
        table_id_output = docker_check_output(
            self.PRIMARY_CONTAINER,
            f"./bin/yb-admin --master_addresses {self.PRIMARY_MASTER} list_tables | grep {self.DB}"
        )
        table_id = table_id_output.strip()
        # Extract the table ID from the output using regex
        table_id = re.search(r"\[([0-9a-f]+)\]$", table_id_output).group(1)

        print(f"Table ID: '{table_id}'")

        # Get bootstrap ID
        # Example output:
        # table id: 000034cb000030008000000000004000, CDC bootstrap id: de0b5affa1d0a6acbd42ab48a22e653f
        bootstrap_output = docker_check_output(
            self.PRIMARY_CONTAINER,
            f"./bin/yb-admin --master_addresses {self.PRIMARY_MASTER} bootstrap_cdc_producer {table_id}"
        )
        # Extract the bootstrap ID from the output using regex
        bootstrap_id = re.search(r"CDC bootstrap id: ([0-9a-f]+)", bootstrap_output).group(1)

        print(f"Bootstrap ID: '{bootstrap_id}'")

        # Set up universe replication on the first secondary
        docker_check_output(
            self.SECONDARY_CONTAINERS[0],
            f"./bin/yb-admin --master_addresses {self.SECONDARY_MASTER} setup_universe_replication kv_store_replication {self.PRIMARY_MASTER} {table_id} {bootstrap_id} transactional"
        )

        # Set the secondary cluster to STANDBY mode
        docker_check_output(
            self.SECONDARY_CONTAINERS[0],
            f"./bin/yb-admin --master_addresses {self.SECONDARY_MASTER} change_xcluster_role STANDBY"
        )

        safe_time_info = self.get_safe_time_info()
        print(safe_time_info)


    def create_partition(self, from_container, to_container):
        assert from_container in self.PRIMARY_CONTAINER or from_container in self.SECONDARY_CONTAINERS
        assert to_container in self.PRIMARY_CONTAINER or to_container in self.SECONDARY_CONTAINERS
        assert from_container != to_container

        # Get from_container PID
        from_container_pid = check_output(["docker", "inspect", "-f", "{{.State.Pid}}", from_container]).strip()

        # Get to_container IP
        to_container_ip = check_output(["docker", "inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", to_container]).strip()

        # Use nsenter to install an IPTables rule in the from_container's network namespace
        # Block traffic from source to destination container using iptables
        check_output([
            "sudo", "nsenter", "--target", from_container_pid, "--net",
            "iptables", "-A", "OUTPUT", "-d", to_container_ip, "-j", "DROP"
        ])

    def remove_partition(self, from_container, to_container):
        assert from_container in self.PRIMARY_CONTAINER or from_container in self.SECONDARY_CONTAINERS
        assert to_container in self.PRIMARY_CONTAINER or to_container in self.SECONDARY_CONTAINERS
        assert from_container != to_container

        # Get from_container PID
        from_container_pid = check_output(["docker", "inspect", "-f", "{{.State.Pid}}", from_container]).strip()

        # Get to_container IP
        to_container_ip = check_output(["docker", "inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", to_container]).strip()

        # Use nsenter to remove the IPTables rule in the from_container's network namespace
        check_output([
            "sudo", "nsenter", "--target", from_container_pid, "--net",
            "iptables", "-D", "OUTPUT", "-d", to_container_ip, "-j", "DROP"
        ])

    def __start_docker_compose(self):
        if self.is_running():
            raise Exception("Docker compose is already running")
        cmd = ["docker", "compose", "up", "--force-recreate"]
        compose_file = os.path.join(os.path.dirname(__file__), "yugabyte_setup", "docker-compose.yaml")
        if not os.path.exists(compose_file):
            raise Exception(f"Docker compose file not found at {compose_file}")
        compose_dir = os.path.dirname(compose_file)
        self.__compose_process = Popen(cmd, cwd=compose_dir)
        time.sleep(15)
        if not self.is_running():
            raise Exception("Failed to start docker compose")
        primary_address = check_output(["docker", "inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", self.PRIMARY_CONTAINER]).strip()
        secondary_address_0 = check_output(["docker", "inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", self.SECONDARY_CONTAINERS[0]]).strip()
        secondary_address_1 = check_output(["docker", "inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", self.SECONDARY_CONTAINERS[1]]).strip()
        if not primary_address or not secondary_address_0 or not secondary_address_1:
            raise Exception("Failed to get IP addresses for containers")
        print(f"Primary: {primary_address}")
        print(f"Secondary 1: {secondary_address_0}")
        print(f"Secondary 2: {secondary_address_1}")
        self.PRIMARY_MASTER = primary_address
        self.SECONDARY_MASTER = secondary_address_0
        self.SECONDARY_MASTER_1 = secondary_address_1
        print("Docker compose started")

    def failover_to_secondary(self, safe_time=None):
        """Failover to the secondary cluster."""
        # Pause replication
        docker_check_output(
            self.SECONDARY_CONTAINERS[0],
            f"./bin/yb-admin --master_addresses {self.SECONDARY_MASTER} set_universe_replication_enabled kv_store_replication 0"
        )

        # Get safe time info
        if not safe_time:
            safe_time_info = self.get_safe_time_info()
            safe_time = safe_time_info["safe_time_epoch"]
            print(f"Replication info: {safe_time_info}")

        # Get snapshot schedule ID
        schedules = docker_check_output(
            self.SECONDARY_CONTAINERS[0],
            f"./bin/yb-admin --master_addresses {self.SECONDARY_MASTER} list_snapshot_schedules"
        )
        schedules = json.loads(schedules.strip())["schedules"]
        if len(schedules) != 1:
            raise ValueError(f"Expected exactly 1 snapshot schedule, got {len(schedules)}")
        schedule_id = schedules[0]["id"]
        print(f"Schedule ID: {schedule_id}")

        # Restore to safe time
        start_time = time.time()
        docker_check_output(
            self.SECONDARY_CONTAINERS[0],
            f"./bin/yb-admin --master_addresses {self.SECONDARY_MASTER} restore_snapshot_schedule {schedule_id} {safe_time}"
        )

        # Verify restoration completed
        while True:
            restorations = docker_check_output(
                self.SECONDARY_CONTAINERS[0],
                f"./bin/yb-admin --master_addresses {self.SECONDARY_MASTER} list_snapshot_restorations"
            )
            restorations = json.loads(restorations.strip())

            if not restorations["restorations"]:
                raise Exception("No restorations found. Output: " + str(restorations))

            state = restorations["restorations"][0]["state"]
            if state == "RESTORED":
                break
            elif state == "RESTORING":
                time.sleep(0.1)
                continue
            else:
                raise Exception(f"Unexpected restoration state: {state}. Output: {str(restorations)}")

        end_time = time.time()
        restore_time = end_time - start_time
        print(f"Snapshot restoration completed in {restore_time:.2f} seconds")

        # Delete old replication group
        docker_check_output(
            self.SECONDARY_CONTAINERS[0],
            f"./bin/yb-admin --master_addresses {self.SECONDARY_MASTER} delete_universe_replication kv_store_replication"
        )

    def teardown(self):
        """Stop the docker compose process by sending SIGINT (Ctrl+C)."""
        if self.__compose_process:
            self.__compose_process.send_signal(signal.SIGINT)
            time.sleep(1)
            self.__compose_process.send_signal(signal.SIGKILL)
            self.__compose_process.send_signal(signal.SIGTERM)
            print("Sent SIGINT to docker compose. Waiting for it to exit...")
            self.__compose_process.wait()
            self.__compose_process = None


    def is_running(self):
        """Check if the docker compose process is still running.

        Returns:
            bool: True if running, False otherwise
        """
        if not self.__compose_process:
            return False
        return self.__compose_process.poll() is None


    def get_safe_time_info(self):
        """Get cross-cluster safe time info from the secondary cluster.

        Returns:
            dict: Dictionary containing:
                - namespace_id (str): ID of the namespace (e.g. '000034cb000030008000000000000000')
                - namespace_name (str): Name of the namespace (e.g. 'yugabyte')
                - safe_time (str): Safe time in human readable format (e.g. '2025-06-23 21:24:52.764103')
                - safe_time_epoch (str): Safe time in epoch microseconds (e.g. '1750713892764103')
                - safe_time_lag_sec (str): Lag in seconds between clusters (e.g. '0.64')
                - safe_time_skew_sec (str): Clock skew in seconds between clusters (e.g. '0.30')
        """
        safe_time_info = docker_check_output(
            self.SECONDARY_CONTAINERS[0],
            f"./bin/yb-admin --master_addresses {self.SECONDARY_MASTER} get_xcluster_safe_time include_lag_and_skew"
        )
        safe_time_info = json.loads(safe_time_info.strip())

        if len(safe_time_info) != 1:
            raise ValueError(f"Expected exactly 1 safe time info entry, got {len(safe_time_info)}")

        return safe_time_info[0]


def create_bench_table(cursor, delete_if_exists=True):
    # if delete_if_exists:
    #     cursor.execute("DROP TABLE IF EXISTS kv_store")
    create_table_sql = """
CREATE TABLE kv_store (
    k TEXT,
    v TEXT,
    PRIMARY KEY (k ASC)
) SPLIT AT VALUES (('d'), ('m'), ('t'));
    """
    # cursor.execute(create_table_sql)


def insert_row(cursor, key, value):
    insert_sql = """
    INSERT INTO kv_store (k, v) VALUES (%s, %s)
    """
    cursor.execute(insert_sql, (key, value))

def insert_rows(cursor, key_prefix, value, num_rows):
    """Insert multiple rows into the kv_store table.

    Args:
        cursor: Database cursor to use for inserts
        key_prefix (str): Prefix to use for all keys
        value_size (int): Size in bytes of the value to insert
        num_rows (int): Number of rows to insert
    """
    # value = random.randbytes(value_size).hex()

    for i in range(num_rows):
        key = f"{key_prefix}-{i}"
        insert_row(cursor, key, value)


def main():
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser()
    args = parser.parse_args()

    # Start yugabyte
    yb = YugabyteDB()
    yb.start()
    TEARDOWN_TASKS.append(yb.teardown)
    yb.setup()

    # Insert a bunch of rows
    print("Inserting rows")
    start_time = time.time()
    cursor = connect_to_yugabyte(yb.PRIMARY_MASTER)
    secondary_cursor = connect_to_yugabyte(yb.SECONDARY_MASTER)
    num_iter = 10000
    num_rows_per_partition = 1
    for i in range(num_iter):
        uuid_prefix = str(i)
        # uuid_prefix = str(uuid.uuid4())
        insert_rows(cursor, "a" + uuid_prefix, "1", num_rows_per_partition)
        insert_rows(cursor, "e" + uuid_prefix, "1", num_rows_per_partition)
        insert_rows(cursor, "n" + uuid_prefix, "1", num_rows_per_partition)
        insert_rows(cursor, "z" + uuid_prefix, "1", num_rows_per_partition)
    duration = time.time() - start_time
    num_rows = num_iter * num_rows_per_partition * 4
    print(f"Inserted {num_rows} rows in {duration:.2f} seconds")

    # Ensure all rows are visible on the secondary
    time.sleep(5)
    print("Performing full table scan to ensure replication")
    scan_sql = "SELECT * FROM kv_store"
    secondary_cursor.execute(scan_sql)
    count = 0
    page_size = 1000
    while True:
        rows = secondary_cursor.fetchmany(page_size)
        if not rows:
            break
        for row in rows:
            if row[1] != "1":
                raise Exception(f"Unexpected value {row[1]} found in row {row[0]}, expected '1'")
            count += 1
    print(f"Verified {count} rows have value '1'")
    expected_count = num_iter * num_rows_per_partition * 4
    assert count == expected_count, f"Expected {expected_count} rows, got {count}"
    safe_time_info = yb.get_safe_time_info()
    print(f"Before partition: Safe time info: {safe_time_info}")
    safe_time_before_partition = safe_time_info["safe_time_epoch"]

    # Create partition
    print("Creating partition")
    yb.create_partition(yb.PRIMARY_CONTAINER, yb.SECONDARY_CONTAINERS[0])
    time.sleep(5)

    # Insert rows
    print("Inserting rows")
    num_iter_after = num_iter * 1
    for i in range(num_iter_after):
        uuid_prefix = str(i) + "_"
        insert_rows(cursor, "a" + uuid_prefix, "2", num_rows_per_partition)
        insert_rows(cursor, "e" + uuid_prefix, "2", num_rows_per_partition)
        insert_rows(cursor, "n" + uuid_prefix, "2", num_rows_per_partition)
        insert_rows(cursor, "z" + uuid_prefix, "2", num_rows_per_partition)

    # Removeme
    time.sleep(10**9)

    # Ensure new rows are not visible on the secondary
    time.sleep(1)
    print("After partition: Performing full table scan to ensure transactional consistency")
    scan_sql = "SELECT * FROM kv_store"
    secondary_cursor.execute(scan_sql)
    count = 0
    page_size = 1000
    while True:
        rows = secondary_cursor.fetchmany(page_size)
        if not rows:
            break
        for row in rows:
            if row[1] != "1":
                raise Exception(f"After partition: Unexpected value {row[1]} found in row {row[0]}, expected '1'")
            count += 1
    print(f"After partition: Verified {count} rows have value '1'")
    expected_count = num_iter * num_rows_per_partition * 4
    assert count == expected_count, f"After partition: Expected {expected_count} rows, got {count}"

    safe_time_info = yb.get_safe_time_info()
    print(f"After partition: Safe time info: {safe_time_info}")

    # Perform failover
    print("Performing failover")
    start_time = time.time()
    yb.failover_to_secondary(safe_time=safe_time_before_partition)
    duration = time.time() - start_time
    print(f"Failover completed in {duration:.2f} seconds")

    # Insert a single row
    print("Inserting a single row")
    insert_row(secondary_cursor, "a", "1")
    insert_row(secondary_cursor, "e", "1")
    insert_row(secondary_cursor, "n", "1")
    insert_row(secondary_cursor, "z", "1")

    time.sleep(1)
    print("After failover: Performing full table scan to ensure transactional consistency")
    scan_sql = "SELECT * FROM kv_store"
    secondary_cursor.execute(scan_sql)
    count = 0
    page_size = 1000
    while True:
        rows = secondary_cursor.fetchmany(page_size)
        if not rows:
            break
        for row in rows:
            if row[1] != "1":
                raise Exception(f"After failover: Unexpected value {row[1]} found in row {row[0]}, expected '1'")
            count += 1
    print(f"After failover: Verified {count} rows have value '1'")
    expected_count = num_iter * num_rows_per_partition * 4 + 4
    assert count == expected_count, f"After partition: Expected {expected_count} rows, got {count}"

    print("Waiting a bit before teardown")
    time.sleep(1000)

    # Teardown
    print("Teardown")
    yb.teardown()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Error: {e}")
        for task in TEARDOWN_TASKS:
            task()
        raise e