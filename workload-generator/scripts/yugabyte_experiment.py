import uuid
import argparse
import logging
import psycopg2

log = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser()
    return parser.parse_args()

def connect_to_yugabyte():
    conn = psycopg2.connect(
        host="localhost",
        user="yugabyte",
        port=5433,
        password="yugabyte",
        database="yugabyte"
    )
    conn.autocommit = True
    return conn.cursor()


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

def main():
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser()
    args = parser.parse_args()

    cursor = connect_to_yugabyte()
    create_bench_table(cursor, delete_if_exists=True)

    my_uuid = uuid.uuid4()
    for key in ["a", "e", "n", "z"]:
        key = key + "-" + my_uuid.hex
        insert_row(cursor, key, "v1")

    cursor.execute("SELECT * FROM kv_store")
    print(cursor.fetchall())



if __name__ == "__main__":
    main()