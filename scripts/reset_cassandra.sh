#!/bin/bash

set -euo pipefail

function hard_reset_cassandra() {
    # Stop and remove existing container if it exists
    docker stop cassandra 2>/dev/null || true
    docker rm cassandra 2>/dev/null || true

    # Start new Cassandra container
    docker run -d -p 9042:9042 --name cassandra cassandra:5.0

    # Wait for Cassandra to be ready
    echo "Waiting for Cassandra to start..."
    until docker exec cassandra cqlsh -e "describe keyspaces;" >/dev/null 2>&1; do
        sleep 2
    done
    echo "Cassandra is ready"

    # Load schema
    echo "Loading schema..."
    docker exec -i cassandra cqlsh < schema/cassandra/atomix/keyspace.cql
    docker exec -i cassandra cqlsh -k atomix < schema/cassandra/atomix/schema.cql
    echo "Schema loaded successfully"
}

function soft_reset_cassandra() {
    # Wait for Cassandra to be ready
    echo "Waiting for Cassandra to be ready..."
    until docker exec cassandra cqlsh -e "describe keyspaces;" >/dev/null 2>&1; do
        sleep 2
    done
    echo "Cassandra is ready"

    # Drop existing keyspace if it exists
    echo "Dropping existing keyspace..."
    docker exec -i cassandra cqlsh -e "DROP KEYSPACE IF EXISTS atomix;"

    # Load schema
    echo "Loading schema..."
    docker exec -i cassandra cqlsh < schema/cassandra/atomix/keyspace.cql
    docker exec -i cassandra cqlsh -k atomix < schema/cassandra/atomix/schema.cql
    echo "Schema loaded successfully"
}


# Default to soft reset
RESET_TYPE="soft"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        hard)
            RESET_TYPE="hard"
            shift
            ;;
        soft)
            RESET_TYPE="soft"
            shift
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

if [ "$RESET_TYPE" = "hard" ]; then
    hard_reset_cassandra
else
    soft_reset_cassandra
fi
