#!/bin/bash

sudo docker exec -i cassandra cqlsh -e "DROP KEYSPACE atomix;"
sudo docker exec -i cassandra cqlsh < schema/cassandra/atomix/keyspace.cql
sudo docker exec -i cassandra cqlsh -k atomix < schema/cassandra/atomix/schema.cql

