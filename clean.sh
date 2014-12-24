#!/bin/bash
/usr/local/opt/cassandra/bin/cassandra-cli << EOF
drop keyspace akka;
drop keyspace akka_snapshot;
EOF
