#!/bin/bash

export METASTORE_DB_HOSTNAME=${METASTORE_DB_HOSTNAME:-postgres}

echo "Waiting for database on ${METASTORE_DB_HOSTNAME}:5432..."
while ! nc -z ${METASTORE_DB_HOSTNAME} 5432; do
    sleep 1
done

echo "Database on ${METASTORE_DB_HOSTNAME}:5432 is available."
echo "Initializing Hive Metastore schema..."

${HIVE_HOME}/bin/schematool -dbType postgres -initSchema

echo "Starting Hive Metastore..."
${HIVE_HOME}/bin/start-metastore