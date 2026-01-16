#!/bin/bash

MASTER_HOST="spark-1"
MASTER_PORT="7077"
MASTER_URL="spark://${MASTER_HOST}:${MASTER_PORT}"

echo "Waiting for metastore on ${METASTORE_HOSTNAME}:${METASTORE_PORT}..."
while ! nc -z ${METASTORE_HOSTNAME} ${METASTORE_PORT}; do
    sleep 1
done
echo "Metastore is available."

# Only start master on spark-1
if [ "${SPARK_HOST_NAME}" = "${MASTER_HOST}" ]; then
    echo "Starting Spark master on ${SPARK_HOST_NAME}..."
    /opt/spark/sbin/start-master.sh \
        -h ${SPARK_HOST_NAME} \
        -p ${MASTER_PORT} \
        --webui-port ${SPARK_WEB_PORT}
fi

# Wait for master to be available before starting worker
echo "Waiting for Spark master at ${MASTER_HOST}:${MASTER_PORT}..."
while ! nc -z ${MASTER_HOST} ${MASTER_PORT}; do
    sleep 1
done
echo "Spark master is available."

# Start worker connecting to the master
echo "Starting Spark worker on ${SPARK_HOST_NAME}, connecting to ${MASTER_URL}..."
/opt/spark/sbin/start-worker.sh ${MASTER_URL} \
    --webui-port ${SPARK_WEB_PORT}

# Only start history server on spark-1
if [ "${SPARK_HOST_NAME}" = "${MASTER_HOST}" ]; then
    echo "Starting Spark history server..."
    /opt/spark/sbin/start-history-server.sh
fi

echo "Spark node ${SPARK_HOST_NAME} started successfully."

# Keep container running
tail -f /dev/null