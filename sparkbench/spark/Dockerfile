FROM apache/spark:3.5.0

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    netcat-openbsd \
    iproute2 \
    iperf3 \
    iputils-ping \
    && rm -rf /var/lib/apt/lists/*

ENV SPARK_VERSION=3.5.0 \
    HADOOP_VERSION=3.3.5 \
    SCALA_VERSION=2.12 \
    PATH="/opt/spark/bin:/opt/spark/sbin:${PATH}"

WORKDIR /opt/spark

COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY hive-site.xml /opt/spark/conf/hive-site.xml
COPY entrypoint.sh /opt/spark/entrypoint.sh

RUN chmod +x /opt/spark/entrypoint.sh && \
    mkdir -p /opt/spark/spark-events && \
    mkdir -p /opt/spark/logs

EXPOSE 7077 7078 7079 8080 8081 8082

ENTRYPOINT ["/opt/spark/entrypoint.sh"]
