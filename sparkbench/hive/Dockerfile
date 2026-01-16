FROM eclipse-temurin:17-jdk-jammy

ENV HADOOP_VERSION=3.3.5
ENV HIVE_METASTORE_VERSION=3.0.0
ENV HADOOP_HOME=/opt/hadoop
ENV HIVE_HOME=/opt/hive-metastore
ENV METASTORE_DB_HOSTNAME=postgres

WORKDIR /opt

# Install dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    netcat-openbsd \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Download and install Hadoop
RUN wget -q https://dlcdn.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz && \
    tar xf hadoop-${HADOOP_VERSION}.tar.gz && \
    mv hadoop-${HADOOP_VERSION} ${HADOOP_HOME} && \
    rm hadoop-${HADOOP_VERSION}.tar.gz

# Download and install Hive Metastore
RUN wget -q https://dlcdn.apache.org/hive/hive-standalone-metastore-${HIVE_METASTORE_VERSION}/hive-standalone-metastore-${HIVE_METASTORE_VERSION}-bin.tar.gz && \
    tar xf hive-standalone-metastore-${HIVE_METASTORE_VERSION}-bin.tar.gz && \
    mv apache-hive-metastore-${HIVE_METASTORE_VERSION}-bin ${HIVE_HOME} && \
    rm hive-standalone-metastore-${HIVE_METASTORE_VERSION}-bin.tar.gz

# Download JDBC driver and AWS SDK
RUN wget -q https://jdbc.postgresql.org/download/postgresql-42.6.0.jar -P ${HIVE_HOME}/lib/ && \
    wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar -P ${HIVE_HOME}/lib/ && \
    wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.316/aws-java-sdk-bundle-1.12.316.jar -P ${HIVE_HOME}/lib/

COPY metastore-site.xml ${HIVE_HOME}/conf/
COPY init-hive.sh /opt/init-hive.sh

RUN chmod +x /opt/init-hive.sh

EXPOSE 9083

ENTRYPOINT ["/opt/init-hive.sh"]