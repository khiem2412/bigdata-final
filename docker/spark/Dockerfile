FROM python:3.11-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-21-jre-headless \
        curl \
        procps \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PYSPARK_PYTHON=python3

COPY docker/spark/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Download additional JARs for Delta Lake, S3/MinIO, and Kafka
RUN SPARK_JARS=$(python3 -c "import pyspark; print(pyspark.__path__[0])")/jars && \
    echo "Downloading JARs to $SPARK_JARS" && \
    curl -sL -o $SPARK_JARS/delta-spark_2.12-3.1.0.jar \
      https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.1.0/delta-spark_2.12-3.1.0.jar && \
    curl -sL -o $SPARK_JARS/delta-storage-3.1.0.jar \
      https://repo1.maven.org/maven2/io/delta/delta-storage/3.1.0/delta-storage-3.1.0.jar && \
    curl -sL -o $SPARK_JARS/hadoop-aws-3.3.4.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -sL -o $SPARK_JARS/aws-java-sdk-bundle-1.12.262.jar \
      https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    curl -sL -o $SPARK_JARS/spark-sql-kafka-0-10_2.12-3.5.1.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar && \
    curl -sL -o $SPARK_JARS/spark-token-provider-kafka-0-10_2.12-3.5.1.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar && \
    curl -sL -o $SPARK_JARS/commons-pool2-2.12.0.jar \
      https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar && \
    curl -sL -o $SPARK_JARS/kafka-clients-3.4.1.jar \
      https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar && \
    echo "All JARs downloaded successfully"

WORKDIR /app
