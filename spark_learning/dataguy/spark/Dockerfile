FROM apache/spark:latest

USER root

# Install Python and pip if not already available
RUN apt-get update && \
    apt-get install -y python3-pip curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install Python packages for Spark jobs
RUN pip3 install --no-cache-dir \
    boto3==1.28.57 \
    pandas==2.1.1 \
    pyarrow==13.0.0 \
    numpy==1.24.3

# Add Hadoop AWS and AWS SDK JARs for S3 compatibility with MinIO
RUN cd /opt/spark/jars && \
    curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# Create directory for Spark jobs
RUN mkdir -p /opt/spark-jobs && \
    chmod 777 /opt/spark-jobs

WORKDIR /opt/spark

# Set Spark home
ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"

USER spark