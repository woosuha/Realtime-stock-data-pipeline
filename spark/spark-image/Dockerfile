FROM docker.io/library/spark:4.0.1

USER root

COPY gcs-connector-hadoop3.jar /opt/spark/jars/
COPY spark-avro_2.13-4.0.1.jar /opt/spark/jars/
COPY spark-sql-kafka-0-10_2.13-4.0.1.jar /opt/spark/jars/
COPY kafka-clients-3.9.0.jar /opt/spark/jars/
COPY spark-token-provider-kafka-0-10_2.13-4.0.1.jar /opt/spark/jars/
COPY commons-pool2-2.13.1.jar /opt/spark/jars/

COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY utils /opt/spark/work-dir/utils

ENV PYTHONPATH=/opt/spark/work-dir

USER spark