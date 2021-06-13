FROM apache/airflow:2.1.0-python3.7

WORKDIR /opt

USER root:root

RUN apt-get update && apt-get install --quiet --yes openjdk-11-jdk \
  && apt-get clean && rm -rf /var/lib/apt/lists/*

RUN curl --output spark.tgz https://downloads.apache.org/spark/spark-3.0.2/spark-3.0.2-bin-hadoop2.7.tgz

RUN tar --extract --file spark.tgz && rm spark.tgz && mv spark-3.0.2-bin-hadoop2.7 spark

COPY ./spark/jars/ /opt/spark/jars

USER airflow:airflow

COPY ./requirements.txt ./

RUN pip3 install --quiet --requirement requirements.txt
