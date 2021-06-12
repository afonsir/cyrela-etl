FROM openjdk:8

WORKDIR /opt

RUN apt-get update && apt-get install --quiet --yes curl python3 python3-venv python3-pip tar \
  && apt-get clean && rm -rf /var/lib/apt/lists/*

RUN curl --output spark.tgz https://downloads.apache.org/spark/spark-3.0.2/spark-3.0.2-bin-hadoop2.7.tgz

RUN tar --extract --file spark.tgz && rm spark.tgz && mv spark-3.0.2-bin-hadoop2.7 spark

COPY ./jars/ /opt/spark/jars

COPY ./requirements.txt ./

RUN pip3 install --quiet --requirement requirements.txt

COPY ./start-main.sh /start-main.sh
COPY ./start-worker.sh /start-worker.sh
