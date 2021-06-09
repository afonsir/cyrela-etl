FROM apache/airflow:2.1.0-python3.7

WORKDIR /opt

COPY ./requirements.txt ./

RUN pip install --requirement requirements.txt
