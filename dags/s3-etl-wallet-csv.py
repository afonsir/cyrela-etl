# [START import_module]
import pandas as pd
from minio import Minio
from os import getenv
from io import BytesIO
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor
from airflow.providers.amazon.aws.operators.s3_list import S3ListOperator
from airflow.providers.amazon.aws.operators.s3_copy_object import S3CopyObjectOperator
from airflow.providers.amazon.aws.operators.s3_delete_objects import S3DeleteObjectsOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
# [END import_module]

# [START env_variables]
MINIO = getenv("MINIO", "minio:9000")
ACCESS_KEY = getenv("ACCESS_KEY", "airflow_access_key")
SECRET_ACCESS = getenv("SECRET_ACCESS", "airflow_secret_key")
POSTGRESDB = getenv("POSTGRESDB", "postgresql://datawarehouse:datawarehouse@postgres-dw:5432/datawarehouse")
BUSINESS_ORIGINAL_FILE_LOCATION = getenv("BUSINESS_ORIGINAL_FILE_LOCATION", "cyrela/wallet-data.csv")
LANDING_ZONE = getenv("LANDING_ZONE", "landing")
PROCESSING_ZONE = getenv("PROCESSING_ZONE", "processing")
CURATED_ZONE = getenv("CURATED_ZONE", "curated")
CURATED_BUSINESS_CSV_FILE = getenv("CURATED_BUSINESS_CSV_FILE", "cyrela/wallet-data.csv")
# [END env_variables]

# [START default_args]
default_args = {
    'owner': 'afonso de sousa costa',
    'start_date': datetime(2021, 6, 12),
    'depends_on_past': False,
    'email': ['afonsir21@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)}
# [END default_args]

# [START instantiate_dag]
dag = DAG(
    's3-etl-wallet-csv',
    default_args=default_args,
    schedule_interval='@daily',
    tags=['development', 's3', 'sensor', 'minio', 'python', 'postgresdb', 'spark', 'tensorflow'])
# [END instantiate_dag]


# [START functions]
def format_date_to_postgres(string_date):
    if isinstance(string_date, str):
        return datetime.strptime(string_date, '%d/%m/%Y').strftime('%Y-%m-%d')

def parse_wallet_data(ti):
    # airflow feature that allows the exchange messages for sharing states
    # defined by a key and value, to get access use the task id
    get_xcom_file_name = ti.xcom_pull(task_ids=['list_file_s3'])

    print(get_xcom_file_name)
    print(type(get_xcom_file_name))

    # set up connectivity with minio storage
    client = Minio(MINIO, ACCESS_KEY, SECRET_ACCESS, secure=False)

    # download file from bucket
    # processing engine access processing bucket [always]
    # empty if nothing to process
    # reading xcom from list s3 task
    obj_wallet = client.get_object(
        PROCESSING_ZONE,
        get_xcom_file_name[0][0],
    )

    # read csv file using pandas
    df_header = [
	'empresa',
	'marca',
	'empreendimento',
	'cliente',
	'regional',
	'obra',
	'bloco',
	'unidade',
	'dt_venda',
	'dt_chaves',
	'carteira_sd_gerencial',
	'saldo_devedor',
	'data_base',
	'total_atraso',
	'faixa_de_atraso',
	'dias_atraso',
	'valor_pago_atualizado',
	'valor_pago',
	'status',
	'dt_reneg',
	'descosn',
	'vaga',
	'vgv'
	]
    df_wallet = pd.read_csv(obj_wallet, header=1, names=df_header)

    for date_column in ['dt_venda', 'dt_chaves', 'data_base', 'dt_reneg']:
        df_wallet[date_column] = df_wallet[date_column].apply(format_date_to_postgres)

    # dataframe to csv - encode and buffer bytes
    csv_bytes = df_wallet.to_csv(header=True, index=False).encode('utf-8')
    csv_buffer = BytesIO(csv_bytes)

    # writing into minio storage [curated zone]
    client.put_object(
        CURATED_ZONE,
        CURATED_BUSINESS_CSV_FILE,
        data=csv_buffer,
        length=len(csv_bytes),
        content_type='application/csv')

def write_wallet_dt_postgres():
    # set up connectivity with minio storage
    client = Minio(MINIO, ACCESS_KEY, SECRET_ACCESS, secure=False)

    # download file from bucket
    obj_wallet = client.get_object(CURATED_ZONE, CURATED_BUSINESS_CSV_FILE)

    # read file from curated zone
    df_wallet = pd.read_csv(obj_wallet)

    # insert pandas data frame into postgres db [sqlalchemy]
    # connect into postgres and ingest data
    #  adding method multi
    postgres_engine = create_engine(POSTGRESDB)

    df_wallet.to_sql('wallet', postgres_engine, if_exists='append', index=False, chunksize=10)
# [END functions]


# [START set_tasks]
# verify if new file has landed into bucket
verify_file_existence_landing = S3KeySensor(
    task_id='verify_file_existence_landing',
    bucket_name=LANDING_ZONE,
    bucket_key='cyrela/*.csv',
    wildcard_match=True,
    timeout=18 * 60 * 60,
    poke_interval=120,
    aws_conn_id='minio',
    dag=dag)

# list all files inside of a bucket [names]
list_file_s3 = S3ListOperator(
    task_id='list_file_s3',
    bucket=LANDING_ZONE,
    prefix='cyrela/',
    delimiter='/',
    aws_conn_id='minio',
    dag=dag)

# copy file from landing to processing zone
copy_s3_file_processed_zone = S3CopyObjectOperator(
    task_id='copy_s3_file_processed_zone',
    source_bucket_name=LANDING_ZONE,
    source_bucket_key=BUSINESS_ORIGINAL_FILE_LOCATION,
    dest_bucket_name=PROCESSING_ZONE,
    dest_bucket_key=BUSINESS_ORIGINAL_FILE_LOCATION,
    aws_conn_id='minio',
    dag=dag)

# delete file from landing zone [old file]
delete_s3_file_landing_zone = S3DeleteObjectsOperator(
    task_id='delete_s3_file_landing_zone',
    bucket=LANDING_ZONE,
    keys=BUSINESS_ORIGINAL_FILE_LOCATION,
    aws_conn_id='minio',
    dag=dag)

# apply transformation [python function]
process_wallet_data = PythonOperator(
    task_id='process_wallet_data',
    python_callable=parse_wallet_data,
    dag=dag)

tf_pre_processing_wallet_data = SparkSubmitOperator(
    task_id='tf_pre_processing_wallet_data',
    conn_id='spark_local',
    application='/spark-jobs/pr-wallet-data-tf.py',
    jars='/opt/spark/jars/aws-java-sdk-1.7.4.jar,/opt/spark/jars/hadoop-aws-2.7.3.jar',
    total_executor_cores=1,
    executor_cores=1,
    executor_memory='1g',
    driver_memory='1g',
    dag=dag)

# delete file from processed zone
delete_s3_file_processed_zone = S3DeleteObjectsOperator(
    task_id='delete_s3_file_processed_zone',
    bucket=PROCESSING_ZONE,
    keys=BUSINESS_ORIGINAL_FILE_LOCATION,
    aws_conn_id='minio',
    dag=dag)

# create table on postgres [if not exists]
create_postgres_tb = PostgresOperator(
    task_id='create_postgres_tb',
    postgres_conn_id='postgresdb_psql',
    sql="""
        CREATE TABLE IF NOT EXISTS wallet 
        (
            id                    SERIAL PRIMARY KEY, 
            empresa               VARCHAR NULL,
            marca                 VARCHAR NULL,
            empreendimento        VARCHAR NULL,
            cliente               VARCHAR NULL,
            regional              VARCHAR NULL,
            obra                  VARCHAR NULL,
            bloco                 VARCHAR NULL,
            unidade               VARCHAR NULL,
            dt_venda              DATE NULL,
            dt_chaves             DATE NULL,
            carteira_sd_gerencial VARCHAR NULL,
            saldo_devedor         NUMERIC NULL,
            data_base             DATE NULL,
            total_atraso          NUMERIC NULL,
            faixa_de_atraso       VARCHAR NULL,
            dias_atraso           VARCHAR NULL,
            valor_pago_atualizado NUMERIC NULL,
            valor_pago            NUMERIC NULL,
            status                VARCHAR NULL,
            dt_reneg              VARCHAR NULL,
            descosn               VARCHAR NULL,
            vaga                  VARCHAR NULL,
            vgv                   NUMERIC NULL
        );
        """,
    dag=dag)

# write data into postgres database ingestion
# of [~ 2 million] rows on table
write_wallet_dt_postgresdb = PythonOperator(
    task_id='write_wallet_dt_postgresdb',
    python_callable=write_wallet_dt_postgres,
    dag=dag)
# [END set_tasks]

# [START task_sequence]
verify_file_existence_landing >> list_file_s3 >> copy_s3_file_processed_zone >> delete_s3_file_landing_zone >> process_wallet_data >> tf_pre_processing_wallet_data
verify_file_existence_landing >> list_file_s3 >> copy_s3_file_processed_zone >> delete_s3_file_landing_zone >> process_wallet_data >> delete_s3_file_processed_zone >> create_postgres_tb >> write_wallet_dt_postgresdb
# [END task_sequence]
