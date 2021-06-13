# import libraries
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, IntegerType, DoubleType, StringType
from pyspark import SparkContext, SparkConf

# set config
conf = (
SparkConf()
    .set("spark.hadoop.fs.s3a.endpoint", "http://172.24.0.2:9000")
    .set("spark.hadoop.fs.s3a.access.key", "airflow_access_key")
    .set("spark.hadoop.fs.s3a.secret.key", "airflow_secret_key")
    .set("spark.hadoop.fs.s3a.path.style.access", True)
    .set("spark.hadoop.fs.s3a.fast.upload", True)
    .set("spark.hadoop.fs.s3a.connection.maximum", 100)
    .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
)

# apply config
sc = SparkContext(conf=conf).getOrCreate()

# main spark program
if __name__ == '__main__':

    # init spark session
    # name of the app
    spark = SparkSession \
            .builder \
            .appName("pr-wallet-data-tf") \
            .getOrCreate()

    # set log level to info
    spark.sparkContext.setLogLevel("INFO")

    schema = StructType() \
        .add("empresa",               IntegerType(), True) \
        .add("marca",                 StringType(),  True) \
        .add("empreendimento",        StringType(),  True) \
        .add("cliente",               StringType(),  True) \
        .add("regional",              StringType(),  True) \
        .add("obra",                  IntegerType(), True) \
        .add("bloco",                 IntegerType(), True) \
        .add("unidade",               IntegerType(), True) \
        .add("dt_venda",              StringType(),  True) \
        .add("dt_chaves",             StringType(),  True) \
        .add("carteira_sd_gerencial", IntegerType(), True) \
        .add("saldo_devedor",         DoubleType(),  True) \
        .add("data_base",             StringType(),  True) \
        .add("total_atraso",          DoubleType(),  True) \
        .add("faixa_de_atraso",       IntegerType(), True) \
        .add("dias_atraso",           IntegerType(), True) \
        .add("valor_pago_atualizado", DoubleType(),  True) \
        .add("valor_pago",            DoubleType(),  True) \
        .add("status",                StringType(),  True) \
        .add("dt_reneg",              StringType(),  True) \
        .add("descosn",               StringType(),  True) \
        .add("vaga",                  StringType(),  True) \
        .add("vgv",                   DoubleType(),  True)

    # get data from processing zone
    df_wallet = spark.read \
        .format('csv') \
        .options(header='true') \
		.schema(schema) \
        .load("s3a://curated/cyrela/wallet-data.csv")

    # display data into dataframe
    df_wallet.show()

    # print schema
    df_wallet.printSchema()

    # register df into sql engine
    df_wallet.createOrReplaceTempView("vw_wallet")

    # pre processing columns for neural network
    df_wallet_sql = spark.sql("""
        SELECT \
            (empresa               / (SELECT MAX(empresa)               FROM vw_wallet)) AS p_empresa, \
            CASE WHEN LOWER(marca) = 'cyrela' THEN 0.01
                 WHEN LOWER(marca) = 'living' THEN 0.02
                 WHEN LOWER(marca) = 'vivaz'  THEN 0.03
                 ELSE 0.00
            END AS p_marca, \
            (obra                  / (SELECT MAX(obra)                  FROM vw_wallet)) AS p_obra, \
            (bloco                 / (SELECT MAX(bloco)                 FROM vw_wallet)) AS p_bloco, \
            (unidade               / (SELECT MAX(unidade)               FROM vw_wallet)) AS p_unidade, \
            (carteira_sd_gerencial / (SELECT MAX(carteira_sd_gerencial) FROM vw_wallet)) AS p_carteira_sd_gerencial, \
            (saldo_devedor         / (SELECT MAX(saldo_devedor)         FROM vw_wallet)) AS p_saldo_devedor, \
            (ABS(dias_atraso)      / (SELECT MAX(ABS(dias_atraso))      FROM vw_wallet)) AS p_dias_atraso, \
            CASE WHEN dias_atraso >= 0 THEN 0
                 WHEN dias_atraso < 0   AND dias_atraso >= -15 THEN 1
                 WHEN dias_atraso < -15 AND dias_atraso >= -30 THEN 2
                 WHEN dias_atraso < -30 AND dias_atraso >= -90 THEN 3
                 ELSE 4
            END AS p_dias_atraso_category, \
            (valor_pago_atualizado / (SELECT MAX(valor_pago_atualizado) FROM vw_wallet)) AS p_valor_pago_atualizado, \
            (valor_pago            / (SELECT MAX(valor_pago)            FROM vw_wallet)) AS p_valor_pago, \
            (vgv                   / (SELECT MAX(vgv)                   FROM vw_wallet)) AS p_vgv \
        FROM vw_wallet
    """)

    # .add("empreendimento",        StringType(),  True) \
    # .add("dt_venda",              StringType(),  True) \
    # .add("dt_chaves",             StringType(),  True) \
    # .add("data_base",             StringType(),  True) \

    # display sql data frame
    df_wallet_sql.show(5)

    # save into csv format
    # curated zone
    df_wallet_sql.write \
		.format("csv") \
		.options(header='true') \
		.mode('overwrite') \
		.save("s3a://tensorflow/cyrela/wallet/")

    # stop spark session
    spark.stop()
