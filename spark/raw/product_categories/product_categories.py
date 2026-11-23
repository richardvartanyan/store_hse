
import os

from pyspark.sql import SparkSession

PG = dict(
    host='postgres',
    port=5432,
    dbname=os.environ.get('POSTGRES_DB'),
    user=os.environ.get('POSTGRES_USER'),
    password=os.environ.get('POSTGRES_PASSWORD'),
)

BUCKET = 'de-course'
CATEGORIES_PATH = f"s3a://{BUCKET}/raw/product_categories"

spark = (
    SparkSession.builder
    .config("spark.hadoop.fs.s3a.endpoint", os.environ.get('MINIO_ENDPOINT'))
    .config("spark.hadoop.fs.s3a.access.key", os.environ.get('MINIO_ROOT_USER'))
    .config("spark.hadoop.fs.s3a.secret.key", os.environ.get('MINIO_ROOT_PASSWORD'))
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

jdbc = dict(
    url=f"jdbc:postgresql://{PG['host']}:{PG['port']}/{PG['dbname']}",
    user=PG['user'],
    password=PG['password'] ,
    driver='org.postgresql.Driver',
)

query= """
    SELECT
        category_id,
        parent_category_id,
        category_name
    FROM public.product_categories
"""

df_categories = (
    spark.read.format("jdbc")
    .option("query", query)
    .options(**jdbc)
    .load()
)

(
    df_categories.write
    .format("parquet")
    .mode("overwrite")
    .save(CATEGORIES_PATH)
)

print(f"Сохранено: {CATEGORIES_PATH}, строк: {df_categories.count()}")
