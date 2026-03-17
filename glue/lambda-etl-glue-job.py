import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import col, trim, sum

print("Starting Glue job")

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Spark Log4j Logger
logger = spark._jvm.org.apache.log4j.LogManager.getLogger(__name__)
logger.info("Spark logger initialized")

print("Glue Job Started")
logger.info("Glue Job Started")

# -------- READ CLEAN DATA --------

input_path = "s3://enterprise-data-clean/cleaned/raw"

print(f"Reading data from: {input_path}")
logger.info(f"Reading data from: {input_path}")

df = spark.read.csv(
    input_path,
    header=True,
    inferSchema=True
)

initial_rows = df.count()

print(f"Rows read from cleaned bucket: {initial_rows}")
logger.info(f"Rows read from cleaned bucket: {initial_rows}")

# -------- TRANSFORMATIONS --------

df = df.withColumn("product", trim(col("product")))

before = df.count()
df = df.filter(col("product").isNotNull())

removed = before - df.count()
print(f"Removed rows with null product: {removed}")
logger.info(f"Removed rows with null product: {removed}")

before = df.count()
df = df.filter(col("price") > 0)

removed = before - df.count()
print(f"Removed rows with invalid price: {removed}")
logger.info(f"Removed rows with invalid price: {removed}")

before = df.count()
df = df.filter(col("quantity") > 0)

removed = before - df.count()
print(f"Removed rows with invalid quantity: {removed}")
logger.info(f"Removed rows with invalid quantity: {removed}")

# -------- CREATE REVENUE --------

df = df.withColumn(
    "revenue",
    col("price") * col("quantity")
)

# -------- DATA QUALITY CHECKS --------

before = df.count()
df = df.dropDuplicates()

removed = before - df.count()
print(f"Removed duplicate rows: {removed}")
logger.info(f"Removed duplicate rows: {removed}")

before = df.count()
df = df.filter(col("order_id").isNotNull())

removed = before - df.count()
print(f"Removed rows with null order_id: {removed}")
logger.info(f"Removed rows with null order_id: {removed}")

clean_rows = df.count()

print(f"Rows after cleaning: {clean_rows}")
logger.info(f"Rows after cleaning: {clean_rows}")

# -------- AGGREGATION --------

sales_summary = df.groupBy("product").agg(
    sum("revenue").alias("revenue")
)

agg_rows = sales_summary.count()

print(f"Rows after aggregation: {agg_rows}")
logger.info(f"Rows after aggregation: {agg_rows}")

# -------- WRITE OUTPUT --------

output_path = "s3://enterprise-data-processed/sales-summary/"

print(f"Writing output to: {output_path}")
logger.info(f"Writing output to: {output_path}")

sales_summary.write.mode("overwrite").parquet(output_path)

print("Data successfully written")
logger.info("Data successfully written")

print("Glue Job Completed Successfully")
logger.info("Glue Job Completed Successfully")