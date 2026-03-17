import boto3
import pandas as pd
import io
import logging

# Setup logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")
glue = boto3.client("glue")

CLEAN_BUCKET = "enterprise-data-clean-26"
GLUE_JOB_NAME = "lambda-etl-job"

def lambda_handler(event, context):

    record = event['Records'][0]
    bucket = record['s3']['bucket']['name']
    key = record['s3']['object']['key']

    logger.info(f"File received: s3://{bucket}/{key}")

    response = s3.get_object(Bucket=bucket, Key=key)

    # -------- READ FILE --------

    if key.endswith(".csv"):
        df = pd.read_csv(response['Body'])
    else:
        df = pd.read_excel(response['Body'])

    logger.info(f"Rows before cleaning: {len(df)}")

    # -------- VALIDATION --------

    required_columns = [
        "order_id","product","price",
        "quantity","region","sales_rep","order_date"
    ]

    for col in required_columns:
        if col not in df.columns:
            logger.error(f"Missing required column: {col}")
            raise Exception(f"Missing column {col}")

    # -------- DATA CLEANSING --------

    initial_rows = len(df)

    # remove null products
    df = df[df["product"].notna()]
    logger.info(f"Removed null product rows: {initial_rows - len(df)}")

    # trim whitespace
    df["product"] = df["product"].astype(str).str.strip()

    before = len(df)
    df = df[~df["product"].isin(["nan","None",""])]
    logger.info(f"Removed invalid product rows: {before - len(df)}")

    # remove missing region
    before = len(df)
    df = df[df["region"].notna()]
    logger.info(f"Removed rows with missing region: {before - len(df)}")

    # remove invalid price
    before = len(df)
    df = df[df["price"].notna()]
    df = df[df["price"] > 0]
    df = df[df["price"] < 10000]
    logger.info(f"Removed rows with invalid price: {before - len(df)}")

    # remove invalid quantity
    before = len(df)
    df = df[df["quantity"].notna()]
    df = df[df["quantity"] > 0]
    df = df[df["quantity"] < 100]
    logger.info(f"Removed rows with invalid quantity: {before - len(df)}")

    # validate order_date
    before = len(df)
    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
    df = df[df["order_date"].notna()]
    logger.info(f"Removed rows with invalid date: {before - len(df)}")

    # remove duplicate orders
    before = len(df)
    df = df.drop_duplicates(subset=["order_id"])
    logger.info(f"Removed duplicate rows: {before - len(df)}")

    logger.info(f"Rows after cleaning: {len(df)}")

    # -------- CALCULATE REVENUE --------

    df["price"] = df["price"].astype(float)
    df["quantity"] = df["quantity"].astype(int)

    df["revenue"] = df["price"] * df["quantity"]

    # -------- SAVE CLEAN DATA --------

    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)

    cleaned_key = f"cleaned/{key}"

    s3.put_object(
        Bucket=CLEAN_BUCKET,
        Key=cleaned_key,
        Body=csv_buffer.getvalue()
    )

    logger.info(f"Cleaned file written to s3://{CLEAN_BUCKET}/{cleaned_key}")

    # -------- TRIGGER GLUE JOB --------

    glue_response = glue.start_job_run(
        JobName=GLUE_JOB_NAME,
        Arguments={
            "--input_path": f"s3://{CLEAN_BUCKET}/cleaned/"
        }
    )

    logger.info(f"Glue Job Started: {glue_response['JobRunId']}")

    return {
        "statusCode": 200,
        "body": f"Rows after cleaning: {len(df)}"
    }