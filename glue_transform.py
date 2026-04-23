import sys
from pyspark.sql.functions import *
from pyspark.sql.types import StringType
from pyspark.sql.window import Window
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# =========================================================
# PARAMETERS
# =========================================================
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'bucket'])
bucket = args['bucket']

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# =========================================================
# HELPERS
# =========================================================
def safe_col(df, col_name):
    return col(col_name) if col_name in df.columns else lit(None)

def ensure_columns(df, cols):
    for c in cols:
        if c not in df.columns:
            df = df.withColumn(c, lit(None).cast(StringType()))
    return df

def trim_strings(df):
    for c, t in df.dtypes:
        if t == "string":
            df = df.withColumn(c, trim(col(c)))
    return df

# =========================================================
# LOAD DATA
# =========================================================
claimant_df = spark.read.option("header", True).option("inferSchema", True).csv(f"s3://{bucket}/raw/claimants/")
claims_df = spark.read.option("header", True).option("inferSchema", True).csv(f"s3://{bucket}/raw/claims/")
claims_v2_df = spark.read.option("header", True).option("inferSchema", True).csv(f"s3://{bucket}/raw/claims_v2/")
employers_df = spark.read.option("header", True).option("inferSchema", True).csv(f"s3://{bucket}/raw/employers/")
payments_df = spark.read.option("header", True).option("inferSchema", True).csv(f"s3://{bucket}/raw/payments/")
policies_df = spark.read.option("header", True).option("inferSchema", True).csv(f"s3://{bucket}/raw/policies/")



# =========================================================
# 1. CLAIMANTS
# =========================================================
claimant_df = trim_strings(claimant_df)

claimant_df = ensure_columns(claimant_df, [
    "claimant_id","first_name","last_name","date_of_birth","gender",
    "employment_start_date","employer_id","created_at","updated_at"
])

claimant_df = claimant_df.withColumn("updated_at", to_timestamp(safe_col(claimant_df,"updated_at")))

w = Window.partitionBy("claimant_id").orderBy(col("updated_at").desc())

claimant_df = claimant_df.withColumn("rn", row_number().over(w))

claimant_df = claimant_df.withColumn("effective_from", col("created_at")) \
    .withColumn("effective_to", lit(None).cast("timestamp")) \
    .withColumn("is_current", when(col("rn") == 1, lit(True)).otherwise(lit(False))) \
    .drop("rn")

# =========================================================
# 2. CLAIMS 
# =========================================================
claims_df = trim_strings(claims_df)
claims_v2_df = trim_strings(claims_v2_df)

required_cols = [
    "claim_id","claimant_id","policy_id",
    "incident_date","report_date",
    "claim_type","claim_status","claim_severity",
    "claim_amount","approved_amount",
    "created_at","updated_at"
]

claims_df = ensure_columns(claims_df, required_cols)
claims_v2_df = ensure_columns(claims_v2_df, required_cols)

def transform_claims(df):
    return df \
        .withColumn("incident_date", to_date(safe_col(df,"incident_date"))) \
        .withColumn("report_date", to_date(safe_col(df,"report_date"))) \
        .withColumn("claim_type", initcap(lower(safe_col(df,"claim_type")))) \
        .withColumn("claim_status", initcap(lower(safe_col(df,"claim_status")))) \
        .withColumn("claim_severity", initcap(lower(safe_col(df,"claim_severity")))) \
        .withColumn("claim_amount", col("claim_amount").cast("double")) \
        .withColumn("approved_amount", col("approved_amount").cast("double")) \
        .withColumn("created_at", to_timestamp(safe_col(df,"created_at"))) \
        .withColumn("updated_at", to_timestamp(safe_col(df,"updated_at"))) \
        .fillna({"approved_amount": 0}) \
        .filter(col("claim_id").isNotNull() & col("claimant_id").isNotNull())

claims_final_df = transform_claims(claims_df).withColumn("source", lit("v1")) \
    .unionByName(transform_claims(claims_v2_df).withColumn("source", lit("v2")),
                 allowMissingColumns=True)

claims_final_df = claims_final_df.dropDuplicates(["claim_id"])

# =========================================================
# 3. EMPLOYERS 
# =========================================================
employers_df = trim_strings(employers_df)

employers_df = ensure_columns(employers_df, ["employer_id","policy_id","updated_at"])

w_emp = Window.partitionBy("employer_id").orderBy(col("updated_at").desc())

employers_df = employers_df.withColumn("rn", row_number().over(w_emp)) \
    .withColumn("effective_from", col("created_at")) \
    .withColumn("effective_to", lit(None).cast("timestamp")) \
    .withColumn("is_current", col("rn") == 1) \
    .drop("rn")

# =========================================================
# 4. PAYMENTS 
# =========================================================
payments_df = trim_strings(payments_df)

payments_df = ensure_columns(payments_df, ["payment_id","claim_id"])

payments_df = payments_df.filter(
    col("payment_id").isNotNull() & col("claim_id").isNotNull()
)

payments_df = payments_df.dropDuplicates(["payment_id"])

# =========================================================
# 5. POLICIES (SCD2)
# =========================================================
policies_df = trim_strings(policies_df)

policies_df = ensure_columns(policies_df, [
    "policy_id","policy_number","start_date","end_date","updated_at"
])

policies_df = policies_df.withColumn("updated_at", to_timestamp(safe_col(policies_df,"updated_at")))

w_pol = Window.partitionBy("policy_id").orderBy(col("updated_at").desc())

policies_df = policies_df.withColumn("rn", row_number().over(w_pol)) \
    .withColumn("effective_from", col("start_date")) \
    .withColumn("effective_to", col("end_date")) \
    .withColumn("is_current", col("rn") == 1) \
    .drop("rn")

# =========================================================
# WRITE OUTPUT
# =========================================================
claimant_df.write.mode("overwrite").parquet(f"s3://{bucket}/staging/claimants/")
claims_final_df.write.mode("overwrite").parquet(f"s3://{bucket}/staging/claims/")
employers_df.write.mode("overwrite").parquet(f"s3://{bucket}/staging/employers/")
payments_df.write.mode("overwrite").parquet(f"s3://{bucket}/staging/payments/")
policies_df.write.mode("overwrite").parquet(f"s3://{bucket}/staging/policies/")

job.commit()