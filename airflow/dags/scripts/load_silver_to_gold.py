from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, row_number, lit, dayofmonth, month, year, quarter, dayofweek, date_format
from pyspark.sql.window import Window

# Tạo SparkSession với cấu hình Iceberg
spark = SparkSession.builder \
    .appName("silver_to_gold_iceberg") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", "s3a://warehouse/gold/icebergTables") \
    .getOrCreate()

# Đọc dữ liệu silver từ MinIO (parquet)
df_customers = spark.read.format("iceberg").load("s3a://warehouse/silver/icebergTables/crm/crm_customers")
df_erp_demo = spark.read.format("iceberg").load("s3a://warehouse/silver/icebergTables/erp/erp_customer_demographic")
df_erp_location = spark.read.format("iceberg").load("s3a://warehouse/silver/icebergTables/erp/erp_customer_location")

df_products = spark.read.format("iceberg").load("s3a://warehouse/silver/icebergTables/crm/crm_products")
df_categories = spark.read.format("iceberg").load("s3a://warehouse/silver/icebergTables/erp/erp_categories")

df_sales = spark.read.format("iceberg").load("s3a://warehouse/silver/icebergTables/crm/crm_sales_details")

# ========================
# Tạo bảng dim_customers
# ========================

df_dim_customers = df_customers.alias("ci") \
    .join(df_erp_demo.alias("ca"), col("ci.customer_id") == col("ca.customer_id"), "left") \
    .join(df_erp_location.alias("la"), col("ci.customer_id") == col("la.customer_id"), "left") \
    .select(
        col("ci.customer_id"),
        col("ci.customer_crm_id"),
        col("ci.firstname"),
        col("ci.lastname"),
        col("la.customer_country"),
        col("ci.marital_status"),
        when(col("ci.gender") != "n/a", col("ci.gender")).otherwise(
            when(col("ca.gender").isNotNull(), col("ca.gender")).otherwise(lit("n/a"))
        ).alias("gender"),
        col("ca.birthday"),
        col("ci.join_crm_date")
    )

window_cust = Window.orderBy("customer_id")
df_dim_customers = df_dim_customers.withColumn("customer_key", row_number().over(window_cust))

# Ghi Iceberg
df_dim_customers.writeTo("local.dim_customers").createOrReplace()

# ========================
# Tạo bảng dim_products
# ========================

df_dim_products = df_products.alias("pro") \
    .join(df_categories.alias("cate"), "category_id", "left") \
    .select(
        col("pro.product_id"),
        col("pro.product_crm_id"),
        col("pro.product_name"),
        col("pro.category_id"),
        col("cate.category_name"),
        col("cate.subcategory_name"),
        col("cate.maintenance_flag"),
        col("pro.product_cost"),
        col("pro.product_line"),
        col("pro.start_date"),
        col("pro.end_date")
    )

window_prod = Window.orderBy("product_id", "start_date", "end_date")
df_dim_products = df_dim_products.withColumn("product_key", row_number().over(window_prod))

# Ghi Iceberg
df_dim_products.writeTo("local.dim_products").createOrReplace()

# ========================
# Tạo bảng dim_dates
# ========================

min_date = df_sales.selectExpr("min(order_date) as min_date").collect()[0]["min_date"]
max_date = df_sales.selectExpr("max(due_date) as max_date").collect()[0]["max_date"]

from pyspark.sql.functions import sequence, explode, to_date

date_seq_df = spark.sql(f"SELECT sequence(to_date('{min_date}'), to_date(date_add('{max_date}', 365)), interval 1 day) as date_seq") \
    .select(explode("date_seq").alias("date_id"))

df_dim_dates = date_seq_df.select(
    col("date_id"),
    dayofmonth("date_id").alias("day"),
    month("date_id").alias("month"),
    year("date_id").alias("year"),
    quarter("date_id").alias("quarter"),
    dayofweek("date_id").alias("day_of_week"),
    date_format("date_id", "EEEE").alias("day_name"),
    date_format("date_id", "MMMM").alias("month_name"),
    (dayofweek("date_id").isin([1,7])).cast("boolean").alias("is_weekend")
)

df_dim_dates.writeTo("local.dim_dates").createOrReplace()

# ========================
# Tạo bảng fact_sales
# ========================

df_fact_sales = df_sales.alias("sd") \
    .join(df_dim_products.alias("pro"), col("sd.product_id") == col("pro.product_id"), "left") \
    .join(df_dim_customers.alias("cust"), col("sd.customer_crm_id") == col("cust.customer_crm_id"), "left") \
    .select(
        col("sd.order_id"),
        col("pro.product_key"),
        col("cust.customer_key"),
        col("sd.order_date"),
        col("sd.ship_date"),
        col("sd.due_date"),
        col("sd.price"),
        col("sd.quantity"),
        col("sd.total_amount")
    )

df_fact_sales.writeTo("local.fact_sales").createOrReplace()
