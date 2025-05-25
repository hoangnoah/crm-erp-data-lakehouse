from pyspark.sql import SparkSession
from pyspark.sql.functions import sequence, explode, to_date, date_add, lit
import os

AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")

spark = (
    SparkSession.builder
    .appName("GoldTableETL")
    .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY)
    .enableHiveSupport()
    .getOrCreate()
)

# Tạo database gold nếu chưa có
spark.sql("CREATE DATABASE IF NOT EXISTS lakehouse.gold")

# Tạo bảng dim_customers gold
spark.sql("""
CREATE TABLE IF NOT EXISTS lakehouse.gold.dim_customers (
    customer_id STRING,
    customer_crm_id STRING,
    firstname STRING,
    lastname STRING,
    customer_country STRING,
    marital_status STRING,
    gender STRING,
    birthday DATE,
    join_crm_date DATE,
    customer_key INT
) USING iceberg
""")

spark.sql("""
INSERT OVERWRITE lakehouse.gold.dim_customers
SELECT
    ci.customer_id,
    ci.customer_crm_id,
    ci.firstname,
    ci.lastname,
    la.customer_country,
    ci.marital_status,
    CASE 
        WHEN ci.gender != 'n/a' THEN ci.gender
        WHEN ca.gender IS NOT NULL THEN ca.gender
        ELSE 'n/a'
    END AS gender,
    ca.birthday,
    ci.join_crm_date,
    ROW_NUMBER() OVER (ORDER BY ci.customer_id) AS customer_key
FROM lakehouse.silver.crm_customers ci
LEFT JOIN lakehouse.silver.erp_customer_demographic ca ON ci.customer_id = ca.customer_id
LEFT JOIN lakehouse.silver.erp_customer_location la ON ci.customer_id = la.customer_id
""")

# Tạo bảng dim_products gold
spark.sql("""
CREATE TABLE IF NOT EXISTS lakehouse.gold.dim_products (
    product_id STRING,
    product_crm_id STRING,
    product_name STRING,
    category_id STRING,
    category_name STRING,
    subcategory_name STRING,
    maintenance_flag BOOLEAN,
    product_cost DOUBLE,
    product_line STRING,
    start_date DATE,
    end_date DATE,
    product_key INT
) USING iceberg
""")

spark.sql("""
INSERT OVERWRITE lakehouse.gold.dim_products
SELECT
    pro.product_id,
    pro.product_crm_id,
    pro.product_name,
    pro.category_id,
    cate.category_name,
    cate.subcategory_name,
    cate.maintenance_flag,
    pro.product_cost,
    pro.product_line,
    pro.start_date,
    pro.end_date,
    ROW_NUMBER() OVER (ORDER BY pro.product_id, pro.start_date, pro.end_date) AS product_key
FROM lakehouse.silver.crm_products pro
LEFT JOIN lakehouse.silver.erp_categories cate ON pro.category_id = cate.category_id
""")

# Lấy min max ngày từ bảng sales silver
min_max = spark.sql("""
SELECT MIN(order_date) AS min_date, MAX(due_date) AS max_date FROM lakehouse.silver.crm_sales_details
""").collect()[0]

min_date = min_max.min_date
max_date = min_max.max_date

# Tạo DataFrame với sequence ngày
date_df = spark.createDataFrame([(1,)], ["id"]) \
    .select(
        explode(
            sequence(
                to_date(lit(min_date)),
                date_add(to_date(lit(max_date)), 365)
            )
        ).alias("date_seq")
    )
date_df.createOrReplaceTempView("date_sequence")

# Tạo bảng dim_dates gold
spark.sql("""
CREATE TABLE IF NOT EXISTS lakehouse.gold.dim_dates (
    date_id DATE,
    day INT,
    month INT,
    year INT,
    quarter INT,
    day_of_week INT,
    day_name STRING,
    month_name STRING,
    is_weekend BOOLEAN
) USING iceberg
""")

spark.sql("""
INSERT OVERWRITE lakehouse.gold.dim_dates
SELECT
    date_seq AS date_id,
    DAY(date_seq) AS day,
    MONTH(date_seq) AS month,
    YEAR(date_seq) AS year,
    QUARTER(date_seq) AS quarter,
    DAYOFWEEK(date_seq) AS day_of_week,
    DATE_FORMAT(date_seq, 'EEEE') AS day_name,
    DATE_FORMAT(date_seq, 'MMMM') AS month_name,
    (DAYOFWEEK(date_seq) IN (1,7)) AS is_weekend
FROM date_sequence
""")

# Tạo bảng fact_sales gold
spark.sql("""
CREATE TABLE IF NOT EXISTS lakehouse.gold.fact_sales (
    order_id STRING,
    product_key INT,
    customer_key INT,
    order_date DATE,
    ship_date DATE,
    due_date DATE,
    price DOUBLE,
    quantity INT,
    total_amount DOUBLE
) USING iceberg
""")

spark.sql("""
INSERT OVERWRITE lakehouse.gold.fact_sales
SELECT
    sd.order_id,
    pro.product_key,
    cust.customer_key,
    sd.order_date,
    sd.ship_date,
    sd.due_date,
    sd.price,
    sd.quantity,
    sd.total_amount
FROM lakehouse.silver.crm_sales_details sd
LEFT JOIN lakehouse.gold.dim_products pro ON sd.product_id = pro.product_id
LEFT JOIN lakehouse.gold.dim_customers cust ON sd.customer_crm_id = cust.customer_crm_id
""")
