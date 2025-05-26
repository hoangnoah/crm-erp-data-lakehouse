from pyspark.sql import SparkSession
import os

# Lấy key từ biến môi trường
AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")

# Khởi tạo SparkSession + Iceberg + Hive Metastore
spark = (
    SparkSession.builder
    .appName("IcebergSilverETL")
    .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY)
    .getOrCreate()
)

    

spark.sql("CREATE DATABASE IF NOT EXISTS lakehouse.bronze")
spark.sql("CREATE DATABASE IF NOT EXISTS lakehouse.silver")

# 1. CRM CUSTOMERS
df = spark.read.option("header", True).csv("s3a://warehouse/bronze/crm/cust_info.csv")

df.writeTo("lakehouse.bronze.crm_cust_info").createOrReplace()

spark.sql("""
CREATE TABLE IF NOT EXISTS lakehouse.silver.crm_customers (
    customer_crm_id INT, customer_id STRING, firstname STRING, lastname STRING,
    marital_status STRING, gender STRING, join_crm_date DATE
) USING iceberg
""")

spark.sql("""
INSERT OVERWRITE lakehouse.silver.crm_customers
SELECT 
    CAST(cst_id AS INT),
    cst_key,
    TRIM(cst_firstname),
    TRIM(cst_lastname),
    CASE 
        WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
        WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
        ELSE 'n/a'
    END,
    CASE 
        WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
        ELSE 'n/a'
    END,
    CAST(cst_create_date AS DATE)
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS rn
    FROM lakehouse.bronze.crm_cust_info
    WHERE cst_id IS NOT NULL
) t WHERE rn = 1
""")

# 2. CRM PRODUCTS
df = spark.read.option("header", True).csv("s3a://warehouse/bronze/crm/prd_info.csv")


df.writeTo("lakehouse.bronze.crm_prd_info").createOrReplace()

spark.sql("""
CREATE TABLE IF NOT EXISTS lakehouse.silver.crm_products (
    product_crm_id INT, product_id STRING, category_id STRING,
    product_name STRING, product_cost INT, product_line STRING,
    start_date DATE, end_date DATE
) USING iceberg
""")

spark.sql("""
INSERT OVERWRITE lakehouse.silver.crm_products
SELECT 
    CAST(prd_id AS INT),
    SUBSTRING(prd_key, 7),
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_'),
    prd_nm,
    CAST(COALESCE(prd_cost, 0) AS INT),
    CASE 
        WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
        WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
        WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
        WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
        ELSE 'n/a'
    END,
    CAST(prd_start_dt AS DATE),
    CAST(
        LEAD(CAST(prd_start_dt AS DATE)) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 
        AS DATE
    )
FROM lakehouse.bronze.crm_prd_info
""")

# 3. CRM SALES
df = spark.read.option("header", True).csv("s3a://warehouse/bronze/crm/sales_details.csv")
df.writeTo("lakehouse.bronze.crm_sales_details").createOrReplace()

spark.sql("""
CREATE TABLE IF NOT EXISTS lakehouse.silver.crm_sales_details (
    order_id STRING, product_id STRING, customer_crm_id INT,
    order_date DATE, ship_date DATE, due_date DATE,
    total_amount INT, quantity INT, price INT
) USING iceberg
""")

spark.sql("""
INSERT OVERWRITE lakehouse.silver.crm_sales_details
SELECT 
    sls_ord_num,
    sls_prd_key,
    CAST(sls_cust_id AS INT),
    CASE WHEN sls_order_dt RLIKE '^[0-9]{8}$' THEN TO_DATE(CAST(sls_order_dt AS STRING), 'yyyyMMdd') ELSE NULL END,
    CASE WHEN sls_ship_dt RLIKE '^[0-9]{8}$' THEN TO_DATE(CAST(sls_ship_dt AS STRING), 'yyyyMMdd') ELSE NULL END,
    CASE WHEN sls_due_dt RLIKE '^[0-9]{8}$' THEN TO_DATE(CAST(sls_due_dt AS STRING), 'yyyyMMdd') ELSE NULL END,
    CAST(
        CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
             THEN sls_quantity * ABS(sls_price)
             ELSE sls_sales END AS INT),
    CAST(sls_quantity AS INT),
    CAST(
        CASE WHEN sls_price IS NULL OR sls_price <= 0
             THEN sls_sales / NULLIF(sls_quantity, 0)
             ELSE sls_price END AS INT)
FROM lakehouse.bronze.crm_sales_details
""")

# 4. ERP DEMOGRAPHIC
df = spark.read.option("header", True).csv("s3a://warehouse/bronze/erp/CUST_AZ12.csv")
df.writeTo("lakehouse.bronze.erp_cust_az12").createOrReplace()

spark.sql("""
CREATE TABLE IF NOT EXISTS lakehouse.silver.erp_customer_demographic (
    customer_id STRING, birthday DATE, gender STRING
) USING iceberg
""")

spark.sql("""
INSERT OVERWRITE lakehouse.silver.erp_customer_demographic
SELECT 
    CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4) ELSE cid END,
    CASE WHEN bdate > CURRENT_DATE() THEN NULL ELSE CAST(bdate AS DATE) END,
    CASE 
        WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
        WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
        ELSE 'n/a'
    END
FROM lakehouse.bronze.erp_cust_az12
""")

# 5. ERP LOCATION
df = spark.read.option("header", True).csv("s3a://warehouse/bronze/erp/LOC_A101.csv")
df.writeTo("lakehouse.bronze.erp_loc_a101").createOrReplace()

spark.sql("""
CREATE TABLE IF NOT EXISTS lakehouse.silver.erp_customer_location (
    customer_id STRING, customer_country STRING
) USING iceberg
""")

spark.sql("""
INSERT OVERWRITE lakehouse.silver.erp_customer_location
SELECT 
    REPLACE(cid, '-', ''),
    CASE 
        WHEN TRIM(cntry) = 'DE' THEN 'Germany'
        WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
        WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
        ELSE TRIM(cntry)
    END
FROM lakehouse.bronze.erp_loc_a101
""")

# 6. ERP CATEGORY
df = spark.read.option("header", True).csv("s3a://warehouse/bronze/erp/PX_CAT_G1V2.csv")
df.writeTo("lakehouse.bronze.erp_px_cat_g1v2").createOrReplace()

spark.sql("""
CREATE TABLE IF NOT EXISTS lakehouse.silver.erp_categories (
    category_id STRING, category_name STRING, subcategory_name STRING, maintenance_flag BOOLEAN
) USING iceberg
""")

spark.sql("""
INSERT OVERWRITE lakehouse.silver.erp_categories
SELECT 
    id, cat, subcat,
    CASE WHEN UPPER(TRIM(maintenance)) = 'YES' THEN TRUE ELSE FALSE END
FROM lakehouse.bronze.erp_px_cat_g1v2
""")