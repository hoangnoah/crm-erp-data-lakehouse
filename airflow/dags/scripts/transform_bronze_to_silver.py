from pyspark.sql import SparkSession
from pyspark.sql.functions import abs,col, trim, upper, when, row_number,substring, regexp_replace, isnan, isnull, lead, expr, to_date, current_date
from pyspark.sql.window import Window


spark = SparkSession.builder \
    .appName("silver_transform") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", "s3a://warehouse/silver/icebergTables") \
    .getOrCreate()

# crm_customers

# Đọc từ bronze layer (MinIO)
df = spark.read.option("header", True).csv("s3a://warehouse/bronze/crm/cust_info.csv")

# Chỉ lấy bản ghi mới nhất mỗi customer
window_spec = Window.partitionBy("cst_id").orderBy(col("cst_create_date").desc())
df = df.withColumn("row_num", row_number().over(window_spec)).filter(col("row_num") == 1)

# ETL giống SQL Server
df = df.select(
    col("cst_id").alias("customer_crm_id"),
    col("cst_key").alias("customer_id"),
    trim(col("cst_firstname")).alias("firstname"),
    trim(col("cst_lastname")).alias("lastname"),
    when(upper(trim(col("cst_marital_status"))) == "S", "Single")
        .when(upper(trim(col("cst_marital_status"))) == "M", "Married")
        .otherwise("n/a").alias("marital_status"),
    when(upper(trim(col("cst_gndr"))) == "F", "Female")
        .when(upper(trim(col("cst_gndr"))) == "M", "Male")
        .otherwise("n/a").alias("gender"),
    col("cst_create_date").alias("join_crm_date")
)

# Ghi xuống silver layer (Parquet trên MinIO)
df.write.mode("overwrite").parquet("s3a://warehouse/silver/parquetFiles/crm/crm_customers")
df.writeTo("local.crm_customers").createOrReplace()

# crm_products

# Load
df_products = spark.read.option("header", True).csv("s3a://warehouse/bronze/crm/prd_info.csv")

# Chuẩn hóa cột cần thiết và giữ lại prd_key cho window
df_products = df_products.withColumn("product_id", substring("prd_key", 7, 100)) \
    .withColumn("category_id", regexp_replace(substring("prd_key", 1, 5), "-", "_")) \
    .withColumn("product_line", when(upper(trim(col("prd_line"))) == "M", "Mountain")
        .when(upper(trim(col("prd_line"))) == "R", "Road")
        .when(upper(trim(col("prd_line"))) == "S", "Other Sales")
        .when(upper(trim(col("prd_line"))) == "T", "Touring")
        .otherwise("n/a")) \
    .withColumn("start_date", col("prd_start_dt").cast("date"))

# Tính end_date
window_spec = Window.partitionBy("prd_key").orderBy("prd_start_dt")
df_products = df_products.withColumn("end_date", lead("start_date").over(window_spec))
df_products = df_products.withColumn("end_date", expr("date_sub(end_date, 1)"))

# Chọn lại cột final
df_products = df_products.select(
    col("prd_id").alias("product_crm_id"),
    "product_id",
    "category_id",
    col("prd_nm").alias("product_name"),
    when(col("prd_cost").isNull(), 0).otherwise(col("prd_cost")).alias("product_cost"),
    "product_line",
    "start_date",
    "end_date"
)

# Save
df_products.write.mode("overwrite").parquet("s3a://warehouse/silver/parquetFiles/crm/crm_products")
df_products.writeTo("local.crm_products").createOrReplace()
#crm_sales_details

# Đọc từ MinIO
df_sales = spark.read.option("header", True).csv("s3a://warehouse/bronze/crm/sales_details.csv")

# Format ngày đơn giản cho các cột ngày (vì file gốc kiểu INT 20220504 → cần ép kiểu)
def parse_date(colname):
    return when((col(colname).cast("string").rlike("^[0-9]{8}$")),
                to_date(col(colname).cast("string"), "yyyyMMdd")
            ).otherwise(None)

df_sales = df_sales.select(
    col("sls_ord_num").alias("order_id"),
    col("sls_prd_key").alias("product_id"),
    col("sls_cust_id").alias("customer_crm_id"),
    parse_date("sls_order_dt").alias("order_date"),
    parse_date("sls_ship_dt").alias("ship_date"),
    parse_date("sls_due_dt").alias("due_date"),
    when((col("sls_sales").isNull()) | (col("sls_sales") <= 0) | 
         (col("sls_sales") != col("sls_quantity") * abs(col("sls_price"))),
         col("sls_quantity") * abs(col("sls_price"))).otherwise(col("sls_sales")).alias("total_amount"),
    col("sls_quantity").cast("int").alias("quantity"),
    when((col("sls_price").isNull()) | (col("sls_price") <= 0),
         col("sls_sales") / when(col("sls_quantity") != 0, col("sls_quantity")).otherwise(None))
         .otherwise(col("sls_price")).alias("price")
)

# Ghi ra silver
df_sales.write.mode("overwrite").parquet("s3a://warehouse/silver/parquetFiles/crm/crm_sales_details")
df_sales.writeTo("local.crm_sales_details").createOrReplace()

# erp_customer_location

df_location = spark.read.option("header", True).csv("s3a://warehouse/bronze/erp/LOC_A101.csv")

df_location = df_location.select(
    regexp_replace(col("cid"), "-", "").alias("customer_id"),
    when(trim(col("cntry")) == "DE", "Germany")
    .when(trim(col("cntry")).isin("US", "USA"), "United States")
    .when(trim(col("cntry")) == "", "n/a")
    .when(col("cntry").isNull(), "n/a")
    .otherwise(trim(col("cntry"))).alias("customer_country")
)

df_location.write.mode("overwrite").parquet("s3a://warehouse/silver/parquetFiles/erp/erp_customer_location")
df_location.writeTo("local.erp_customer_location").createOrReplace()
# erp_customer_demographic

df_demo = spark.read.option("header", True).csv("s3a://warehouse/bronze/erp/CUST_AZ12.csv")

df_demo = df_demo.select(
    when(col("cid").startswith("NAS"), substring(col("cid"), 4, 100)).otherwise(col("cid")).alias("customer_id"),
    when(col("bdate") > current_date(), None).otherwise(col("bdate").cast("date")).alias("birthday"),
    when(upper(trim(col("gen"))).isin("F", "FEMALE"), "Female")
    .when(upper(trim(col("gen"))).isin("M", "MALE"), "Male")
    .otherwise("n/a").alias("gender")
)

df_demo.write.mode("overwrite").parquet("s3a://warehouse/silver/parquetFiles/erp/erp_customer_demographic")
df_demo.writeTo("local.erp_customer_demographic").createOrReplace()
# erp_categories

df_cat = spark.read.option("header", True).csv("s3a://warehouse/bronze/erp/PX_CAT_G1V2.csv")

df_cat = df_cat.select(
    col("id").alias("category_id"),
    col("cat").alias("category_name"),
    col("subcat").alias("subcategory_name"),
    when(upper(trim(col("maintenance"))) == "YES", 1).otherwise(0).alias("maintenance_flag")
)

df_cat.write.mode("overwrite").parquet("s3a://warehouse/silver/parquetFiles/erp/erp_categories")
df_cat.writeTo("local.erp_categories").createOrReplace()