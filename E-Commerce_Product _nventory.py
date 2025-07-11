from pyspark.sql import SparkSession
from delta.tables import DeltaTable

# Step 1: Load Initial Dataset and Create Delta Table
spark = SparkSession.builder \
    .appName("E-Commerce Inventory") \
    .getOrCreate()

initial_data = [
    ("P001", "Laptop", "Electronics", 999.99),
    ("P002", "Smartphone", "Electronics", 499.99),
    ("P003", "T-Shirt", "Clothing", 29.99)
]

columns = ["product_id", "name", "category", "price"]
initial_df = spark.createDataFrame(initial_data, columns)

initial_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("default.products")

# Step 2: Perform Upsert with Schema Evolution
update_data = [
    ("P001", "Laptop", "Electronics", 1099.99, 50.0),
    ("P004", "Headphones", "Electronics", 149.99, 10.0),
    ("P005", "Jeans", "Clothing", 59.99, 5.0)
]

update_columns = ["product_id", "name", "category", "price", "discount"]
update_df = spark.createDataFrame(update_data, update_columns)

update_df.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("default.products_temp")

delta_table = DeltaTable.forName(spark, "default.products")
update_table = DeltaTable.forName(spark, "default.products_temp")

delta_table.alias("target") \
    .merge(
        update_df.alias("source"),
        "target.product_id = source.product_id"
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

# Step 3: Time Travel to Audit Schema Change
spark.sql("DESCRIBE HISTORY default.products").show(truncate=False)

df_before = spark.read.format("delta") \
    .option("versionAsOf", 0) \
    .table("default.products")
df_before.show()

df_after = spark.read.format("delta") \
    .option("versionAsOf", 1) \
    .table("default.products")
df_after.show()

# Step 4: Revert to Previous Version (Optional Rollback)
df_before.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("default.products")

# Step 5: Historical Price Analysis
history_df = spark.sql("DESCRIBE HISTORY default.products")
history_df.select("version", "timestamp", "operation", "operationParameters").show(truncate=False)

for version in range(0, 2):
    print(f"--- Version {version} ---")
    df = spark.read.format("delta") \
        .option("versionAsOf", version) \
        .table("default.products") \
        .filter("product_id = 'P001'")
    df.show()
