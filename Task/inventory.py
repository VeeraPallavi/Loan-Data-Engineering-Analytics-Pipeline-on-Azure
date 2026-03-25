#Configure ADLS Storage Access using Shared Key
spark.conf.set(
"fs.azure.account.auth.type.datasets04.dfs.core.windows.net",
"SharedKey"
)

spark.conf.set(
"fs.azure.account.key.datasets04.dfs.core.windows.net",
"<storage-access-key>"
)


from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("Inventory").getOrCreate()


#Load Dataset from ADLS
df_inventory = spark.read.format("csv") \
.option("header","true") \
.option("inferSchema","true") \
.load("abfss://output@datasets04.dfs.core.windows.net/aws_inventory_logistics_raw.csv")

display(df_inventory)

#Clean and Tansform Data
df_inventory_clean = df_inventory.dropna()

display(df_inventory_clean)


from pyspark.sql.functions import col

df_alerts = df_inventory_clean.filter(
    col("stock_level") < col("reorder_level")
)

display(df_alerts)

df_alerts.printSchema()

df_cost = df_inventory_clean.groupBy("supplier") \
    .sum("transport_cost") \
    .withColumnRenamed("sum(transport_cost)", "total_cost")

display(df_cost)

# Configure Azure SQL Connection
jdbcHostname = "<server-name>"
jdbcDatabase = "<datbase-name>"
jdbcPort = 1433

jdbcUrl = f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};database={jdbcDatabase};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30"

connectionProperties = {
"user": "<username>",
"password": "<Password>",
"driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# Write Data to Azure SQL Table
df_alerts.write \
.mode("append") \
.jdbc(
    url=jdbcUrl,
    table="dbo.inventory",
    properties=connectionProperties
)