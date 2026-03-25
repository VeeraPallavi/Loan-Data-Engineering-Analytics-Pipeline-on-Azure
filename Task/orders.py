#Configure ADLS Storage Access using Shared Key
spark.conf.set(
"fs.azure.account.auth.type.datasets04.dfs.core.windows.net",
"SharedKey"
)

spark.conf.set(
"fs.azure.account.key.datasets04.dfs.core.windows.net",
"<shared-access-key>"
)


from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("Supply Chain Orders").getOrCreate()


#Load Dataset from ADLS
df = spark.read.format("csv") \
.option("header","true") \
.option("inferSchema","true") \
.load("abfss://output@datasets04.dfs.core.windows.net/aws_supply_chain_orders_raw.csv")

display(df)

#Clean and Transform Data
df_clean = df.dropna()

from pyspark.sql.functions import to_date
df_clean = df_clean.withColumn("order_date", to_date("order_date"))


display(df_clean)


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
df_clean.write \
.mode("append") \
.jdbc(
    url=jdbcUrl,
    table="dbo.orders",
    properties=connectionProperties
)


df_clean.printSchema()