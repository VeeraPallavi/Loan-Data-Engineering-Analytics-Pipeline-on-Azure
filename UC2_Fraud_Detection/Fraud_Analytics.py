'''
Use Case 2: Fraud Detection in Loan Applications
Topic: Fraud Analytics
ETL Flow:
1.Event Hubs ingests streaming loan application events.
2.Databricks Structured Streaming applies anomaly detection ML model.
3.Suspicious applications flagged and written into dbo.FraudAlerts in Azure SQL.
4.Power BI Desktop dashboard highlights fraud alerts for investigation.
'''

#Configure ADLS Access using Shared Key
spark.conf.set(
"fs.azure.account.auth.type.datasets04.dfs.core.windows.net",
"SharedKey"
)

spark.conf.set(
"fs.azure.account.key.datasets04.dfs.core.windows.net",
"<Storage-account-key>"
)

# Initialize Spark session
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Fraud Analytics").getOrCreate()

#Configure Event Hub Connection
connectionString = "<Your-Connection-String>"
event_hub_conf = {
  "eventhubs.connectionString":
    sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connectionString),

  "eventhubs.startingPosition": """{
      "offset": "-1",
      "seqNo": -1,
      "enqueuedTime": null,
      "isInclusive": true
  }"""
}

#Configure Event Hub Connection
raw_stream = spark.readStream \
.format("eventhubs") \
.options(**event_hub_conf) \
.load()

#Convert Binary Data to String
json_df = raw_stream.selectExpr("CAST(body AS STRING)")

#Define Schema for JSON Data
from pyspark.sql.types import *
loan_schema = StructType() \
.add("Loan_ID", StringType()) \
.add("Applicant_Income", StringType()) \
.add("Loan_Amount", StringType()) \
.add("CIBIL_Score", StringType()) \
.add("Debt_to_Income_Ratio", StringType()) \
.add("Employment_Length_Years", StringType()) \
.add("Number_of_Previous_Loans", StringType()) \
.add("Default_History_Count", StringType()) \
.add("Property_Area", StringType())

# Parse JSON Data
from pyspark.sql.functions import col, from_json

loan_df = json_df.select(
    from_json(col("body"), loan_schema).alias("data")
).select("data.*")
# Converts JSON string into structured columns

#Clear Previous Checkpoint (Optional for Fresh Run)
dbutils.fs.rm(
"abfss://<Container>@<Storge-account-name>.dfs.core.windows.net/tmp/checkpoints/loan_data/",
True
)

# Display Streaming Data
display(
    loan_df,
    checkpointLocation="abfss://<Container>@<Storage-account-name>.dfs.core.windows.net/tmp/checkpoints/loan_data"
)

# Display Streaming Data
loan_df = loan_df.select(
    col("Loan_ID"),
    col("Applicant_Income").cast("int"),
    col("Loan_Amount").cast("int"),
    col("CIBIL_Score").cast("int"),
    col("Debt_to_Income_Ratio").cast("double")
)

#Fraud Detection Logic (Rule-Based)
from pyspark.sql.functions import when
fraud_df = loan_df.withColumn(
"FraudFlag",
when(
(col("Loan_Amount") > col("Applicant_Income") * 5) |
(col("CIBIL_Score") < 600) |
(col("Debt_to_Income_Ratio") > 0.7),
1
).otherwise(0)
)


# Final Fraud Classification
fraud_df = fraud_df.withColumn(
"Fraud_Flag",
when(
(col("CIBIL_Score") < 550) |
(col("Debt_to_Income_Ratio") > 2) |
(col("Applicant_Income") == 0),
"Fraud"
).otherwise("Normal")
)

# Remove Invalid Records
fraud_df = fraud_df.filter("Loan_ID IS NOT NULL")

# Clear Checkpoint for Fraud Output
dbutils.fs.rm(
"abfss://<Container>@<Storage-account-name>.dfs.core.windows.net/tmp/checkpoints/fraud_detection_result/",
True
)

#Display Fraud Results
display(
    fraud_df,
    checkpointLocation="abfss://<Container>@<Storage-account-name>.dfs.core.windows.net/tmp/checkpoints/fraud_detection_result"
)

# Configure Azure SQL Connection
jdbcHostname = "<server-name>"
jdbcDatabase = "<database-name>"
jdbcPort = 1433

jdbcUrl = f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};database={jdbcDatabase};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30"

connectionProperties = {
"user": "<username>",
"password": "<Password>",
"driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# Define Function to Write Streaming Data to SQL
def write_to_sql(batch_df, batch_id):

    batch_df.write \
    .mode("append") \
    .jdbc(
        url=jdbcUrl,
        table="dbo.FraudAlerts",
        properties=connectionProperties
    )

#Clear SQL Checkpoint
dbutils.fs.rm(
"abfss://<Container>@<Storage-account-name>.dfs.core.windows.net/tmp/checkpoints/fraud_sql/",
True
)

#Start Streaming Write to SQL
query = fraud_df.writeStream \
.foreachBatch(write_to_sql) \
.outputMode("append") \
.option(
"checkpointLocation",
"abfss://<Conatiner>@<Storage-ccount-name>.dfs.core.windows.net/tmp/checkpoints/fraud_sql"
) \
.start()