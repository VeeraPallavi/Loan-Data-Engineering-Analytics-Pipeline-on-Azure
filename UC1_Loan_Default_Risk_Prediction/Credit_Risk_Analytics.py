'''
Use Case 1: Loan Default Risk Prediction
Topic: Credit Risk Analytics
ETL Flow:
1.ADF ingests historical repayment data into Data Lake.
2.Databricks cleans data, engineers features (income, credit score, repayment history).
3.ML model predicts default probability.
4.Results stored in dbo.LoanRiskScores in Azure SQL.
5.Power BI Desktop dashboard shows risk distribution across customer segments.
'''

#Configure ADLS Access using OAuth (Service Principal)
# ADLS Storage Account Name
storage_account_name = "<Storage-account-name>"
container_name = "<Container>"

# OAuth 2.0 Endpoint
oauth_endpoint = f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"

# Set Spark Config for ADLS Gen2 OAuth
spark.conf.set(
f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net",
"OAuth"
)

spark.conf.set(
f"fs.azure.account.oauth.provider.type.{storage_account_name}.dfs.core.windows.net",
"org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
)

spark.conf.set(
f"fs.azure.account.oauth2.client.id.{storage_account_name}.dfs.core.windows.net",
client_id
)

spark.conf.set(
f"fs.azure.account.oauth2.client.secret.{storage_account_name}.dfs.core.windows.net",
client_secret
)

spark.conf.set(
f"fs.azure.account.oauth2.client.endpoint.{storage_account_name}.dfs.core.windows.net",
oauth_endpoint
)

print("Connection Configured Successfully")

# Initialize Spark session
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("LoanDefaultRiskPrediction").getOrCreate()

spark.conf.set("fs.azure.account.key.datasets04.blob.core.windows.net","<Storage-account-key>"
)

#Define ADLS Path
adls_path = "wasbs://input@datasets04.blob.core.windows.net/"

# Load Dataset
df = spark.read.format("csv") \
.option("header","true") \
.option("inferSchema","true") \
.load(adls_path+"Processed_Data/hdfc_loan_dataset.csv") \

display(df)

# Check Schema
df.printSchema()


# Select Relevant Features for ML Model
loan_df = df.select(
"Applicant_Income",
"Coapplicant_Income",
"Loan_Amount",
"Loan_Term_Months",
"Credit_History",
"CIBIL_Score",
"Debt_to_Income_Ratio",
"Existing_EMIs",
"Number_of_Previous_Loans",
"Default_History_Count",
"Employment_Length_Years",
"Loan_Status"
)

display(loan_df)

# Data Cleaning
#Removes NULL values
loan_df = loan_df.dropna()

from pyspark.sql.functions import when

# Converts Loan_Status into binary label
loan_df = loan_df.withColumn(
"label",
when(loan_df.Loan_Status=="Rejected",1).otherwise(0)
)

# Drop original column 
loan_df = loan_df.drop("Loan_Status")

# Feature Engineering using VectorAssembler
from pyspark.ml.feature import VectorAssembler
features = [
"Applicant_Income",
"Coapplicant_Income",
"Loan_Amount",
"Loan_Term_Months",
"Credit_History",
"CIBIL_Score",
"Debt_to_Income_Ratio",
"Existing_EMIs",
"Number_of_Previous_Loans",
"Default_History_Count",
"Employment_Length_Years"
]

assembler = VectorAssembler(
inputCols=features,
outputCol="features")

final_df = assembler.transform(loan_df)

# Train-Test Split
train_df, test_df = final_df.randomSplit([0.8,0.2], seed=42)


# Train Machine Learning Model
from pyspark.ml.classification import LogisticRegression
lr = LogisticRegression(
featuresCol="features",
labelCol="label")

# Trains Logistic Regression model
model = lr.fit(train_df)

# Make Predictions
predictions = model.transform(test_df)

display(predictions.select(
"features",
"label",
"prediction",
"probability"
))


from pyspark.sql.functions import col
from pyspark.ml.functions import vector_to_array

# Extract Risk Output
risk_df = predictions.select(
    col("Applicant_Income"),
    col("Loan_Amount"),
    col("CIBIL_Score"),
    col("prediction").alias("Risk_Flag"),
    vector_to_array(col("probability"))[1].alias("Default_Probability")
)

display(risk_df)

# Configure Azure SQL Connection
server = '<sever-name>'
database = '<database-name'
username = '<username>'
password = '<Password>'

jdbc_url = f"jdbc:sqlserver://{server}:1433;database={database}"

connection_properties = {
"user": username,
"password": password,
"driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# Write Results to Azure SQL
risk_df.write.jdbc(
url=jdbc_url,
table="dbo.LoanRiskScores",
mode="overwrite",
properties=connection_properties
)