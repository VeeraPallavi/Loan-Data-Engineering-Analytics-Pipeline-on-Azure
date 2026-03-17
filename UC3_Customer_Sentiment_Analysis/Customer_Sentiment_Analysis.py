'''
Use Case 3: Customer Sentiment Analysis
Topic: Customer Experience Analytics
ETL Flow:
1.ADF ingests customer feedback files into Data Lake.
2.Databricks applies NLP sentiment analysis model.
3.Results stored in dbo.CustomerSentiment in Azure SQL.
4.Power BI Desktop dashboard shows sentiment trends over time and by product type.
'''

#Configure ADLS Storage Access using Shared Key
spark.conf.set(
"fs.azure.account.auth.type.datasets04.dfs.core.windows.net",
"SharedKey"
)

spark.conf.set(
"fs.azure.account.key.datasets04.dfs.core.windows.net",
"<STORAGE-KEY>"
)

# Initialize Spark session
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Customer Sentiment Analysis").getOrCreate()

#Load Dataset from ADLS
feedback= spark.read.format("csv") \
.option("header","true") \
.option("inferSchema","true") \
.load("abfss://<container>@Storage-account.dfs.core.windows.net/Processed_Data/hdfc_loan_dataset.csv")

# Displays the raw dataset
display(feedback)

#Select Required Columns for Sentiment Analysis
feedback_df = feedback.select(
    "Loan_ID",
    "Customer_Feedback",
    "Purpose_of_Loan",
    "Loan_Status"
)

# Extract only relevant columns for analysis
display(feedback_df)

#Install NLP Library
!pip install textblob
# Installs TextBlob library for sentiment analysis

# Import Required Libraries
from textblob import TextBlob
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Define Sentiment Function
def sentiment(text):

    if text is None:
        return "Neutral"

    polarity = TextBlob(text).sentiment.polarity

    if polarity > 0:
        return "Positive"
    elif polarity < 0:
        return "Negative"
    else:
        return "Neutral"

# Convert Function to Spark UDF
sentiment_udf = udf(sentiment, StringType())

# Apply Sentiment Analysis
sentiment_df = feedback_df.withColumn(
    "Predicted_Sentiment",
    sentiment_udf(feedback_df.Customer_Feedback)
)

# Adds new column with predicted sentiment
display(sentiment_df)

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
sentiment_df.write \
.mode("append") \
.jdbc(
    url=jdbcUrl,
    table="dbo.CustomerSentiment",
    properties=connectionProperties
)

