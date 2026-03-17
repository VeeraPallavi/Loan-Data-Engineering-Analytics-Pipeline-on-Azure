# Loan Data Engineering & Analytics Pipeline on Azure

## 📌 Project Overview

This project demonstrates a **modern data engineering pipeline built on Microsoft Azure** to process and analyze loan-related data.

The pipeline integrates:
- Batch Data Ingestion
- Real-time Streaming
- ETL Processing
- Machine Learning Analytics
- Dashboard Reporting

🎯 **Goal:** Transform raw loan data into actionable insights for:
- Credit Risk Analysis
- Fraud Detection
- Customer Sentiment Analysis

---


## 🔹 Step 1: Data Ingestion

### Batch Ingestion (ADF)
- Ingest historical loan datasets (CSV)
- Load into Data Lake

### Streaming Ingestion (Event Hubs)
- Capture real-time events:
  - Loan applications
  - Repayment updates
  - Customer interactions

---

## 🔹 Step 2: Data Storage

Data stored in **Azure Data Lake Storage Gen2** using layered architecture:

###  Raw Zone
- Original unprocessed data

```
raw/hdfc_loan_dataset.csv
```

###  Processed Zone
- Cleaned and normalized data

### Curated Zone
- Business-ready datasets

```
curated.loan_features
curated.customer_sentiment
curated.fraud_features
```

---

## 🔹 Step 3: Data Processing (ETL)

Performed using **Azure Databricks (Apache Spark)**

### ETL Tasks
- Data cleaning
- Normalization
- Feature engineering
- ML model execution
- Aggregation

---

##  Machine Learning Use Cases

### 1️⃣ Loan Default Risk Prediction
- Predict default probability

**Output Table:**
```
dbo.LoanRiskScores
```

---

### 2️⃣ Fraud Detection
- Detect suspicious applications using streaming data

**Output Table:**
```
dbo.FraudAlerts
```

---

### 3️⃣ Customer Sentiment Analysis
- NLP-based sentiment classification

**Output Table:**
```
dbo.CustomerSentiment
```

---

## 🔹 Step 4: Reporting Layer

### Database
- Azure SQL Database (Reporting Layer)

### Tables
```
dbo.LoanRiskScores
dbo.FraudAlerts
dbo.CustomerSentiment
```

---

## Visualization

Built using **Power BI Desktop**

### Dashboards
- Loan Risk Dashboard
- Fraud Monitoring Dashboard
- Customer Sentiment Dashboard

### Insights
- Risk distribution
- Fraud alerts
- Sentiment trends
- Loan approval analysis

---

##  Dataset

Sample dataset includes:
- Applicant income
- Credit score
- Loan amount
- Repayment history
- Customer feedback

---

## Technologies Used

| Category         | Technology                   |
|----------------|----------------------------|
| Cloud Platform  | Microsoft Azure            |
| Ingestion       | Azure Data Factory         |
| Streaming       | Azure Event Hubs           |
| Storage         | Azure Data Lake Gen2       |
| Processing      | Azure Databricks           |
| Database        | Azure SQL Database         |
| Visualization   | Power BI Desktop           |
| Language        | Python / PySpark           |

---

## Project Outcomes

- Scalable cloud data pipeline
- Real-time + batch processing
- Automated ETL workflows
- ML-driven insights
- Interactive dashboards

---

## 📌 Conclusion

This project showcases a complete **Azure-based data engineering pipeline** for financial analytics.

It enables:
- Better risk management
- Fraud prevention
- Improved customer experience
