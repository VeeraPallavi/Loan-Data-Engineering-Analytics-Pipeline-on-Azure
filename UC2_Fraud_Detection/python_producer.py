import pandas as pd
import json
import time
from azure.eventhub import EventHubProducerClient, EventData

# Event Hub connection
connection_str = "<your-Connection-String>"
eventhub_name = "<eventhub-name>"

producer = EventHubProducerClient.from_connection_string(
    conn_str=connection_str,
    eventhub_name=eventhub_name
)

# Load dataset
df = pd.read_csv("hdfc_loan_dataset.csv")

for index, row in df.iterrows():

    event_data = {
        "LoanID": str(row["Loan_ID"]),
        "ApplicantIncome": float(row["Applicant_Income"]),
        "LoanAmount": float(row["Loan_Amount"]),
        "CIBILScore": int(row["CIBIL_Score"]),
        "DebtToIncomeRatio": float(row["Debt_to_Income_Ratio"]),
        "EmploymentYears": int(row["Employment_Length_Years"]),
        "PreviousLoans": int(row["Number_of_Previous_Loans"]),
        "DefaultHistory": int(row["Default_History_Count"]),
        "PropertyArea": str(row["Property_Area"])
    }

    batch = producer.create_batch()
    batch.add(EventData(json.dumps(event_data)))

    producer.send_batch(batch)

    print("Sent:", event_data)

    time.sleep(2)   # simulate real-time streaming

producer.close()