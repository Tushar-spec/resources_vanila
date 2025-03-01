import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import csv
import io
import pandas as pd
from google.cloud import storage, bigquery

# GCP Configurations
PROJECT_ID = "vanilla-steel-task-2"  # Replace with your actual GCP Project ID in my case vanilla-stell-task-2
BUCKET_NAME = "vanila_steel_task_2"
DATASET_ID = "vanila_steel_dataset_1"  # dataset name on bigquery
TABLE_ID = "recommendations"

# GCS file paths
BUYER_PREFERENCES_FILE = f"gs://{BUCKET_NAME}/resources/task_3/buyer_preferences.csv"
SUPPLIER_DATA1_FILE = f"gs://{BUCKET_NAME}/resources/task_3/supplier_data1.csv"
SUPPLIER_DATA2_FILE = f"gs://{BUCKET_NAME}/resources/task_3/supplier_data2.csv"

class ReadCSVFile(beam.DoFn):
    """this Reads CSV file from GCS and returns a list of dictionaries."""
    def __init__(self, file_path):
        self.file_path = file_path

    def process(self, element):
        client = storage.Client()
        bucket = client.bucket(BUCKET_NAME)
        blob = bucket.blob(self.file_path.split("/")[-1])  # Extracts the filename
        content = blob.download_as_text()

        # Reads CSV into a list of dictionaries 
        reader = csv.DictReader(io.StringIO(content))
        return [row for row in reader]

class MatchSupplierWithBuyer(beam.DoFn):
    """Matches supplier materials with buyer preferences."""
    def process(self, element):
        buyer_data, supplier_data = element

        # Converting list of dicts to DataFrames of bothh buyer and supplier
        buyer_df = pd.DataFrame(buyer_data)
        supplier_df = pd.DataFrame(supplier_data)

        # Standardize column names, because anyspace or lowercase or uppercase can create problem
        buyer_df.columns = buyer_df.columns.str.lower().str.replace(" ", "_")
        supplier_df.columns = supplier_df.columns.str.lower().str.replace(" ", "_")

        # Merge supplier datasets to find recommendations
        merged_df = buyer_df.merge(supplier_df, on='material_type', how='inner')

        # Generate recommendation table
        recommendations = merged_df[["buyer_id", "supplier_id", "material_type", "price", "availability"]]

        return recommendations.to_dict(orient="records")

class WriteToBigQuery(beam.DoFn):
    """Write the output data to BigQuery ."""
    def process(self, element):
        client = bigquery.Client()
        table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

        job = client.insert_rows_json(table_ref, [element])
        if job:
            print(f"Error inserting row: {job}")
        return

def run():
    """Run the Apache Beam pipeline."""
    options = PipelineOptions(
        runner="DataflowRunner",  
        project=PROJECT_ID,
        temp_location=f"gs://{BUCKET_NAME}/resources/task_3/temp",
        region="us-central1",
        staging_location=f"gs://{BUCKET_NAME}/resources/task_3/staging"
    )

    with beam.Pipeline(options=options) as p:
        buyer_prefs = p | "Read Buyer Preferences" >> beam.ParDo(ReadCSVFile(BUYER_PREFERENCES_FILE))
        supplier_data1 = p | "Read Supplier Data 1" >> beam.ParDo(ReadCSVFile(SUPPLIER_DATA1_FILE))
        supplier_data2 = p | "Read Supplier Data 2" >> beam.ParDo(ReadCSVFile(SUPPLIER_DATA2_FILE))

        supplier_data = (supplier_data1, supplier_data2) | beam.Flatten()

        recommendations = (
            (buyer_prefs, supplier_data)
            | "Match Suppliers with Buyers" >> beam.ParDo(MatchSupplierWithBuyer())
        )

        recommendations | "Write to BigQuery" >> beam.ParDo(WriteToBigQuery())

if __name__ == "__main__":
    run()

