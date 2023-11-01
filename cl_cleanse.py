import json
import io
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from azure.storage.blob import BlobServiceClient
from datetime import datetime

def cleanse_and_store_data(connection_string, source_container_name, source_blob_name, destination_container_name):
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    source_container_client = blob_service_client.get_container_client(source_container_name)
    source_blob_client = source_container_client.get_blob_client(source_blob_name)

    csv_content = source_blob_client.download_blob()
    csv_data = csv_content.content_as_text()
    df = pd.read_csv(io.StringIO(csv_data))


    df['timestamp'] = datetime.now()
    df['Name'] = df['First Name'] + ' ' + df['Last Name']
    df = df.drop(columns=["First Name", "Last Name"])

    table = pa.Table.from_pandas(df)
    metadata = {
        'source': 'Azure Blob Storage',
        'description': 'Cleansed data for 2023',
        'author_id': 'TXS1COB',
    }

    table = table.replace_schema_metadata(metadata)
    pq.write_table(table, 'data_with_metadata.parquet')

    destination_container_client = blob_service_client.get_container_client(destination_container_name)
    destination_blob_client = destination_container_client.get_blob_client('data_with_metadata.parquet')
    with open('data_with_metadata.parquet', 'rb') as data:
        destination_blob_client.upload_blob(data, overwrite=True)

if __name__ == "__main__":
    with open("credentials.json", "r") as credentials_file:
        credentials = json.load(credentials_file)
    connection_string = credentials.get("AzureStorage", {}).get("ConnectionString", "")

    source_container_name = "raw"  
    source_blob_name = "leads-100.csv" 
    destination_container_name = "cleanse"  

    cleanse_and_store_data(connection_string, source_container_name, source_blob_name, destination_container_name)
