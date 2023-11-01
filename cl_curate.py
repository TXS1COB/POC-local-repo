import pandas as pd
import pandera as pa
import pyarrow.parquet as pq
from azure.storage.blob import BlobServiceClient
import json

def validate_and_clean_data():
    with open('credentials.json', 'r') as credentials_file:
        credentials = json.load(credentials_file)    
    connection_string = credentials.get("AzureStorage", {}).get("ConnectionString", "")
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    source_container_name = "cleanse"
    destination_container_name = "curated"
    parquet_file_name = "data_with_metadata.parquet"

    source_container_client = blob_service_client.get_container_client(source_container_name)
    destination_container_client = blob_service_client.get_container_client(destination_container_name)

    blob_client = source_container_client.get_blob_client(parquet_file_name)
    blob_data = blob_client.download_blob()
    df = pd.read_parquet(blob_data.readall())
    

    # Define a customized schema for validation using pandera
    schema = pa.DataFrameSchema({
        'Index': pa.Column(pa.Int, checks=[pa.Check.in_range(1, 100)]),
        'Account Id': pa.Column(str, checks=[pa.Check.str_length(1, 15)]),
        'Lead Owner': pa.Column(str),
        'Company': pa.Column(str),
        'Phone 1': pa.Column(str, checks=[
            pa.Check(lambda s: len(s) == 12 and s.replace("-", "").isdigit(), element_wise=True)
        ]),
        'Phone 2': pa.Column(str, checks=[
            pa.Check(lambda s: len(s) == 12 and s.replace("-", "").isdigit(), element_wise=True)
        ]),
        'Email': pa.Column(str, checks=[pa.Check.str_matches(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,7}\b')]),
        'Website': pa.Column(str),
        'Notes': pa.Column(str),
        'timestamp': pa.Column(pa.DateTime),
        'Name': pa.Column(str, checks=[pa.Check(lambda s: not any(c.isdigit() for c in s))])
    })

    schema.validate(df)
    try:
        schema.validate(df)
    except pa.errors.SchemaError as e:
        failed_conditions = e.failure_cases
        failed_df = pd.DataFrame(failed_conditions)
        #raise Exception("Data not according to needs") from e

    passed_df = df.drop(failed_df.index)
    
    destination_blob_name = "curated_data.parquet"
    table = pa.Table.from_pandas(passed_df)
    pq.write_table(table, destination_blob_name)
    #table_read = pq.read_table('curated_data.parquet')
    #print(table_read)
    
    destination_blob_client = destination_container_client.get_blob_client(destination_blob_name)
    with destination_blob_client.upload_blob(data=passed_df.to_parquet(), overwrite=True) as blob:
        pass

    passed_df.to_parquet("local_copy.parquet")
if __name__ == "__main":
    validate_and_clean_data()


