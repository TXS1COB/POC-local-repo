import json
import io
from azure.storage.blob import BlobServiceClient
from zipfile import ZipFile

def extract_and_upload_zip(connection_string, source_container_name, source_blob_name, destination_container_name):   
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    source_container_client = blob_service_client.get_container_client(source_container_name)
    source_blob_client = source_container_client.get_blob_client(source_blob_name)

    zip_content = source_blob_client.download_blob()
    zip_data = zip_content.readall()

    with ZipFile(io.BytesIO(zip_data)) as zip_obj:
        for file_info in zip_obj.infolist():
            destination_container_client = blob_service_client.get_container_client(destination_container_name)
            destination_blob_client = destination_container_client.get_blob_client(file_info.filename)
            destination_blob_client.upload_blob(zip_obj.read(file_info.filename), overwrite=True)

if __name__ == "__main__":
    with open("credentials.json", "r") as credentials_file:
        credentials = json.load(credentials_file)
    connection_string = credentials.get("AzureStorage", {}).get("ConnectionString", "")

    source_container_name = "landing"  
    source_blob_name = "leads-100.zip"  
    destination_container_name = "raw"  
    extract_and_upload_zip(connection_string, source_container_name, source_blob_name, destination_container_name)
