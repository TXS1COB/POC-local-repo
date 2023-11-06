import requests
import os
from azure.storage.blob import BlobServiceClient
import json

def download_zip(url, destination_container, connection_string):
    try:
        response = requests.get(url)
        filename = url.split("/")[-1]
        
        temp_file_path = os.path.join(local_dir, filename)
        with open(temp_file_path, 'wb') as file:
            file.write(response.content)
        print(f"File downloaded to {temp_file_path}")

        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client(destination_container)
        blob_client = container_client.get_blob_client(filename)

        with open(temp_file_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)

        print(f"File uploaded to Azure Blob Storage container: {destination_container}")
        return temp_file_path

    except requests.exceptions.RequestException as e:
        print(f"Error: {e}")
        return None

if __name__ == "__main__":
    url = "https://github.com/datablist/sample-csv-files/raw/main/files/leads/leads-100.zip"
    destination_container = "landing"  
    local_dir = r"C:\Users\TXS1COB\Documents\Python Scripts\POC_Cloud"
    with open("credentials.json", "r") as credentials_file:
        credentials = json.load(credentials_file)
    connection_string = credentials.get("AzureStorage", {}).get("ConnectionString", "")

    downloaded_file = download_zip(url, destination_container, connection_string)
    if downloaded_file:
        print("Download and upload completed")
    else:
        print("Download and upload failed")
