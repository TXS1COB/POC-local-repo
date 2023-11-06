from prefect import task, flow
# Import functions from other python scripts
from ingest import download_zip
from extract import extract_zip
from cleanse import cleanse_and_store_data
from curate import validate_and_clean_data

# Define a Prefect task for ingesting data
@task
def ingest_task():
    # Specify the required URL of the zip file to download
    url = "https://github.com/datablist/sample-csv-files/raw/main/files/leads/leads-100.zip"
    destination_folder = r"C:\Users\TXS1COB\python\landing"
    downloaded_file = download_zip(url, destination_folder)
    return downloaded_file

# Define a Prefect task for extracting data from the downloaded zip file
@task
def extract_task(downloaded_file):
    zip_file_path = downloaded_file
    destination_folder = 'C:\\Users\\TXS1COB\\python\\raw'
    extract_zip(zip_file_path, destination_folder)

# Define a Prefect task for cleansing the data
@task
def cleanse_task():
    cleanse_and_store_data()

# Define a Prefect task for curating the data
@task
def curate_task():
    validate_and_clean_data()

# Define the main Prefect flow that orchestrates the tasks
@flow(name="Data Pipeline")
def main_flow():
    downloaded_file = ingest_task()
    extract_task(downloaded_file)
    cleanse_task()
    curate_task()    
    
if __name__ == "__main__":
    main_flow()
