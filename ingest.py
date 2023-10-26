import requests
import os

def download_zip(url, destination_folder):
    # Function code...
    try:
        # Send an HTTP GET request to the specified URL to download the file.
        response = requests.get(url)

        # Extract the filename from the URL.
        filename = url.split("/")[-1]

        # Create the full path to the destination file in the specified folder.
        file_path = os.path.join(destination_folder, filename)

        # Save the downloaded content to the file.
        with open(file_path, 'wb') as file:
            file.write(response.content)

        print(f"File downloaded to {file_path}")
        return file_path
    except requests.exceptions.RequestException as e:
        # If there is an error during download, print an error message and return None.
        print(f"Error: {e}")
        return None

if __name__ == "__main__":
    url = "https://github.com/datablist/sample-csv-files/raw/main/files/leads/leads-100.zip"
    destination_folder = r"C:\Users\TXS1COB\python\landing"
    downloaded_file = download_zip(url, destination_folder)
    if downloaded_file:
        print("Download completed")
    else:
        print("Download failed")
