from zipfile import ZipFile

def extract_zip(zip_file_path, destination_folder):
    with ZipFile('C:\\Users\\TXS1COB\\python\\landing\\leads-100.zip') as zip_obj:
    #Extract the contents to the specified folder
        zip_obj.extractall(path='C:\\Users\\TXS1COB\\python\\raw')

if __name__ == "__main__":
    zip_file_path = 'C:\\Users\\TXS1COB\\python\\landing\\leads-100.zip'
    destination_folder = 'C:\\Users\\TXS1COB\\python\\raw'
    extract_zip(zip_file_path, destination_folder)
