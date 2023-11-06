import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime 

def cleanse_and_store_data():
# Read the CSV file into a DataFrame
    df = pd.read_csv('C:\\Users\\TXS1COB\\python\\myenv\\leads-100.csv')
    

# Add timestamp and full name columns
    df['timestamp'] = datetime.now()
    df['Name'] = df['First Name'] + ' ' + df['Last Name']

# Drop unnecessary columns
    df = df.drop(columns=["First Name", "Last Name"])

# Create a PyArrow Table from the DataFrame
    table = pa.Table.from_pandas(df)

# Create custom metadata
    metadata = {
        'source': 'Github',
        'description': 'Data leads for 2023',
        'author_id': 'TXS1COB',
}

# Add metadata to the PyArrow schema
    table = table.replace_schema_metadata(metadata)

# Write the PyArrow Table to a Parquet file
    pq.write_table(table, 'data_with_metadata.parquet')

# ----------------------------Optional! - Remove this ------------------------------
# Read the Parquet file and extract metadata
    table_read = pq.read_table('data_with_metadata.parquet')
    metadata_read = table_read.schema.metadata
    print(metadata_read)
    
if __name__ == "__main__":
    cleanse_and_store_data()


