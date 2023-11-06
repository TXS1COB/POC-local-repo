import csv
import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime


input_csv_file = 'HAD_SensorLog_39519.csv'
output_metadata_json_file = 'sensor_metadata.json'
output_clean_csv_file = 'sensor_data.csv'
output_clean_parquet_file = 'sensor-data.parquet'  
metadata = {}

with open(input_csv_file, 'r', newline='') as csv_file:
    csv_reader = csv.reader(csv_file)

    for i in range(4):
        row = next(csv_reader)[0]  
        parts = row.split(':')
        if len(parts) == 2:
            key = parts[0].strip()
            value = parts[1].strip()
            metadata[key] = value

with open(output_metadata_json_file, 'w') as json_file:
    json.dump(metadata, json_file, indent=4)

with open(input_csv_file, 'r', newline='') as csv_file, open(output_clean_csv_file, 'w', newline='') as clean_csv_file:
    csv_reader = csv.reader(csv_file)
    csv_writer = csv.writer(clean_csv_file)
    for _ in range(5):
        next(csv_reader) 

    column_names = next(csv_reader)
    csv_writer.writerow(column_names)

    for row in csv_reader:
        csv_writer.writerow(row)

    df = pd.read_csv(output_clean_csv_file,low_memory=False)
    
    df['Updated timestamp'] = datetime.now()
    df['index'] = range(1, len(df) + 1)
    print(df.head(5))

with open(output_metadata_json_file, 'r') as metadata_file:
    metadata = json.load(metadata_file)
    
table = pa.Table.from_pandas(df)
table = table.replace_schema_metadata(metadata)
pq.write_table(table, output_clean_parquet_file)

table_read = pq.read_table(output_clean_parquet_file)
metadata_read = table_read.schema.metadata
print(metadata_read)
    

print("Clean CSV file created:", output_clean_csv_file)
print("Clean Parquet file created:", output_clean_parquet_file)
