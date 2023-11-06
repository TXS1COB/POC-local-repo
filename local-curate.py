import pandas as pd
from datetime import datetime 
import pandera as pa

def validate_and_clean_data():
    # Read data from a CSV file into a pandas DataFrame
    df = pd.read_csv('C:\\Users\\TXS1COB\\python\\myenv\\leads-100.csv')
    df.head(5)
    df.describe()

#Using infer_schema from pandera to automatically analyze and prepare a regular schema for my data
    schema = pa.infer_schema(df)
    print(schema)
    df = pd.DataFrame(df)
    '''# Define a schema for validation using pandera
    schema = pa.DataFrameSchema({
        'Index': pa.Column(int),
        'Account Id': pa.Column(str),
        'Lead Owner': pa.Column(str),
        'Company': pa.Column(str),
        'Phone 1': pa.Column(str),
        'Phone 2': pa.Column(str),
        'Email': pa.Column(str),
        'Website': pa.Column(str),
        'Notes': pa.Column(str),
        'timestamp': pa.Column(pa.DateTime, nullable=True),
        'Name': pa.Column(str)
    })
    schema.validate(df)
    print(schema.to_yaml())
    '''
    # Define a customized schema to perform specific validations on data
    schema = pa.DataFrameSchema({
        'Index': pa.Column(pa.Int, checks=[pa.Check.in_range(1, 100)]),
        'Account Id': pa.Column(str, checks=[pa.Check.str_length(1, 15)]),
        'Lead Owner': pa.Column(str),
        'Company': pa.Column(str),
        'Phone 1': pa.Column(str, checks=[
        pa.Check(lambda s: len(s) == 12 and s.replace("-","").isdigit(), element_wise=True)
    ]),
        'Phone 2': pa.Column(str, checks=[
        pa.Check(lambda s: len(s) == 12 and s.replace("-","").isdigit(), element_wise=True)
    ]),
        'Email': pa.Column(str, checks=[pa.Check.str_matches(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,7}\b')]),
        'Website': pa.Column(str),
        'Notes': pa.Column(str),
        'timestamp': pa.Column(pa.DateTime),
        'Name': pa.Column(str, checks=[pa.Check(lambda s: not any(c.isdigit() for c in s))])
    })

# If validation fails, catch the SchemaError and raise an exception
    try:
        schema.validate(df)
    except pa.errors.SchemaError as e:
        failed_conditions = e.failure_cases
        failed_df = pd.DataFrame(failed_conditions)
        #raise Exception("Data not according to needs") from e
    print(schema.to_yaml())

    #Create a dataframe that have passed schema validation
    passed_df = df.drop(failed_df.index)
    passed_df    

if __name__ == "__main__":
    validate_and_clean_data()
