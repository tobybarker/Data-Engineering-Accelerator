import sys
sys.path.append('C:/Users/TobyBarker/Documents/Data_Engineering_Accelerator/src/databricks')
from pyspark.sql.functions import *
from datetime import datetime
from utilities.spark_session import spark
from schemas.cleansed.product_category import product_category_schema
from utilities.helpers.readers import read_csv_to_df
from utilities.helpers.transform_helpers import apply_schema_to_df

#Variables
file_path = "C:/Users/TobyBarker/Documents/Data_Engineering_Accelerator/src/databricks/sample_lake/Sourced/SystemA/ProductCategory/date=20220606/ProductCategory.csv"
workspace_folder = "C:/Users/TobyBarker/Documents/Data_Engineering_Accelerator/"
schemaVersion = 1
data_feed = "ProductCategory"

def transform(df):
    '''
    This function transforms the ProductCategory source data based off the business requirements,
    preparing it for the Cleansed layer
    '''
    df = apply_schema_to_df(df, product_category_schema)
    string_columns = [col_name for col_name, col_type in df.dtypes if col_type == "string"]
    for col_name in string_columns:
        df = df.withColumn(col_name, regexp_replace(col(col_name), "\t", ""))
        df = df.withColumn(col_name, trim(initcap(col(col_name))))
    return df

def write(df):
    '''This function creates the Cleansed file path, transforms the source file using the transform function 
    and writes it to the Cleansed folder in parquet format.

    Args:
        df
    '''
    # Retrieve folder names to write the data too

    current_datetime = datetime.now()
    submission_year = current_datetime.year
    submission_month = current_datetime.month
    submission_day = current_datetime.day
    submission_hour = current_datetime.hour
    submission_minute = current_datetime.minute
    submission_second = current_datetime.second

    cleansed_path = f"{workspace_folder}/src/databricks/sample_lake/Cleansed/DataFeed={data_feed}/schemaVersion={schemaVersion}/SubmissionYear={submission_year}/SubmissionMonth={submission_month}/SubmissionDay={submission_day}/SubmissionHour={submission_hour}/SubmissionMinute={submission_minute}/SubmissionSecond={submission_second}"
    
    df.write.mode("overwrite").parquet(cleansed_path)

def execute():
    input_df = read_csv_to_df(spark, file_path)
    transformed_df = transform(input_df)
    write(transformed_df)