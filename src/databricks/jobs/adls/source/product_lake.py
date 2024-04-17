import sys
sys.path.append('C:/Users/TobyBarker/Documents/Data_Engineering_Accelerator/src/databricks')
from pyspark.sql.functions import *
from datetime import datetime
from utilities.spark_session import spark
from schemas.cleansed.product import product_schema
from utilities.helpers.readers import lake_reader
from utilities.helpers.transform_helpers import apply_schema_to_df

#Variables
storage_account = "casestudylake"
container = "casestudy"
layer = "Sourced"
source = "Product"
schemaVersion = 1

def transform(df):
    '''
    This function transforms the Product source data based off the business requirements,
    preparing it for the Cleansed layer
    '''
    df = apply_schema_to_df(df, product_schema)
    # Convert necessary columns to StringType and DoubleType
    string_columns = [col_name for col_name, col_type in df.dtypes if col_type == "string"]
    for col_name in string_columns:
        df = df.withColumn(col_name, trim(col(col_name)))
    double_columns = [col_name for col_name, col_type in df.dtypes if col_type == "double"]
    for col_name in double_columns: 
        df = df.withColumn(col_name, round(col(col_name), 2)).fillna("None")
    df = df.withColumn("ProductWeightKilograms", when(col("ProductWeightKilograms") == 0, None).otherwise(col("ProductWeightKilograms")))
    df = df.withColumn("ProductWeightGrams", (col("ProductWeightKilograms")*1000).cast("int"))
    df = df.withColumn("ProductProfitAtListPrice", (col("ProductListPrice") - col("ProductCost")).cast("double"))
    df = df.withColumn("ProductMarginAtListPrice", (col("ProductProfitAtListPrice") / col("ProductCost")).cast("double"))
    df = df.withColumn("ProductProfitAtListPrice", round(col("ProductProfitAtListPrice"), 2))
    df = df.withColumn("ProductMarginAtListPrice", round(col("ProductMarginAtListPrice"), 2))
    for column in df.columns:
        df = df.withColumn(column, when(col(column).isNull(), None).otherwise(col(column)))
        df = df.withColumn(column, when(col(column) == lit("NULL"), None).otherwise(col(column)))
    return df


def write(df):
    '''This function creates the Cleansed file path, transforms the source file using the transform function 
    and writes it to the Cleansed folder in parquet format.

    Args:
        file_path (str)
        workspace_folder (str): the root directory you are working from
    '''
    # Retrieve folder names to write the data too

    current_datetime = datetime.now()
    submission_year = current_datetime.year
    submission_month = current_datetime.month
    submission_day = current_datetime.day
    submission_hour = current_datetime.hour
    submission_minute = current_datetime.minute
    submission_second = current_datetime.second

    cleansed_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/Cleansed/DataFeed={source}/schemaVersion={schemaVersion}/SubmissionYear={submission_year}/SubmissionMonth={submission_month}/SubmissionDay={submission_day}/SubmissionHour={submission_hour}/SubmissionMinute={submission_minute}/SubmissionSecond={submission_second}"
    
    df.write.mode("overwrite").parquet(cleansed_path)

def execute():
    input_df = lake_reader(spark, layer, source)
    transformed_df = transform(input_df)
    write(transformed_df)

execute()