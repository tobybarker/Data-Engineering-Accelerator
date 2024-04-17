import sys
sys.path.append('C:/Users/TobyBarker/Documents/Data_Engineering_Accelerator/src/databricks')
from pyspark.sql.functions import *
from datetime import datetime
from utilities.spark_session import spark
from schemas.integrated.FactSales import FactSales_schema
from utilities.helpers.readers import read_parquet_to_df

#Variables
file_path = "C:/Users/TobyBarker/Documents/Data_Engineering_Accelerator/src/databricks/sample_lake/Cleansed/DataFeed=Sales/schemaVersion=1/SubmissionYear=2023/SubmissionMonth=10/SubmissionDay=30"
workspace_folder = "C:/Users/TobyBarker/Documents/Data_Engineering_Accelerator/"
entity = "Sales"
domain = "Commercial" 
schemaVersion = 1
cleansed_date_format = "dd/MM/yyyy HH:mm:ss"

def transform(df):
    '''
    This function transforms the Product cleansed data based off the business requirements,
    preparing it for the Integrated layer

    Args:
        df
    '''   
    schema = FactSales_schema
    df_fact_sales = df
    # Convert date columns to yyyyMMdd format
    date_columns = ["SalesOrderDate"]
    for column in date_columns:
        df_fact_sales = df_fact_sales.withColumn(column, date_format(to_date(col(column), cleansed_date_format), "yyyyMMdd").cast("int"))

    df_fact_sales = df_fact_sales.withColumn("FactHashID", xxhash64(concat_ws("|", *[col(column) for column in df_fact_sales.columns])))
    df_fact_sales = df_fact_sales.withColumn("CurrencyKey", lit("GBP"))
    df_fact_sales = df_fact_sales.withColumn("ProductKey", xxhash64(concat_ws("|", col("ProductID"), lit("Product"), lit("ProductCategory"))))
    df_fact_sales = df_fact_sales.withColumn("SpecialOfferKey", xxhash64(concat_ws("|", col("SpecialOfferID"), lit("Sales"))))
    df_fact_sales = df_fact_sales.withColumnRenamed("TotalUnits", "UnitVolume").withColumnRenamed("TotalLineValue", "TotalSalesLineValue")
    # Select only columns in the target schema and convert all columns to target data types
    df_fact_sales = df_fact_sales.select(schema.fieldNames())
    for col_name in df_fact_sales.columns:
        data_type = df_fact_sales.schema[col_name].dataType
        df_fact_sales = df_fact_sales.withColumn(col_name, col(col_name).cast(data_type))

    return df_fact_sales

def write(df):
    '''This function creates the Integrated file path, transforms the cleansed file and writes
    it to the Integrated folder in parquet format. 
    '''
    # Retrieve folder names to write the data too
    current_datetime = datetime.now()
    submission_year = current_datetime.year
    submission_month = current_datetime.month
    submission_day = current_datetime.day
    submission_hour = current_datetime.hour
    submission_minute = current_datetime.minute
    submission_second = current_datetime.second 
    
    integrated_path = f"{workspace_folder}/src/databricks/sample_lake/Integrated/Domain={domain}/Entity={entity}/schemaVersion={schemaVersion}/SubmissionYear={submission_year}/SubmissionMonth={submission_month}/SubmissionDay={submission_day}/SubmissionHour={submission_hour}/SubmissionMinute={submission_minute}/SubmissionSecond={submission_second}"

    df = df.withColumn("SalesOrderYear", substring("SalesOrderDate", 1, 4)).withColumn("SalesOrderMonth", substring("SalesOrderDate", 5, 2))
    df.write.partitionBy("SalesOrderYear", "SalesOrderMonth").mode("overwrite").parquet(integrated_path)

def execute():
    input_df = read_parquet_to_df(spark, file_path)
    transformed_df = transform(input_df)
    write(transformed_df)