import sys
sys.path.append('C:/Users/TobyBarker/Documents/Data_Engineering_Accelerator/src/databricks')
from pyspark.sql.functions import *
from datetime import datetime
from pyspark.sql.types import DoubleType
from utilities.spark_session import spark
from schemas.integrated.DimProduct import DimProduct_schema
from utilities.helpers.readers import lake_reader

#Variables
storage_account = "casestudylake"
container = "casestudy"
layer = "Cleansed"
source = "Product"
source2 = "ProductCategory"
domain = "Master" 
schemaVersion = 1
cleansed_date_format = "dd-MM-yyyy HH:mm:ss"  # when testing change the format to "dd/MM/yyyy HH:mm:ss"

def transform(df, df2):
    '''
    This function transforms the Product cleansed data based off the business requirements,
    preparing it for the Integrated layer

    Args:
        product_file_path (str)
        product_category_file_path (str)
        cleansed_date_format (str): the date format of the date columns in product_file_path
    '''    
    schema = DimProduct_schema
    df_dim_product = df
    df_product_category = df2
    # Convert date columns to yyyyMMdd format
    date_columns = ["ProductSellingStartDate", "ProductSellingEndDate", "ProductDiscontinuedDate"]
    for column in date_columns:
        df_dim_product = df_dim_product.withColumn(column, date_format(to_date(col(column), cleansed_date_format), "yyyyMMdd").cast("int"))

    df_dim_product = df_dim_product.withColumn("CurrencyKey", lit("GBP"))
    df_dim_product = df_dim_product.withColumn("ProductLineName", when(col("ProductLine") == "M", "Mountain Bikes")
                                                                .when(col("ProductLine") == "Null", "Bike Parts")
                                                                .when(col("ProductLine") == "R", "Road Bikes")
                                                                .when(col("ProductLine") == "S", "Accessories and Attire")
                                                                .when(col("ProductLine") == "T", "Touring Bikes")
                                                                .otherwise("Unknown"))
    df_dim_product = df_dim_product.withColumn("ProductKey", xxhash64(concat_ws("|", col("SourceProductID"), lit("Product"), lit("ProductCategory"))))
    df_dim_product = df_dim_product.join(df_product_category, on="ProductCategoryID", how="left")
    df_dim_product = df_dim_product.withColumnRenamed("ProductSubCategory", "ProductSubCategoryName")
    # Select only columns in the target schema and convert all columns to target data types
    df_dim_product = df_dim_product.select(schema.fieldNames())
    for col_name in df_dim_product.columns:
        data_type = df_dim_product.schema[col_name].dataType
        df_dim_product = df_dim_product.withColumn(col_name, col(col_name).cast(data_type))
        if data_type == DoubleType:
            df_dim_product = df_dim_product.withColumn(col_name, round(col(col_name), 2))
    return df_dim_product

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
    
    integrated_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/Integrated/Domain={domain}/Entity={source}/schemaVersion={schemaVersion}/SubmissionYear={submission_year}/SubmissionMonth={submission_month}/SubmissionDay={submission_day}/SubmissionHour={submission_hour}/SubmissionMinute={submission_minute}/SubmissionSecond={submission_second}"

    df.write.mode("overwrite").parquet(integrated_path)

def execute():
    product_df = lake_reader(spark, layer, source)
    product_category_df = lake_reader(spark, layer, source2)
    transformed_df = transform(product_df, product_category_df)
    write(transformed_df)