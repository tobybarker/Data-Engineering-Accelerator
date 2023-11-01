from pyspark.sql.functions import *
from pyspark.sql.types import DoubleType
from pyspark.sql import SparkSession
from schemas.integrated.DimProduct import DimProduct_schema
spark = SparkSession.builder.appName("DataEngineerAccelerator").getOrCreate()

def read_parquet_to_df(product_file_path):
    '''Reads parquet file to a dataframe
    Args:
        product_file_path (string)
    '''
    df = spark.read.parquet(product_file_path, header="True")
    return df

def transform(product_file_path, product_category_file_path, cleansed_date_format):
    '''
    This function transforms the Product cleansed data based off the business requirements,
    preparing it for the Integrated layer

    Args:
        product_file_path (str)
        product_category_file_path (str)
        cleansed_date_format (str): the date format of the date columns in product_file_path
    '''    
    schema = DimProduct_schema
    df = read_parquet_to_df(product_file_path)
    df_product_category = read_parquet_to_df(product_category_file_path)
    df_dim_product = df
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
    df_dim_product = df_dim_product.select(schema.fieldNames())
    for col_name in df_dim_product.columns:
        data_type = df_dim_product.schema[col_name].dataType
        df_dim_product = df_dim_product.withColumn(col_name, col(col_name).cast(data_type))
        if data_type == DoubleType:
            df_dim_product = df_dim_product.withColumn(col_name, round(col(col_name), 2))
    return df_dim_product

def execute(product_file_path, product_category_file_path, cleansed_date_format, workspace_folder):
    '''This function creates the Integrated file path, transforms the cleansed file and writes
    it to the Integrated folder in parquet format. 
    '''
    current_date_df = spark.range(1).select(to_timestamp(current_timestamp()).alias("current_date"))
    year_df = current_date_df.select(year("current_date").alias("submission_year"))
    month_df = current_date_df.select(month("current_date").alias("submission_month"))
    day_df = current_date_df.select(day("current_date").alias("submission_day"))
    hour_df = current_date_df.select(hour("current_date").alias("submission_hour"))
    minute_df = current_date_df.select(minute("current_date").alias("submission_minute"))
    second_df = current_date_df.select(second("current_date").alias("submission_second"))

    submission_year = year_df.first()["submission_year"]
    submission_month = month_df.first()["submission_month"]
    submission_day = day_df.first()["submission_day"]
    submission_hour = hour_df.first()["submission_hour"]
    submission_minute = minute_df.first()["submission_minute"]
    submission_second = second_df.first()["submission_second"]

    df = transform(product_file_path, product_category_file_path, cleansed_date_format)

    entity = "Product"
    domain = "Master"  
    
    integrated_path = f"{workspace_folder}/src/databricks/sample_lake/Integrated/Domain={domain}/Entity={entity}/schemaVersion=1/SubmissionYear={submission_year}/SubmissionMonth={submission_month}/SubmissionDay={submission_day}/SubmissionHour={submission_hour}/SubmissionMinute={submission_minute}/SubmissionSecond={submission_second}"

    df.write.mode("overwrite").parquet(integrated_path)



def transform_test(df_product, df_product_category, cleansed_date_format):
    '''This function is the same as the transform function but adjusted to parse a df instead of file_path
    as that is the input for the unit tests
    '''   
    schema = DimProduct_schema
    df_dim_product = df_product
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
    df_dim_product = df_dim_product.select(schema.fieldNames())
    for col_name in df_dim_product.columns:
        data_type = df_dim_product.schema[col_name].dataType
        df_dim_product = df_dim_product.withColumn(col_name, col(col_name).cast(data_type))
        if data_type == DoubleType:
            df_dim_product = df_dim_product.withColumn(col_name, round(col(col_name), 2))
    return df_dim_product