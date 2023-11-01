from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from schemas.integrated.FactSales import FactSales_schema
spark = SparkSession.builder.appName("DataEngineerAccelerator").getOrCreate()

def read_parquet_to_df(file_path):
    '''
    This function reads the source file csv to a dataframe.

    Args:
        file_path (string): Determines which cleansed file to be read to a dataframe.
    '''
    df = spark.read.parquet(file_path, header="True")
    return df

def transform(file_path, cleansed_date_format):
    '''
    This function transforms the Sales cleansed data based off the business requirements,
    preparing it for the Cleansedlayer
    '''
    schema = FactSales_schema
    df = read_parquet_to_df(file_path)
    df_fact_sales = df
    date_columns = ["SalesOrderDate"]
    for column in date_columns:
        df_fact_sales = df_fact_sales.withColumn(column, date_format(to_date(col(column), cleansed_date_format), "yyyyMMdd").cast("int"))

    df_fact_sales = df_fact_sales.withColumn("FactHashID", xxhash64(concat_ws("|", *[col(column) for column in df_fact_sales.columns])))
    df_fact_sales = df_fact_sales.withColumn("CurrencyKey", lit("GBP"))
    df_fact_sales = df_fact_sales.withColumn("ProductKey", xxhash64(concat_ws("|", col("ProductID"), lit("Product"), lit("ProductCategory"))))
    df_fact_sales = df_fact_sales.withColumn("SpecialOfferKey", xxhash64(concat_ws("|", col("SpecialOfferID"), lit("Sales"))))
    df_fact_sales = df_fact_sales.withColumnRenamed("TotalUnits", "UnitVolume").withColumnRenamed("TotalLineValue", "TotalSalesLineValue")
    df_fact_sales = df_fact_sales.select(schema.fieldNames())
    for col_name in df_fact_sales.columns:
        data_type = df_fact_sales.schema[col_name].dataType
        df_fact_sales = df_fact_sales.withColumn(col_name, col(col_name).cast(data_type))

    return df_fact_sales


def execute(file_path, cleansed_date_format, workspace_folder):
    '''
    This function creates the Integrated file path, transforms the cleansed file and writes it to the Integrated folder in parquet format. 
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

    df = transform(file_path, cleansed_date_format)

    entity = "Sales"
    domain = "Commercial"   
    
    integrated_path = f"{workspace_folder}/src/databricks/sample_lake/Integrated/Domain={domain}/Entity={entity}/schemaVersion=1/SubmissionYear={submission_year}/SubmissionMonth={submission_month}/SubmissionDay={submission_day}/SubmissionHour={submission_hour}/SubmissionMinute={submission_minute}/SubmissionSecond={submission_second}"

    df = df.withColumn("SalesOrderYear", substring("SalesOrderDate", 1, 4)).withColumn("SalesOrderMonth", substring("SalesOrderDate", 5, 2))
    df.write.partitionBy("SalesOrderYear", "SalesOrderMonth").mode("overwrite").parquet(integrated_path)


def transform_test(df, cleansed_date_format):
    '''
    This function transforms the Sales cleansed data based off the business requirements,
    preparing it for the Cleansedlayer
    '''
    schema = FactSales_schema
    df_fact_sales = df
    date_columns = ["SalesOrderDate"]
    for column in date_columns:
        df_fact_sales = df_fact_sales.withColumn(column, date_format(to_date(col(column), cleansed_date_format), "yyyyMMdd").cast("int"))
    df_fact_sales = df_fact_sales.withColumn("FactHashID", xxhash64(concat_ws("|", *[col(column) for column in df_fact_sales.columns])))
    df_fact_sales = df_fact_sales.withColumn("CurrencyKey", lit("GBP"))
    df_fact_sales = df_fact_sales.withColumn("ProductKey", xxhash64(concat_ws("|", col("ProductID"), lit("Product"), lit("ProductCategory"))))
    df_fact_sales = df_fact_sales.withColumn("SpecialOfferKey", xxhash64(concat_ws("|", col("SpecialOfferID"), lit("Sales"))))
    df_fact_sales = df_fact_sales.withColumnRenamed("TotalUnits", "UnitVolume").withColumnRenamed("TotalLineValue", "TotalSalesLineValue")
    df_fact_sales = df_fact_sales.select(schema.fieldNames())
    for col_name in df_fact_sales.columns:
        data_type = df_fact_sales.schema[col_name].dataType
        df_fact_sales = df_fact_sales.withColumn(col_name, col(col_name).cast(data_type))

    return df_fact_sales