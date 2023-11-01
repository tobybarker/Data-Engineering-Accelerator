from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from schemas.cleansed.product import product_schema
spark = SparkSession.builder.appName("DataEngineerAccelerator").getOrCreate()

def read_csv_to_df(file_path):
    df = spark.read.csv(file_path, header="True", schema=product_schema)
    return df

def transform(file_path):
    '''
    This function transforms the Product source data based off the business requirements,
    preparing it for the Cleansedlayer
    '''
    df = read_csv_to_df(file_path)
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

def execute(file_path, workspace_folder):
    '''
    This function creates the Cleansed file path, transforms the source file and writes it to the Cleansed folder in parquet format. 
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

    df = transform(file_path)
    data_feed = df.withColumn("sourcefile",input_file_name())
    data_feed = data_feed.withColumn("sourcefile", substring_index("sourcefile","/", -1))
    data_feed = data_feed.withColumn("sourcefile", split(col("sourcefile"), "\\.")[0]).select("sourcefile").distinct()
    data_feed = data_feed.withColumn("sourcefile", split(col("sourcefile"), "%")[0]).select("sourcefile").distinct()
    data_feed = data_feed.first()["sourcefile"]

    cleansed_path = f"{workspace_folder}/src/databricks/sample_lake/Cleansed/DataFeed={data_feed}/schemaVersion=1/SubmissionYear={submission_year}/SubmissionMonth={submission_month}/SubmissionDay={submission_day}/SubmissionHour={submission_hour}/SubmissionMinute={submission_minute}/SubmissionSecond={submission_second}"
    
    df.write.mode("overwrite").parquet(cleansed_path)


def transform_test(df):
    '''
    This function transforms the Product source data based off the business requirements,
    preparing it for the Cleansedlayer
    '''
    schema=product_schema
    df = spark.createDataFrame(df.rdd, schema=schema)
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