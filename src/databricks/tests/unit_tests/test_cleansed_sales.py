from pyspark.sql.dataframe import DataFrame
from src.databricks.tests.fixtures.fixture_cleansed_sales import input_df_cleansed_sales, expected_df_cleansed_sales
from src.databricks.utilities.helpers.test_helpers import assert_spark_dataframes_match
from src.databricks.jobs.source.sales import transform_test
#import the transform logic applied for cleansed sales

def test_cleansed_sales_transforms(input_df_cleansed_sales: DataFrame, expected_df_cleansed_sales: DataFrame) -> None:

    actual_df_cleansed_sales: DataFrame = transform_test(input_df_cleansed_sales) # Apply imported transform logic to the input sales cleansed DF here

    assert_spark_dataframes_match(actual_df_cleansed_sales, expected_df_cleansed_sales)
