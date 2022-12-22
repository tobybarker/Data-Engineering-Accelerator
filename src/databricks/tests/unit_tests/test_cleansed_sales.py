from pyspark.sql.dataframe import DataFrame
from tests.fixtures.fixture_cleansed_sales import input_df_cleansed_sales, expected_df_cleansed_sales
from utilities.helpers.test_helpers import assert_spark_dataframes_match
#import the transform logic applied for cleansed sales

def test_cleansed_sales_transforms(input_df_cleansed_sales: DataFrame, expected_df_cleansed_sales: DataFrame) -> None:

    actual_df_cleansed_sales: DataFrame = input_df_cleansed_sales # Apply imported transform logic to the input sales cleansed DF here

    assert_spark_dataframes_match(actual_df_cleansed_sales, expected_df_cleansed_sales)
