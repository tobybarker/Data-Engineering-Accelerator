from pyspark.sql.dataframe import DataFrame
from tests.fixtures.fixture_cleansed_product_category import input_df_cleansed_product_category, expected_df_cleansed_product_category
from utilities.helpers.test_helpers import assert_spark_dataframes_match
from jobs.source.product_category import transform_test
#import the transform logic applied for cleansed product category

def test_cleansed_product_category_transforms(input_df_cleansed_product_category: DataFrame, expected_df_cleansed_product_category: DataFrame) -> None:

    actual_df_cleansed_product_category: DataFrame = transform_test(input_df_cleansed_product_category) # Apply imported transform logic to the input product category cleansed DF here

    assert_spark_dataframes_match(actual_df_cleansed_product_category, expected_df_cleansed_product_category)

