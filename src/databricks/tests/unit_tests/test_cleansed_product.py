import sys
sys.path.append('C:/Users/TobyBarker/Documents/Data_Engineering_Accelerator/src/databricks')

from pyspark.sql.dataframe import DataFrame
from tests.fixtures.fixture_cleansed_product import input_df_cleansed_product, expected_df_cleansed_product
from utilities.helpers.test_helpers import assert_spark_dataframes_match
from jobs.source.product import transform
#import the transform logic applied for cleansed product

def test_cleansed_product_transforms(input_df_cleansed_product: DataFrame, expected_df_cleansed_product: DataFrame) -> None:

    actual_df_cleansed_product: DataFrame = transform(input_df_cleansed_product) # Apply imported transform logic to the input product cleansed DF here

    assert_spark_dataframes_match(actual_df_cleansed_product, expected_df_cleansed_product)



