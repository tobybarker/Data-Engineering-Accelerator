import sys
sys.path.append('C:/Users/TobyBarker/Documents/Data_Engineering_Accelerator/src/databricks')

from pyspark.sql.dataframe import DataFrame
from tests.fixtures.fixture_integrated_dim_product import expected_df_integrated_dim_product
from tests.fixtures.fixture_cleansed_product import expected_df_cleansed_product
from tests.fixtures.fixture_cleansed_product_category import expected_df_cleansed_product_category
from utilities.helpers.test_helpers import assert_spark_dataframes_match
from jobs.target.DimProduct import transform
#import the transform logic applied for integrated dim product

def test_integrated_dim_product_transforms(expected_df_cleansed_product: DataFrame, expected_df_cleansed_product_category: DataFrame, expected_df_integrated_dim_product: DataFrame) -> None:

    actual_df_integrated_dim_product: DataFrame = transform(expected_df_cleansed_product, expected_df_cleansed_product_category) # Apply imported transform logic to the expected cleansed product & expected cleansed product category DFs here

    assert_spark_dataframes_match(actual_df_integrated_dim_product, expected_df_integrated_dim_product)
