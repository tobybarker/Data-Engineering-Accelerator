from pyspark.sql.dataframe import DataFrame
from tests.fixtures.fixture_integrated_fact_sales import expected_df_integrated_fact_sales
from tests.fixtures.fixture_cleansed_sales import expected_df_cleansed_sales
from utilities.helpers.test_helpers import assert_spark_dataframes_match
#import the transform logic applied for integrated fact sales

def test_integrated_fact_sales_transforms(expected_df_cleansed_sales: DataFrame, expected_df_integrated_fact_sales: DataFrame) -> None:

    actual_df_integrated_fact_sales: DataFrame = expected_df_cleansed_sales # Apply imported transform logic to the expected cleansed sales DF here

    assert_spark_dataframes_match(actual_df_integrated_fact_sales, expected_df_integrated_fact_sales)