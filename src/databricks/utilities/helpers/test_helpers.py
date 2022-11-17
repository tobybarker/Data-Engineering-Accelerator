def assert_spark_dataframes_match(result_df, expected_df):
    '''Asserts that results_df matches expected_df

    Will throw an AssertionError with description of dataframe differences
    '''
    # only take columns that exist in expected
    result_df = result_df.select(expected_df.columns)

    assert result_df.rdd.collect() == expected_df.rdd.collect()
