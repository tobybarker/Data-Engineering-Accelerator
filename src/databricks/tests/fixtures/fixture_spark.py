import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope='session')
def spark():
    spark = SparkSession.builder.getOrCreate()
    # spark = SparkSession.builder.master('local').getOrCreate()

    yield spark
    spark.stop()