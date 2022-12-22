import pytest
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, LongType

@pytest.fixture(scope='module')
def expected_df_integrated_fact_sales(spark):
    schema = StructType([
            StructField("FactHashID", LongType(), False),
            StructField("SalesOrderNumber", StringType(), False),
            StructField("SalesOrderDate", IntegerType(), True),
            StructField("ProductKey", LongType(), True),
            StructField("SpecialOfferKey", LongType(), False),
            StructField("UnitVolume", IntegerType(), False),
            StructField("CurrencyKey", StringType(), True),
            StructField("UnitPrice", DoubleType(), False),
            StructField("UnitPriceDiscountPercentage", DoubleType(), True),
            StructField("UnitPriceAfterDiscount", DoubleType(), False),
            StructField("TotalSalesLineValue", DoubleType(), False)
            ])
    df: DataFrame = spark.createDataFrame(
        [
            (1013067066004009972, "SO43659", 20110531, 1448571491627430094, 7804943415359386941, 1, "GBP", 2024.99, 0.0, 2024.99, 2024.99),
            (8458028727701150143, "SO43659", 20110531, -7526531161666529242, 7804943415359386941, 3, "GBP", 2024.99, 0.0, 2024.99, 6074.98),
            (205548279962769564, "SO43659",	20110531, 5979043214446987507, 7804943415359386941, 1, "GBP", 2024.99, 0.0, 2024.99, 2024.99),
            (-3785096989628653810, "SO43659", 20110531, 2729102431827539889, 7804943415359386941, 1, "GBP", 2039.99, 0.0, 2039.99, 2039.99),
            (7561463332271309700, "SO43659", 20110531, -6727538919361665971, 7804943415359386941, 1, "GBP", 2039.99, 0.0, 2039.99, 2039.99),
            (6416287882889046226, "SO43659", 20110531, 2126323075666010660, 7804943415359386941, 2, "GBP", 2039.99, 0.0, 2039.99, 4079.99),
            (5529375964764051787, "SO43659", 20110531, -5247391021351068957, 7804943415359386941, 1, "GBP", 2039.99, 0.0, 2039.99, 2039.99),
            (-9142405478489033387, "SO43659", 20110531, 6662511115584918329, 7804943415359386941, 3, "GBP", 28.84, 0.0, 28.84, 86.52),
            (-1778260330230733222, "SO43659", 20110531, 8217268982441151547, 7804943415359386941, 1, "GBP", 28.84, 0.0, 28.84, 28.84),
            (-7638550306812500379, "SO43659", 20110531, -7821904850998458223, 7804943415359386941, 6, "GBP", 5.7, 0.0, 5.7, 34.2)
        ],
        schema=schema
    )
    return df