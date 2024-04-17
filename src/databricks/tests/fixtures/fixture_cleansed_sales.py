import pytest
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

@pytest.fixture(scope='module')
def input_df_cleansed_sales(spark):
    df: DataFrame = spark.createDataFrame(
        [
            ("1", "43659", "SO43659", "31/05/2011 00:00:00", "776", "1", "No Discount", 0.0, "No Discount", "No Discount", "01/05/2011 00:00:00", "30/11/2014 00:00:00", 1, 2024.994, 0.0, 2024.99),
            ("2", "43659", "SO43659", "31/05/2011 00:00:00", "777", "1", "No Discount", 0.0, "No Discount", "No Discount", "01/05/2011 00:00:00", "30/11/2014 00:00:00", 3, 2024.994, 0.0, 6074.98),
            ("3", "43659", "SO43659", "31/05/2011 00:00:00", "778", "1", "No Discount", 0.0, "No Discount", "No Discount", "01/05/2011 00:00:00", "30/11/2014 00:00:00", 1, 2024.994, 0.0, 2024.99),
            ("4", "43659", "SO43659", "31/05/2011 00:00:00", "771", "1", "No Discount", 0.0, "No Discount", "No Discount", "01/05/2011 00:00:00", "30/11/2014 00:00:00", 1, 2039.994, 0.0, 2039.99),
            ("5", "43659", "SO43659", "31/05/2011 00:00:00", "772", "1", "No Discount", 0.0, "No Discount", "No Discount", "01/05/2011 00:00:00", "30/11/2014 00:00:00", 1, 2039.994, 0.0, 2039.99),
            ("6", "43659", "SO43659", "31/05/2011 00:00:00", "773", "1", "No Discount", 0.0, "No Discount", "No Discount", "01/05/2011 00:00:00", "30/11/2014 00:00:00", 2, 2039.994, 0.0, 4079.99),
            ("7", "43659", "SO43659", "31/05/2011 00:00:00", "774", "1", "No Discount", 0.0, "No Discount", "No Discount", "01/05/2011 00:00:00", "30/11/2014 00:00:00", 1, 2039.994, 0.0, 2039.99),
            ("8", "43659", "SO43659", "31/05/2011 00:00:00", "714", "1", "No Discount", 0.0, "No Discount", "No Discount", "01/05/2011 00:00:00", "30/11/2014 00:00:00", 3, 28.8404, 0.0, 86.52),
            ("9", "43659", "SO43659", "31/05/2011 00:00:00", "716", "1", "No Discount", 0.0, "No Discount", "No Discount", "01/05/2011 00:00:00", "30/11/2014 00:00:00", 1, 28.8404, 0.0, 28.84),
            ("10", "43659", "SO43659", "31/05/2011 00:00:00", "709", "1", "No Discount", 0.0, "No Discount", "No Discount", "01/05/2011 00:00:00", "30/11/2014 00:00:00", 6, 5.7, 0.0, 34.20)
        ],
        ("SalesOrderDetailID", "SalesOrderID", "SalesOrderNumber", "OrderDate", "ProductID", "SpecialOfferID", "SpecialOfferName", "SpecialOfferDiscountPct", 
        "SpecialOfferType", "SpecialOfferCategory", "SpecialOfferStartDate", "SpecialOfferEndDate", "OrderQty", "UnitPrice", "UnitPriceDiscount", "LineTotal")
    )
    return df

@pytest.fixture(scope='module')
def expected_df_cleansed_sales(spark):
    schema = StructType([
            StructField("SalesOrderLineID", StringType(), True),
            StructField("SalesOrderID", StringType(), False),
            StructField("SalesOrderNumber", StringType(), False),
            StructField("SalesOrderDate", StringType(), True),
            StructField("ProductID", StringType(), True),
            StructField("SpecialOfferID", StringType(), True),
            StructField("SpecialOfferName", StringType(), True),
            StructField("SpecialOfferDiscountPct", DoubleType(), True),
            StructField("SpecialOfferType", StringType(), True),
            StructField("SpecialOfferCategory", StringType(), True),
            StructField("SpecialOfferStartDate", StringType(), True),
            StructField("SpecialOfferEndDate", StringType(), True),
            StructField("TotalUnits", IntegerType(), False),
            StructField("UnitPrice", DoubleType(), False),
            StructField("UnitPriceDiscountPercentage", DoubleType(), True),
            StructField("UnitDiscountValue", DoubleType(), False),
            StructField("UnitPriceAfterDiscount", DoubleType(), False),
            StructField("TotalLineValue", DoubleType(), False)
            ])
    df: DataFrame = spark.createDataFrame(
        [
            ("1", "43659", "SO43659", "31/05/2011 00:00:00", "776", "1", "No Discount", 0.0, "No Discount", "No Discount", "01/05/2011 00:00:00", "30/11/2014 00:00:00", 1, 2024.99, 0.0, 0.00, 2024.99, 2024.99),
            ("2", "43659", "SO43659", "31/05/2011 00:00:00", "777", "1", "No Discount", 0.0, "No Discount", "No Discount", "01/05/2011 00:00:00", "30/11/2014 00:00:00", 3, 2024.99, 0.0, 0.00, 2024.99, 6074.98),
            ("3", "43659", "SO43659", "31/05/2011 00:00:00", "778", "1", "No Discount", 0.0, "No Discount", "No Discount", "01/05/2011 00:00:00", "30/11/2014 00:00:00", 1, 2024.99, 0.0, 0.00, 2024.99, 2024.99),
            ("4", "43659", "SO43659", "31/05/2011 00:00:00", "771", "1", "No Discount", 0.0, "No Discount", "No Discount", "01/05/2011 00:00:00", "30/11/2014 00:00:00", 1, 2039.99, 0.0, 0.00, 2039.99, 2039.99),
            ("5", "43659", "SO43659", "31/05/2011 00:00:00", "772", "1", "No Discount", 0.0, "No Discount", "No Discount", "01/05/2011 00:00:00", "30/11/2014 00:00:00", 1, 2039.99, 0.0, 0.00, 2039.99, 2039.99),
            ("6", "43659", "SO43659", "31/05/2011 00:00:00", "773", "1", "No Discount", 0.0, "No Discount", "No Discount", "01/05/2011 00:00:00", "30/11/2014 00:00:00", 2, 2039.99, 0.0, 0.00, 2039.99, 4079.99),
            ("7", "43659", "SO43659", "31/05/2011 00:00:00", "774", "1", "No Discount", 0.0, "No Discount", "No Discount", "01/05/2011 00:00:00", "30/11/2014 00:00:00", 1, 2039.99, 0.0, 0.00, 2039.99, 2039.99),
            ("8", "43659", "SO43659", "31/05/2011 00:00:00", "714", "1", "No Discount", 0.0, "No Discount", "No Discount", "01/05/2011 00:00:00", "30/11/2014 00:00:00", 3, 28.84, 0.0, 0.00, 28.84, 86.52),
            ("9", "43659", "SO43659", "31/05/2011 00:00:00", "716", "1", "No Discount", 0.0, "No Discount", "No Discount", "01/05/2011 00:00:00", "30/11/2014 00:00:00", 1, 28.84, 0.0, 0.00, 28.84, 28.84),
            ("10", "43659", "SO43659", "31/05/2011 00:00:00", "709", "1", "No Discount", 0.0, "No Discount", "No Discount", "01/05/2011 00:00:00", "30/11/2014 00:00:00", 6, 5.7, 0.0, 0.00, 5.7, 34.2)
        ],
        schema=schema
    )
    return df
