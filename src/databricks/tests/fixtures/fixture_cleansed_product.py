import pytest
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, IntegerType

@pytest.fixture(scope='module')
def input_df_cleansed_product(spark):
    df: DataFrame = spark.createDataFrame(
        [
            ("680", "HL Road Frame", "HL Road Frame - Black, 58 ", "Black", "58", 2.24, "FR-R92B-58", "14", 1059.31, 1431.5, "R", "H", "U", "TRUE", True, "30/04/2008 00:00:00", "NULL", "NULL", "NULL"),
            ("706", "HL Road Frame", "HL Road Frame - Red, 58", "Red", "58", 2.24, "FR-R92R-58", "14", 1059.31, 1431.5, "R", "H", "U", "TRUE", True, "30/04/2008 00:00:00", "NULL", "NULL", "NULL"),
            ("707", "Sport-100", "Sport-100 Helmet, Red", "Red", "NULL", 0.0, "HL-U509-R", "24", 13.0863, 34.99, "S", "NULL", "NULL", "FALSE", True, "31/05/2011 00:00:00", "NULL", "NULL", "NULL"),
            ("708", "Sport-100", "Sport-100 Helmet, Black", "Black", "NULL", 0.0, "HL-U509", "24", 13.0863, 34.99, "S", "NULL", "NULL", "FALSE", True, "31/05/2011 00:00:00", "NULL", "NULL", "NULL"),
            ("709", "Mountain Bike Socks", "Mountain Bike Socks, M", "White", "M", 0.0, "SO-B909-M", "23", 3.3963, 9.5, "M", "NULL", "U", "FALSE", True, "31/05/2011 00:00:00", "29/05/2012 00:00:00", "NULL", "NULL"),
            ("710", "Mountain Bike Socks", "Mountain Bike Socks, L ", "White", "L", 0.0, "SO-B909-L", "23", 3.3963, 9.5, "M", "NULL", "U", "FALSE", True, "31/05/2011 00:00:00", "29/05/2012 00:00:00", "NULL", "NULL"),
            ("711", "Sport-100", " Sport-100 Helmet, Blue", "Blue", "NULL", 0.0, "HL-U509-B", "24", 13.0863, 34.99, "S", "NULL", "NULL", "FALSE", True, "31/05/2011 00:00:00", "NULL", "NULL", "NULL"),
            ("712", "Cycling Cap", "AWC Logo Cap", "Multi", "NULL", 0.0, "CA-1098", "19", 6.9223, 8.99, "S", "NULL", "U", "FALSE", True, "31/05/2011 00:00:00", "NULL", "NULL", "NULL"),
            ("713", "Long-Sleeve Logo Jersey", "Long-Sleeve Logo Jersey, S", "Multi", "S", 0.0, "LJ-0192-S", "21", 38.4923, 49.99, "S", "NULL", "U", "FALSE", True, "31/05/2011 00:00:00", "NULL", "NULL", "NULL"),
            ("714", "Long-Sleeve Logo Jersey", "Long-Sleeve Logo Jersey, M", "Multi", "M", 0.0, "LJ-0192-M", "21", 38.4923, 49.99, "X", "NULL", "U", "FALSE", True, "31/05/2011 00:00:00", "NULL", "NULL", "NULL")
        ],
        ("ProductID", "ModelName", "ProductName", "ProductColor", "Size", "ProductWeight", "UniqueProductNumber", 
        "ProductCategoryID", "StandardCost", "ListPrice", "ProductLine", "Class", "Style", "ProductMakeFlag", 
        "ProductFinishedGoodsFlag", "SellStartDate", "SellEndDate", "DiscontinuedDate", "ModifiedDate")
    )
    return df

@pytest.fixture(scope='module')
def expected_df_cleansed_product(spark):
    schema = StructType([
            StructField("SourceProductID", StringType(), False),
            StructField("ProductModelName", StringType(), True),
            StructField("ProductName", StringType(), False),
            StructField("ProductColor", StringType(), True),
            StructField("ProductSize", StringType(), True),
            StructField("ProductWeightKilograms", DoubleType(), True),
            StructField("ProductWeightGrams", IntegerType(), True),
            StructField("ProductUID", StringType(), False),
            StructField("ProductCategoryID", StringType(), True),
            StructField("ProductCost", DoubleType(), True),
            StructField("ProductListPrice", DoubleType(), True),
            StructField("ProductProfitAtListPrice", DoubleType(), True),
            StructField("ProductMarginAtListPrice", DoubleType(), True),
            StructField("ProductLine", StringType(), True),
            StructField("ProductClass", StringType(), True),
            StructField("ProductStyle", StringType(), True),
            StructField("IsFinishedGood", BooleanType(), True),
            StructField("ProductSellingStartDate", StringType(), True),
            StructField("ProductSellingEndDate", StringType(), True),
            StructField("ProductDiscontinuedDate", StringType(), True)
        ])
    df: DataFrame = spark.createDataFrame(
        [
            ("680", "HL Road Frame", "HL Road Frame - Black, 58", "Black", "58", 2.24, 2240, "FR-R92B-58", "14", 1059.31, 1431.5, 372.19, 0.35, "R", "H", "U", True, "30/04/2008 00:00:00", None, None),
            ("706", "HL Road Frame", "HL Road Frame - Red, 58", "Red", "58", 2.24, 2240, "FR-R92R-58", "14", 1059.31, 1431.5, 372.19, 0.35, "R", "H", "U", True, "30/04/2008 00:00:00", None, None),
            ("707", "Sport-100", "Sport-100 Helmet, Red", "Red", None, None, None, "HL-U509-R", "24", 13.09, 34.99, 21.90, 1.67, "S", None, None, True, "31/05/2011 00:00:00", None, None),
            ("708", "Sport-100", "Sport-100 Helmet, Black", "Black", None, None, None, "HL-U509", "24", 13.09, 34.99, 21.90, 1.67, "S", None, None, True, "31/05/2011 00:00:00", None, None),
            ("709", "Mountain Bike Socks", "Mountain Bike Socks, M", "White", "M", None, None, "SO-B909-M", "23", 3.4, 9.5, 6.1, 1.79, "M", None, "U", True, "31/05/2011 00:00:00", "29/05/2012 00:00:00", None),
            ("710", "Mountain Bike Socks", "Mountain Bike Socks, L", "White", "L", None, None, "SO-B909-L", "23", 3.4, 9.5, 6.1, 1.79, "M", None, "U", True, "31/05/2011 00:00:00", "29/05/2012 00:00:00", None),
            ("711", "Sport-100", "Sport-100 Helmet, Blue", "Blue", None, None, None, "HL-U509-B", "24", 13.09, 34.99, 21.90, 1.67, "S", None, None, True, "31/05/2011 00:00:00", None, None),
            ("712", "Cycling Cap", "AWC Logo Cap", "Multi", None, None, None, "CA-1098", "19", 6.92, 8.99, 2.07, 0.3, "S", None, "U", True, "31/05/2011 00:00:00", None, None),
            ("713", "Long-Sleeve Logo Jersey", "Long-Sleeve Logo Jersey, S", "Multi", "S", None, None, "LJ-0192-S", "21", 38.49, 49.99, 11.5, 0.3, "S", None, "U", True, "31/05/2011 00:00:00", None, None),
            ("714", "Long-Sleeve Logo Jersey", "Long-Sleeve Logo Jersey, M", "Multi", "M", None, None, "LJ-0192-M", "21", 38.49, 49.99, 11.5, 0.3, "X", None, "U", True, "31/05/2011 00:00:00", None, None)
        ],
        schema=schema
    )
    return df
