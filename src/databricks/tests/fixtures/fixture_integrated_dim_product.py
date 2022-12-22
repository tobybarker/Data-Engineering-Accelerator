import pytest
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, IntegerType, LongType

@pytest.fixture(scope='module')
def expected_df_integrated_dim_product(spark):
    schema = StructType([
            StructField("ProductKey", LongType(), False),
            StructField("ProductName", StringType(), False),
            StructField("ProductLineName", StringType(), True),
            StructField("ProductModelName", StringType(), True),
            StructField("ProductCategoryName", StringType(), True),
            StructField("ProductSubCategoryName", StringType(), True),
            StructField("ProductColor", StringType(), True),
            StructField("ProductSize", StringType(), True),
            StructField("ProductWeightKilograms", DoubleType(), True),
            StructField("ProductCost", DoubleType(), True),
            StructField("ProductListPrice", DoubleType(), True),
            StructField("ProductProfitAtListPrice", DoubleType(), True),
            StructField("ProductMarginAtListPrice", DoubleType(), True),
            StructField("ProductLine", StringType(), True),
            StructField("ProductClass", StringType(), True),
            StructField("ProductStyle", StringType(), True),
            StructField("IsFinishedGood", BooleanType(), True),
            StructField("ProductSellingStartDate", IntegerType(), True),
            StructField("ProductSellingEndDate", IntegerType(), True),
            StructField("ProductDiscontinuedDate", IntegerType(), True),
            StructField("CurrencyKey", StringType(), True)
        ])
    df: DataFrame = spark.createDataFrame(
        [
            (3417713409978001529, "HL Road Frame - Black, 58", "Road Bikes", "HL Road Frame", "Components", "Road Frames", "Black", "58", 2.24, 1059.31, 1431.5, 372.19, 0.35, "R", "H", "U", True, 20080430, None, None, 'GBP'),
            (-1881900368359868715, "HL Road Frame - Red, 58", "Road Bikes", "HL Road Frame", "Components", "Road Frames", "Red", "58", 2.24, 1059.31, 1431.5, 372.19, 0.35, "R", "H", "U", True, 20080430, None, None, 'GBP'),
            (-2966093393230185734, "AWC Logo Cap", "Accessories and Attire", "Cycling Cap", "Clothing", "Caps", "Multi", None, None, 6.92, 8.99, 2.07, 0.3, "S", None, "U", True, 20110531, None, None, 'GBP'),
            (-1039264019302959644, "Long-Sleeve Logo Jersey, S", "Accessories and Attire", "Long-Sleeve Logo Jersey", "Clothing", "Jerseys", "Multi", "S", None, 38.49, 49.99, 11.5, 0.3, "S", None, "U", True, 20110531, None, None, 'GBP'),
            (6662511115584918329, "Long-Sleeve Logo Jersey, M", "Unknown", "Long-Sleeve Logo Jersey", "Clothing", "Jerseys", "Multi", "M", None, 38.49, 49.99, 11.5, 0.3, "X", None, "U", True, 20110531, None, None, 'GBP'),
            (-7821904850998458223, "Mountain Bike Socks, M", "Mountain Bikes", "Mountain Bike Socks", "Clothing", "Socks", "White", "M", None, 3.4, 9.5, 6.10, 1.79, "M", None, "U", True, 20110531, 20120529, None, 'GBP'),
            (4164473187252973125, "Mountain Bike Socks, L", "Mountain Bikes", "Mountain Bike Socks", "Clothing", "Socks", "White", "L", None, 3.4, 9.5, 6.10, 1.79, "M", None, "U", True, 20110531, 20120529, None, 'GBP'),
            (-985035173493096936, "Sport-100 Helmet, Red", "Accessories and Attire", "Sport-100", "Clothing", "Helmet", "Red", None, None, 13.09, 34.99, 21.90, 1.67, "S", None, None, True, 20110531, None, None, 'GBP'),
            (-4348248501987639895, "Sport-100 Helmet, Black", "Accessories and Attire", "Sport-100", "Clothing", "Helmet", "Black", None, None, 13.09, 34.99, 21.90, 1.67, "S", None, None, True, 20110531, None, None, 'GBP'),
            (-1772491284490633238, "Sport-100 Helmet, Blue", "Accessories and Attire", "Sport-100", "Clothing", "Helmet", "Blue", None, None, 13.09, 34.99, 21.90, 1.67, "S", None, None, True, 20110531, None, None, 'GBP')
            
        ], schema=schema
    )
    return df
