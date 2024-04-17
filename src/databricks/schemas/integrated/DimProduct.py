from pyspark.sql.types import (StructType, StructField, StringType, LongType, DoubleType, BooleanType, IntegerType)

DimProduct_schema = StructType([
    StructField("ProductKey", LongType()),
    StructField("ProductName", StringType()),
    StructField("ProductLineName", StringType()),
    StructField("ProductModelName", StringType()),
    StructField("ProductCategoryName", StringType()),
    StructField("ProductSubCategoryName", StringType()),   
    StructField("ProductColor", StringType()),
    StructField("ProductSize", StringType()),
    StructField("ProductWeightKilograms", DoubleType()),
    StructField("ProductCost", DoubleType()),
    StructField("ProductListPrice", DoubleType()),
    StructField("ProductProfitAtListPrice", DoubleType()),
    StructField("ProductMarginAtListPrice", DoubleType()),
    StructField("ProductLine", StringType()),
    StructField("ProductClass", StringType()),
    StructField("ProductStyle", StringType()),
    StructField("IsFinishedGood", BooleanType()),
    StructField("ProductSellingStartDate", IntegerType()),
    StructField("ProductSellingEndDate", IntegerType()),
    StructField("ProductDiscontinuedDate", IntegerType()),
    StructField("CurrencyKey", StringType())
])