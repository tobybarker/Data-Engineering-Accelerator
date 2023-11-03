from pyspark.sql.types import (StructType, StructField, StringType, LongType, DoubleType, IntegerType)

FactSales_schema = StructType([
    StructField("FactHashID", LongType()),
    StructField("SalesOrderNumber", StringType()),
    StructField("SalesOrderDate", IntegerType()),
    StructField("ProductKey", LongType()),
    StructField("SpecialOfferKey", LongType()),
    StructField("UnitVolume", IntegerType()),
    StructField("CurrencyKey", StringType()),
    StructField("UnitPrice", DoubleType()),
    StructField("UnitPriceDiscountPercentage", DoubleType()),
    StructField("UnitPriceAfterDiscount", DoubleType()),
    StructField("TotalSalesLineValue", DoubleType())
])