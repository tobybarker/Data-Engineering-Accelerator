from pyspark.sql.types import (StructType, StructField, StringType, IntegerType, DoubleType)

sales_schema = StructType([
    StructField("SalesOrderLineID", StringType()),
    StructField("SalesOrderID", StringType()),
    StructField("SalesOrderNumber", StringType()),
    StructField("SalesOrderDate", StringType()),
    StructField("ProductID", StringType()),
    StructField("SpecialOfferID", StringType()),
    StructField("SpecialOfferName", StringType()),
    StructField("SpecialOfferDiscountPct", DoubleType()),
    StructField("SpecialOfferType", StringType()),
    StructField("SpecialOfferCategory", StringType()),
    StructField("SpecialOfferStartDate", StringType()),
    StructField("SpecialOfferEndDate", StringType()),
    StructField("TotalUnits", IntegerType()),
    StructField("UnitPrice", DoubleType()),
    StructField("UnitPriceDiscountPercentage", DoubleType()),
    StructField("TotalLineValue", DoubleType())
])