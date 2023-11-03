from pyspark.sql.types import (StructType, StructField, StringType, DoubleType, BooleanType)

product_schema = StructType([
    StructField("SourceProductID", StringType()),
    StructField("ProductModelName", StringType()),
    StructField("ProductName", StringType()),
    StructField("ProductColor", StringType()),
    StructField("ProductSize", StringType()),
    StructField("ProductWeightKilograms", DoubleType()),
    StructField("ProductUID", StringType()),
    StructField("ProductCategoryID", StringType()),
    StructField("ProductCost", DoubleType()),
    StructField("ProductListPrice", DoubleType()),
    StructField("ProductLine", StringType()),
    StructField("ProductClass", StringType()),
    StructField("ProductStyle", StringType()),
    StructField("ProductMakeFlag", StringType()),
    StructField("IsFinishedGood", BooleanType()),
    StructField("ProductSellingStartDate", StringType()),
    StructField("ProductSellingEndDate", StringType()),
    StructField("ProductDiscontinuedDate", StringType()),
    StructField("ModifiedDate", StringType())
])