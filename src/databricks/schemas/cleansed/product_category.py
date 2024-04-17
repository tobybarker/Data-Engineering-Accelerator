from pyspark.sql.types import (StructType, StructField, StringType)

product_category_schema = StructType([
    StructField("ProductCategoryID", StringType()),
    StructField("ProductCategoryName", StringType()),
    StructField("ProductSubCategoryName", StringType())
])