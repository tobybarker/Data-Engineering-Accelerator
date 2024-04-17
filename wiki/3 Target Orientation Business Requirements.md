# Business Requirements: Target Orientation

## General Requirements

All files should be loaded to the Integrated layer in `parquet` format and `overwrite` mode using the following directory structure:

1. Integrated
2. Domain
3. Entity
4. `schemaVersion=1`
5. Submission Year Number in `yyyy` format.
6. Submission Month of teh Year in `MM` format.
7. Submission Day of the Month in `dd` format.
8. Submission Hour of the Day in `HH` format.
9. Submission Minute of the Hour in `mm` format.
10. Submission Second of the Minute in `ss` format.

## DimProduct

### DimProduct Schema

|Column Name|Data Type|
|---|---|
|ProductKey|LongType|
|ProductName|StringType|
|ProductLineName|StringType|
|ProductModelName|StringType|
|ProductCategoryName|StringType|
|ProductSubCategoryName|StringType|
|ProductColor|StringType|
|ProductSize|StringType|
|ProductWeightKilograms|DoubleType|
|ProductCost|DoubleType|
|ProductListPrice|DoubleType|
|ProductProfitAtListPrice|DoubleType|
|ProductMarginAtListPrice|DoubleType|
|ProductLine|StringType|
|ProductClass|StringType|
|ProductStyle|StringType|
|IsFinishedGood|BooleanType|
|ProductSellingStartDate|IntegerType|
|ProductSellingEndDate|IntegerType|
|ProductDiscontinuedDate|IntegerType|
|CurrencyKey|StringType|

### Integrated DimProduct Requirements

- Dates should be in DateKey format (`yyyyMMdd`).
- New field `CurrencyKey` with Metadata `GBP`.
- New field `ProductLineName` where:
    1. `ProductLine == M` then `Mountain Bikes`
    2. `ProductLine == NULL` then `Bike Parts`
    3. `ProductLine == R` then `Road Bikes`
    4. `ProductLine == S` then `Accessories and Attire`
    5. `ProductLine == T` then `Touring Bikes`
    6. else `Unknown`
- New field `ProductKey` is an xxhash64 key sourced from the following pattern `SourceProductID|STATIC('Product')|STATIC('ProductCategory')`.
- Left outer join between `Product` & `ProductCategory` on `ProductCategoryID`.
- All `doubleType` columns must be rounded to 2 decimal places

## FactSales

### FactSales Schema

|Column Name|Data Type|
|---|---|
|FactHashID|LongType|
|SalesOrderNumber|StringType|
|SalesOrderDate|IntegerType|
|ProductKey|LongType|
|SpecialOfferKey|LongType|
|UnitVolume|IntegerType|
|CurrencyKey|StringType|
|UnitPrice|DoubleType|
|UnitPriceDiscountPercentage|DoubleType|
|UnitPriceAfterDiscount|DoubleType|
|TotalSalesLineValue|DoubleType|

### Integrated FactSales Field Requirements

- New field `CurrencyKey` with Metadata `GBP`.
- Dates should be in DateKey format (`yyyyMMdd`).
- New field `ProductKey` is an xxhash64 key sourced from the following pattern `SourceProductID|STATIC('Product')|STATIC('ProductCategory')`.
- New field `FactHashID` is an xxhash64 key sourced from all existing columns (excluding new fields) with delimiter `|`.
- New field `SpecialOfferKey` is an xxhash64 key sourced from the following pattern `SpecialOfferID|STATIC('Sales')`.
- Partition the file by Month and Year from the field `SalesOrderDate`.


- Note: ensure all column types have been assigned according to its respective schema spec, otherwise hash outcomes may be differ to existing fixtures values (for pytest / unit testing)