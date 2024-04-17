# Business Requirements: Source Orientation

## General Requirements

All files should be loaded to the Cleansed layer in `parquet` format and `overwrite` mode using the following directory structure:

1. Cleansed
2. DataFeed Name
3. `schemaVersion=1`
4. Submission Year Number in `yyyy` format.
5. Submission Month of the Year in `MM` format.
6. Submission Day of the Month in `dd` format.
7. Submission Hour of the Day in `HH` format.
8. Submission Minute of the Hour in `mm` format.
9. Submission Second of the Minute in `ss` format.

## Product Category

### Product Category Column Mapping

|Source Column Header|Target Column Header|Target Column Data Type|
|---|---|---|
|ID|ProductCategoryID|StringType|
|Level1|ProductCategoryName|StringType|
|Level2|ProductSubCategoryName|StringType|

### Cleansed Product Category Field Requirements

All `StringType` columns must be in Title Case format.
All `StringType` columns should be trimmed for leading, trailing whitespaces and tabs 

## Product

### Product Column Mapping

|Source Column Header|Target Column Header|Target Column Data Type|
|---|---|---|
|ProductID|SourceProductID|StringType|
|ModelName|ProductModelName|StringType|
|ProductName|ProductName|StringType|
|ProductColor|ProductColor|StringType|
|Size|ProductSize|StringType|
|ProductWeight|ProductWeightKilograms|DoubleType|
|UniqueProductNumber|ProductUID|StringType|
|ProductCategoryID|ProductCategoryID|StringType|
|StandardCost|ProductCost|DoubleType|
|ListPrice|ProductListPrice|DoubleType|
|ProductLine|ProductLine|StringType|
|Class|ProductClass|StringType|
|Style|ProductStyle|StringType|
|ProductMakeFlag|||
|ProductFinishedGoodsFlag|IsFinishedGood|BooleanType|
|SellStartDate|ProductSellingStartDate|StringType|
|SellEndDate|ProductSellingEndDate|StringType|
|DiscontinuedDate|ProductDiscontinuedDate|StringType|
|ModifiedDate|||

### Cleansed Product Field Requirements

- All empty or 'NULL' cells should be None.
- All `ProductWeightKilograms` cells with value `0` should be None.
- All `StringType` columns should be trimmed for leading, trailing whitespaces and tabs 
- `IsFinishedGood` should be `BooleanType`
- New field `ProductWeightGrams = ProductWeightKilograms * 1000`, cast to 'integerType'.
- New field `ProductProfitAtListPrice = ProductListPrice - ProductCost`, cast to 'doubleType'.
- New field `ProductMarginAtListPrice = ProductProfitAtListPrice / ProductCost`, cast to 'doubleType'.
- All `doubleType` columns must be rounded to 2 decimal places

## Sales

### Sales Column Mapping

|Source Column Header|Target Column Header|Target Column Data Type|
|---|---|---|
|SalesOrderID|SalesOrderID|StringType|
|SalesOrderDetailID|SalesOrderLineID|StringType|
|SalesOrderNumber|SalesOrderNumber|StringType|
|OrderDate|SalesOrderDate|StringType|
|ProductID|ProductID|StringType|
|SpecialOfferID|SpecialOfferID|StringType|
|SpecialOfferName|SpecialOfferName|StringType|
|SpecialOfferDiscountPct|SpecialOfferDiscountPct|DoubleType|
|SpecialOfferType|SpecialOfferType|StringType|
|SpecialOfferCategory|SpecialOfferCategory|StringType|
|SpecialOfferStartDate|SpecialOfferStartDate|StringType|
|SpecialOfferEndDate|SpecialOfferEndDate|StringType|
|OrderQty|TotalUnits|IntegerType|
|UnitPrice|UnitPrice|DoubleType|
|UnitPriceDiscount|UnitPriceDiscountPercentage|DoubleType|
|LineTotal|TotalLineValue|DoubleType|

### Cleansed Sales Field Requirements

- All `StringType` columns should be trimmed for leading, trailing whitespaces and tabs 
- New field `UnitDiscountValue = UnitPrice * UnitDiscountPercentage`, cast to 'doubleType'.
- New field `UnitPriceAfterDiscount = Unit Price - UnitDiscountValue`, cast to 'doubleType'.
- `UnitPriceDiscountPercentage` cells with value `NULL` should be `0`.
- All `doubleType` columns must be rounded to 2 decimal places
