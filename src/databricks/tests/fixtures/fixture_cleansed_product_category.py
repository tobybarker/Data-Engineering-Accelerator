import pytest
from pyspark.sql.dataframe import DataFrame

@pytest.fixture(scope='module')
def input_df_cleansed_product_category(spark):
    df: DataFrame = spark.createDataFrame(
        [
            ("1",	"   bikes",	"mountain bikes"),
            ("2",	"   bikes",	"road bikes "),
            ("3",	"Bikes", "touring bikes"),
            ("4",	"components  ",	"handlebars"),
            ("5",	"components	", "bottom brackets"),
            ("6",	"components	", "brakes"),
            ("7",	"components	", "chains"),
            ("8",	"components	", "cranksets"),
            ("9",	"components", "derailleurs"),
            ("10", "components", "forks"),
            ("11", "components", "headsets"),
            ("12", "components", "mountain frames"),
            ("13", "components", "pedals"),
            ("14", "components", "road frames"),
            ("15", "components", "saddles"),
            ("16", "components", "touring frames"),
            ("17", "components", "wheels"),
            ("18", "clothing", "Bib-shorts"),
            ("19", "clothing", "CAPS"),
            ("20", "clothing", "GLOVES"),
            ("21", " clothing", "JERSEYS"),
            ("22", "clothing  ", "SHORTS"),
            ("23", "clothing  ", "SOCKS"),
            ("24", "clothing", "helmet")
        ], 
        ("ID", "Level1", "Level2")
    )
    return df

@pytest.fixture(scope='module')
def expected_df_cleansed_product_category(spark):
    df: DataFrame = spark.createDataFrame(
        [
            ("1",	"Bikes", "Mountain Bikes"),
            ("2", "Bikes", "Road Bikes"),
            ("3", "Bikes", "Touring Bikes"),
            ("4", "Components", "Handlebars"),
            ("5", "Components", "Bottom Brackets"),
            ("6", "Components", "Brakes"),
            ("7", "Components", "Chains"),
            ("8", "Components", "Cranksets"),
            ("9", "Components", "Derailleurs"),
            ("10", "Components", "Forks"),
            ("11", "Components", "Headsets"),
            ("12", "Components", "Mountain Frames"),
            ("13", "Components", "Pedals"),
            ("14", "Components", "Road Frames"),
            ("15", "Components", "Saddles"),
            ("16", "Components", "Touring Frames"),
            ("17", "Components", "Wheels"),
            ("18", "Clothing", "Bib-shorts"),
            ("19", "Clothing", "Caps"),
            ("20", "Clothing", "Gloves"),
            ("21", "Clothing", "Jerseys"),
            ("22", "Clothing", "Shorts"),
            ("23", "Clothing", "Socks"),
            ("24", "Clothing", "Helmet")
        ], 
        ("ProductCategoryID", "ProductCategoryName", "ProductSubCategoryName")  
    )
    return df