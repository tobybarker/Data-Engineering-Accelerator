from pyspark.sql.functions import *

def apply_schema_to_df(df, schema):
    df_cols = df.columns
    for i in range(len(df_cols)):
        old_col = df_cols[i]
        new_col = schema.fields[i].name
        new_datatype = schema.fields[i].dataType
        df = df.withColumnRenamed(old_col, new_col)
        df = df.withColumn(new_col, col(new_col).cast(new_datatype))
    return df