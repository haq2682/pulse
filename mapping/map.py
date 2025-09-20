import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import List as mapping_list


def normalize_dataframe(df, column_variants):
    variant_to_standard = {
        v.lower(): std_col
        for std_col, variants in column_variants.items()
        for v in variants
    }

    mapped_cols = {}
    new_columns = []
    for col in df.columns:
        col_lower = col.lower()
        if col_lower in variant_to_standard:
            std_col = variant_to_standard[col_lower]
            new_columns.append(std_col)
            mapped_cols[col] = std_col
        else:
            new_columns.append(col)

    for old_col, new_col in zip(df.columns, new_columns):
        df = df.withColumnRenamed(old_col, new_col)

    missing_cols = []
    for std_col in column_variants.keys():
        if std_col not in df.columns:
            df = df.withColumn(std_col, lit(None))
            missing_cols.append(std_col)

    schema_cols = list(column_variants.keys())
    extra_cols = [c for c in df.columns if c not in schema_cols]

    new_df = df.select(schema_cols)
    df_extra = df.select(schema_cols + extra_cols)

    return new_df, df_extra, extra_cols, missing_cols, mapped_cols


base_dir = os.path.dirname(os.path.abspath(__file__))
file_path_excel = os.path.join(base_dir, "./../faker/messy_customer_data.xlsx")
file_path_csv = os.path.join(base_dir, "./../faker/messy_customer_data.csv")

excel_df = pd.read_excel(file_path_excel, engine="openpyxl")
excel_df.to_csv(file_path_csv, index=False)

spark = SparkSession.builder.appName("NormalizeData").getOrCreate()
df = spark.read.csv(file_path_csv, header=True, inferSchema=True)

new_df, extra_df, extra_cols, missing, mapped = normalize_dataframe(
    df, mapping_list.customer_mapping_dict
)

print("\nNormalized DataFrame:")
new_df.show(5)

print("\nDataFrame with Extra Columns:")
extra_df.show(5)

print("\nMissing columns:")
print(missing)

print("\nExtra columns:")
print(extra_cols)

print("\nMapped columns:")
print(mapped)
