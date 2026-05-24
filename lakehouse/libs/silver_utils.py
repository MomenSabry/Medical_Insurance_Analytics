from pyspark.sql.functions import *
from pyspark.sql.types import StringType

# Remove duplicate rows
def remove_duplicates(df, subset=None):
    return df.dropDuplicates(subset)

# Trim all string columns in the DataFrame
def trim_all_string_columns(df):
    for col_name, dtype in df.dtypes:
        if dtype == "string":
            df = df.withColumn(col_name, trim(col(col_name)))
    return df 

# capitalize firest letter of each word
def capitalize_first_letter_of_each_word(df):
   for col_name, dtype in df.dtypes:
        if dtype == "string":
            df = df.withColumn(col_name, initcap(col(col_name)))
   return df

# Convert all strings to lowercase
def lowercase_columns(df):
    for col_name, dtype in df.dtypes:
        if dtype == "string":
            df = df.withColumn(col_name, lower(col(col_name)))
    return df

# Replace null values in string columns with a specified replacement
def replace_null_strings(df, replacement="Unknown"):
    for col_name, dtype in df.dtypes:
        if dtype == "string":
            df = df.fillna({col_name: replacement})
    return df

# Replace null values in numeric columns with a specified replacement
def replace_null_numeric(df, replacement=0):
    numeric_types = ["int", "bigint", "double", "float", "decimal"]

    for col_name, dtype in df.dtypes:
        if any(dtype.startswith(t) for t in numeric_types):
            df = df.fillna({col_name: replacement})

    return df

# Convert string columns to date
def convert_to_date(df, date_columns, fmt="yyyy-MM-dd"):
    for c in date_columns:
        df = df.withColumn(c, to_date(col(c), fmt))
    return df

# Replace all blanck values in date columns with null
def replace_blank_date(df):
    for col_name, dtype in df.dtypes:
        if dtype == "date":
            df = df.withColumn(
                col_name,
                when(col(col_name).cast("string") == "", lit(None).cast("date"))
                .otherwise(col(col_name))
            )
    return df
    
# Add audit columns to the DataFrame
def add_audit_columns(df):
    return (
        df.withColumn("_created_at", current_timestamp())
    )

# Rename columns in the DataFrame based on the provided mapping
def rename_columns(df, rename_map):
    for oldname, newname in rename_map.items():
        df = df.withColumnRenamed(oldname, newname)
    return df

# change data type of columns
def cast_columns(df, cast_map):
    for col_name, data_type in cast_map.items():
        df = df.withColumn(
            col_name,
            col(col_name).cast(data_type)
        )
    return df


# Convert phone numbers to international dashed format
# Example:
# 01001234567   -> +20-100-123-4567
# +201001234567 -> +20-100-123-4567
# 201001234567  -> +20-100-123-4567

def format_egypt_phone_numbers(df, phone_columns):

    for c in phone_columns:

        # remove spaces, dashes, brackets, etc.
        cleaned = regexp_replace(col(c), r"[^0-9]", "")

        # remove leading 0 if exists
        cleaned = when(
            cleaned.startswith("0"),
            substring(cleaned, 2, 20)
        ).otherwise(cleaned)

        # remove country code if already exists
        cleaned = when(
            cleaned.startswith("20"),
            substring(cleaned, 3, 20)
        ).otherwise(cleaned)

        # final formatting
        formatted = concat(
            lit("+20-"),
            substring(cleaned, 1, 3),
            lit("-"),
            substring(cleaned, 4, 3),
            lit("-"),
            substring(cleaned, 7, 4)
        )

        df = df.withColumn(c, formatted)

    return df

# Concat columns
def concat_columns(df, concat_map):
    for col_name, concat_list in concat_map.items():
        df = df.withColumn(
            col_name,
            concat_ws("-", *[col(c) for c in concat_list])
        )
    return df



