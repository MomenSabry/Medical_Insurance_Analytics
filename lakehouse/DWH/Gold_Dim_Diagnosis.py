# =============================================================================
# UHIP DWH — dim_diagnosis  |  SCD Type 1  |  Full Reload
# Source : olist_workspace.silver.diagnosis_silver
# Audit  : dwh_created_at / dwh_updated_at default to current_timestamp()
# =============================================================================

query = """
    SELECT
        cast(ROW_NUMBER() OVER (ORDER BY diagnosis_code) as INT)  AS diagnosis_key,
        diagnosis_code,
        diagnosis_name,
        diagnosis_category,
        severity_level,
        current_timestamp()                               AS dwh_created_at,
        current_timestamp()                               AS dwh_updated_at
    FROM olist_workspace.silver.diagnosis_silver
"""

df = spark.sql(query)

df.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("olist_workspace.gold.dim_diagnosis_scd1")
