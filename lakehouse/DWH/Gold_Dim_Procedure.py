# =============================================================================
# UHIP DWH — dim_procedure  |  SCD Type 1  |  Full Reload
# Source : medical_insurance.silver.medical_procedure_silver
# Audit  : dwh_created_at / dwh_updated_at default to current_timestamp()
# =============================================================================

query = """
    SELECT
        cast(ROW_NUMBER() OVER (ORDER BY procedure_code) as INT)  AS procedure_key,
        procedure_code,
        procedure_name,
        procedure_category,
        expected_amount,
        complexity_score,
        current_timestamp()                               AS dwh_created_at,
        current_timestamp()                               AS dwh_updated_at
    FROM medical_insurance.silver.medical_procedure_silver
"""

df = spark.sql(query)

df.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("medical_insurance.gold.dim_procedure_scd1")
