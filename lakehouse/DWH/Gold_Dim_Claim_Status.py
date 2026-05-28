# =============================================================================
# UHIP DWH — dim_claim_status  |  SCD Type 1  |  Full Reload
# Source : medical_insurance.silver.claim_approval_silver
# Audit  : dwh_created_at / dwh_updated_at default to current_timestamp()
# =============================================================================

query = """
    SELECT
        ROW_NUMBER() OVER (
            ORDER BY approval_status, COALESCE(rejection_reason, '')
        )                           AS claim_status_key,
        approval_status             AS claim_status,
        rejection_reason,
        reviewed_by,
        current_timestamp()         AS dwh_created_at,
        current_timestamp()         AS dwh_updated_at
    FROM (
        SELECT DISTINCT
            approval_status,
            rejection_reason,
            reviewed_by
        FROM medical_insurance.silver.claim_approval_silver
    )
"""

df = spark.sql(query)

df.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("medical_insurance.gold.dim_claim_status_scd1")
