# =============================================================================
# UHIP DWH — dim_drug  |  SCD Type 2
# Source  : medical_insurance.silver.drug_silver
# Tracked : unit_price, drug_category, manufacturer
# =============================================================================

# ── CREATE TABLE (run safely on every execution) ─────────────────────────────
spark.sql("""
    CREATE TABLE IF NOT EXISTS medical_insurance.gold.dim_drug_scd2
    USING DELTA
    AS
    SELECT
        CAST(NULL AS BIGINT)        AS drug_key,
        drug_id,
        drug_name,
        generic_name,
        manufacturer,
        drug_category,
        CAST(unit_amount AS DECIMAL(8,2)) AS unit_price,
        CAST(NULL AS DATE)          AS scd_start_date,
        CAST(NULL AS DATE)          AS scd_end_date,
        CAST(NULL AS INT)           AS scd_is_current,
        CAST(NULL AS INT)           AS scd_version,
        CAST(NULL AS TIMESTAMP)     AS dwh_created_at,
        CAST(NULL AS TIMESTAMP)     AS dwh_updated_at
    FROM medical_insurance.silver.drug_silver
    WHERE 1 = 0
""")

# ── STEP 1: stage source ──────────────────────────────────────────────────────
query = """
    SELECT
        drug_id,
        drug_name,
        generic_name,
        manufacturer,
        drug_category,
        unit_amount
    FROM medical_insurance.silver.drug_silver
"""

df_src = spark.sql(query)
df_src.createOrReplaceTempView("stg_drug")

# ── STEP 2: expire changed current records ────────────────────────────────────
spark.sql("""
    MERGE INTO medical_insurance.gold.dim_drug_scd2 AS tgt
    USING (
        SELECT s.*
        FROM stg_drug s
        INNER JOIN medical_insurance.gold.dim_drug_scd2 d
            ON  d.drug_id        = s.drug_id
            AND d.scd_is_current = 1
        WHERE
            d.unit_price    <> s.unit_amount    OR
            d.drug_category <> s.drug_category OR
            d.manufacturer  <> s.manufacturer
    ) AS src
    ON  tgt.drug_id        = src.drug_id
    AND tgt.scd_is_current = 1
    WHEN MATCHED THEN UPDATE SET
        tgt.scd_end_date   = DATE_SUB(current_date(), 1),
        tgt.scd_is_current = 0,
        tgt.dwh_updated_at = current_timestamp()
""")

# ── STEP 3: insert new version rows + new drugs ───────────────────────────────
query_insert = """
    SELECT
        cast(row_number() over(order by s.drug_id) AS BIGINT)      AS drug_key,
        s.drug_id,
        s.drug_name,
        s.generic_name,
        s.manufacturer,
        s.drug_category,
        CAST(s.unit_amount AS DECIMAL(8,2))         AS unit_price,
        current_date()                      AS scd_start_date,
        CAST(NULL AS DATE)                  AS scd_end_date,
        1                                   AS scd_is_current,
        COALESCE(mv.max_version, 0) + 1     AS scd_version,
        current_timestamp()                 AS dwh_created_at,
        current_timestamp()                 AS dwh_updated_at
    FROM stg_drug s
    LEFT JOIN medical_insurance.gold.dim_drug_scd2 active
        ON  active.drug_id       = s.drug_id
        AND active.scd_is_current = 1
    LEFT JOIN (
        SELECT drug_id, MAX(scd_version) AS max_version
        FROM medical_insurance.gold.dim_drug_scd2
        GROUP BY drug_id
    ) mv ON mv.drug_id = s.drug_id
    WHERE active.drug_id IS NULL
"""

df_insert = spark.sql(query_insert)

df_insert.write \
    .mode("append") \
    .saveAsTable("medical_insurance.gold.dim_drug_scd2")
