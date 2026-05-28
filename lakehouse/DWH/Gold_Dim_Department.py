# =============================================================================
# UHIP DWH — dim_department  |  SCD Type 2
# Source  : medical_insurance.silver.department_silver
# Tracked : department_name, floor_number, hospital_key
# NOTE    : Must run AFTER dim_hospital
# =============================================================================

# ── CREATE TABLE (run safely on every execution) ─────────────────────────────
spark.sql("""
    CREATE TABLE IF NOT EXISTS medical_insurance.gold.dim_department_scd2
    USING DELTA
    AS
    SELECT
        CAST(NULL AS BIGINT)    AS department_key,
        d.department_id,
        d.department_name,
        d.floor_number,
        CAST(h.hospital_key AS BIGINT) AS hospital_key,
        CAST(NULL AS DATE)      AS scd_start_date,
        CAST(NULL AS DATE)      AS scd_end_date,
        CAST(NULL AS INT)       AS scd_is_current,
        CAST(NULL AS INT)       AS scd_version,
        CAST(NULL AS TIMESTAMP) AS dwh_created_at,
        CAST(NULL AS TIMESTAMP) AS dwh_updated_at
    FROM medical_insurance.silver.department_silver d
    INNER JOIN medical_insurance.gold.dim_hospital_scd2 h
        ON  h.hospital_id    = d.hospital_id
        AND h.scd_is_current = 1
    WHERE 1 = 0
""")

# ── STEP 1: stage source — resolve hospital surrogate ─────────────────────────
query = """
    SELECT
        d.department_id,
        d.department_name,
        d.floor_number,
        h.hospital_key
    FROM medical_insurance.silver.department_silver d
    INNER JOIN medical_insurance.gold.dim_hospital_scd2 h
        ON  h.hospital_id    = d.hospital_id
        AND h.scd_is_current = 1
"""

df_src = spark.sql(query)
df_src.createOrReplaceTempView("stg_department")

# ── STEP 2: expire changed current records ────────────────────────────────────
spark.sql("""
    MERGE INTO medical_insurance.gold.dim_department_scd2 AS tgt
    USING (
        SELECT s.*
        FROM stg_department s
        INNER JOIN medical_insurance.gold.dim_department_scd2 d
            ON  d.department_id  = s.department_id
            AND d.scd_is_current = 1
        WHERE
            d.department_name <> s.department_name OR
            d.floor_number    <> s.floor_number    OR
            d.hospital_key    <> s.hospital_key
    ) AS src
    ON  tgt.department_id  = src.department_id
    AND tgt.scd_is_current = 1
    WHEN MATCHED THEN UPDATE SET
        tgt.scd_end_date   = DATE_SUB(current_date(), 1),
        tgt.scd_is_current = 0,
        tgt.dwh_updated_at = current_timestamp()
""")

# ── STEP 3: insert new version rows + new departments ────────────────────────
query_insert = """
    SELECT
        cast(row_number() OVER (
            ORDER BY s.department_id 
        )  as BIGINT)            AS department_key,
        s.department_id,
        s.department_name,
        s.floor_number,
        s.hospital_key,
        current_date()                      AS scd_start_date,
        CAST(NULL AS DATE)                  AS scd_end_date,
        1                                   AS scd_is_current,
        COALESCE(mv.max_version, 0) + 1     AS scd_version,
        current_timestamp()                 AS dwh_created_at,
        current_timestamp()                 AS dwh_updated_at
    FROM stg_department s
    LEFT JOIN medical_insurance.gold.dim_department_scd2 active
        ON  active.department_id  = s.department_id
        AND active.scd_is_current = 1
    LEFT JOIN (
        SELECT department_id, MAX(scd_version) AS max_version
        FROM medical_insurance.gold.dim_department_scd2
        GROUP BY department_id
    ) mv ON mv.department_id = s.department_id
    WHERE active.department_id IS NULL
"""

df_insert = spark.sql(query_insert)

df_insert.write \
    .mode("append") \
    .saveAsTable("medical_insurance.gold.dim_department_scd2")
