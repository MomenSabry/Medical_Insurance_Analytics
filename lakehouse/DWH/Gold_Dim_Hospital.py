# =============================================================================
# UHIP DWH — dim_hospital  |  SCD Type 2
# Source  : medical_insurance.silver.hospital_silver
# Tracked : total_beds, icu_capacity, hospital_type, manager_name
# =============================================================================

# ── CREATE TABLE (run safely on every execution) ─────────────────────────────
spark.sql("""
    CREATE TABLE IF NOT EXISTS medical_insurance.gold.dim_hospital_scd2
    USING DELTA
    AS
    SELECT
        CAST(NULL AS BIGINT)        AS hospital_key,
        hospital_id,
        hospital_name,
        hospital_type,
        governorate,
        district,
        hospital_phone,
        CAST(total_beds   AS INT)   AS total_beds,
        CAST(icu_capacity AS INT)   AS icu_capacity,
        CAST(latitude  AS DECIMAL(9,6))  AS latitude,   -- match table type exactly
        CAST(longitude AS DECIMAL(9,6))  AS longitude,  -- match table type exactly
        manager_name,
        manager_email,
        manager_phone,
        CAST(NULL AS DATE)          AS scd_start_date,
        CAST(NULL AS DATE)          AS scd_end_date,
        CAST(NULL AS INT)           AS scd_is_current,
        CAST(NULL AS INT)           AS scd_version,
        CAST(NULL AS TIMESTAMP)     AS dwh_created_at,
        CAST(NULL AS TIMESTAMP)     AS dwh_updated_at
    FROM medical_insurance.silver.hospital_silver
    WHERE 1 = 0
""")

# ── STEP 1: stage source ──────────────────────────────────────────────────────
query = """
    SELECT
        hospital_id,
        hospital_name,
        hospital_type,
        governorate,
        district,
        hospital_phone,
        total_beds,
        icu_capacity,
        latitude,
        longitude,
        manager_name,
        manager_email,
        manager_phone
    FROM medical_insurance.silver.hospital_silver
"""

df_src = spark.sql(query)
df_src.createOrReplaceTempView("stg_hospital")

# ── STEP 2: expire changed current records ────────────────────────────────────
spark.sql("""
    MERGE INTO medical_insurance.gold.dim_hospital_scd2 AS tgt
    USING (
        SELECT s.*
        FROM stg_hospital s
        INNER JOIN medical_insurance.gold.dim_hospital_scd2 d
            ON  d.hospital_id    = s.hospital_id
            AND d.scd_is_current = 1
        WHERE
            d.total_beds    <> s.total_beds    OR
            d.icu_capacity  <> s.icu_capacity  OR
            d.hospital_type <> s.hospital_type OR
            COALESCE(d.manager_name, '') <> COALESCE(s.manager_name, '')
    ) AS src
    ON  tgt.hospital_id    = src.hospital_id
    AND tgt.scd_is_current = 1
    WHEN MATCHED THEN UPDATE SET
        tgt.scd_end_date   = DATE_SUB(current_date(), 1),
        tgt.scd_is_current = 0,
        tgt.dwh_updated_at = current_timestamp()
""")

# ── STEP 3: insert new version rows + new hospitals ───────────────────────────
query_insert = """
    SELECT
        cast(row_number() OVER (
            ORDER BY s.hospital_id 
        )  as BIGINT)       AS hospital_key,
        s.hospital_id,
        s.hospital_name,
        s.hospital_type,
        s.governorate,
        s.district,
        s.hospital_phone,
        s.total_beds,
        s.icu_capacity,
        CAST(s.latitude AS DECIMAL(9,6))              AS latitude,
        CAST(s.longitude AS DECIMAL(9,6))             AS longitude,
        s.manager_name,
        s.manager_email,
        s.manager_phone,
        current_date()                      AS scd_start_date,
        CAST(NULL AS DATE)                  AS scd_end_date,
        1                                   AS scd_is_current,
        COALESCE(mv.max_version, 0) + 1     AS scd_version,
        current_timestamp()                 AS dwh_created_at,
        current_timestamp()                 AS dwh_updated_at
    FROM stg_hospital s
    LEFT JOIN medical_insurance.gold.dim_hospital_scd2 active
        ON  active.hospital_id    = s.hospital_id
        AND active.scd_is_current = 1
    LEFT JOIN (
        SELECT hospital_id, MAX(scd_version) AS max_version
        FROM medical_insurance.gold.dim_hospital_scd2
        GROUP BY hospital_id
    ) mv ON mv.hospital_id = s.hospital_id
    WHERE active.hospital_id IS NULL
"""

df_insert = spark.sql(query_insert)

df_insert.write \
    .mode("append") \
    .saveAsTable("medical_insurance.gold.dim_hospital_scd2")
