# =============================================================================
# UHIP DWH — dim_doctor  |  SCD Type 2
# Source  : medical_insurance.silver.doctor_silver
# Tracked : department_key, hospital_key, employment_status, specialty
# NOTE    : Must run AFTER dim_hospital AND dim_department
# SCD2 / audit columns default as per project standard
# =============================================================================

spark.sql("""
    CREATE TABLE IF NOT EXISTS medical_insurance.gold.dim_doctor_scd2
    USING DELTA
    AS
    SELECT
        CAST(NULL AS BIGINT)      AS doctor_key,
        d.doctor_id,
        d.doctor_name,
        d.specialty,
        CAST(d.years_experience AS INT) AS years_experience,
        d.doctor_phone,
        d.employment_status,
        CAST(h.hospital_key AS BIGINT)     AS hospital_key,
        CAST(dep.department_key AS BIGINT) AS department_key,
        CAST(NULL AS DATE)        AS scd_start_date,
        CAST(NULL AS DATE)        AS scd_end_date,
        CAST(NULL AS INT)         AS scd_is_current,
        CAST(NULL AS INT)         AS scd_version,
        CAST(NULL AS TIMESTAMP)   AS dwh_created_at,
        CAST(NULL AS TIMESTAMP)   AS dwh_updated_at
    FROM medical_insurance.silver.doctor_silver d
    INNER JOIN medical_insurance.gold.dim_hospital_scd2 h
        ON  h.hospital_id    = d.hospital_id
        AND h.scd_is_current = 1
    INNER JOIN medical_insurance.gold.dim_department_scd2 dep
        ON  dep.department_id  = d.department_id
        AND dep.scd_is_current = 1
    WHERE 1 = 0
""")

# ── STEP 1: stage source — resolve hospital & department surrogates ───────────
query = """
    SELECT
        d.doctor_id,
        d.doctor_name,
        d.specialty,
        d.years_experience,
        d.doctor_phone,
        d.employment_status,
        h.hospital_key,
        dep.department_key
    FROM medical_insurance.silver.doctor_silver d
    INNER JOIN medical_insurance.gold.dim_hospital_scd2 h
        ON  h.hospital_id    = d.hospital_id
        AND h.scd_is_current = 1
    INNER JOIN medical_insurance.gold.dim_department_scd2 dep
        ON  dep.department_id  = d.department_id
        AND dep.scd_is_current = 1
"""

df_src = spark.sql(query)
df_src.createOrReplaceTempView("stg_doctor")

# ── STEP 2: expire changed current records ───────────────────────────────────
spark.sql("""
    MERGE INTO medical_insurance.gold.dim_doctor_scd2 AS tgt
    USING (
        SELECT s.*
        FROM stg_doctor s
        INNER JOIN medical_insurance.gold.dim_doctor_scd2 d
            ON  d.doctor_id      = s.doctor_id
            AND d.scd_is_current = 1
        WHERE
            d.department_key    <> s.department_key    OR
            d.hospital_key      <> s.hospital_key      OR
            d.employment_status <> s.employment_status OR
            d.specialty         <> s.specialty
    ) AS src
    ON  tgt.doctor_id      = src.doctor_id
    AND tgt.scd_is_current = 1
    WHEN MATCHED THEN UPDATE SET
        tgt.scd_end_date   = DATE_SUB(current_date(), 1),
        tgt.scd_is_current = 0,
        tgt.dwh_updated_at = current_timestamp()
""")

# ── STEP 3: insert new version rows + new doctors ─────────────────────────────
query_insert = """
    SELECT
        cast(row_number() OVER (
            PARTITION BY s.doctor_id
            ORDER BY scd_start_date DESC
        ) as BIGINT)          AS doctor_key,
        s.doctor_id,
        s.doctor_name,
        s.specialty,
        s.years_experience,
        s.doctor_phone,
        s.employment_status,
        s.hospital_key,
        s.department_key,
        current_date()                      AS scd_start_date,
        CAST(NULL AS DATE)                  AS scd_end_date,
        1                                   AS scd_is_current,
        COALESCE(mv.max_version, 0) + 1     AS scd_version,
        current_timestamp()                 AS dwh_created_at,
        current_timestamp()                 AS dwh_updated_at
    FROM stg_doctor s
    LEFT JOIN medical_insurance.gold.dim_doctor_scd2 active
        ON  active.doctor_id     = s.doctor_id
        AND active.scd_is_current = 1
    LEFT JOIN (
        SELECT doctor_id, MAX(scd_version) AS max_version
        FROM medical_insurance.gold.dim_doctor_scd2
        GROUP BY doctor_id
    ) mv ON mv.doctor_id = s.doctor_id
    WHERE active.doctor_id IS NULL
"""

df_insert = spark.sql(query_insert)

df_insert.write \
    .mode("append") \
    .saveAsTable("medical_insurance.gold.dim_doctor_scd2")
