# =============================================================================
# UHIP DWH — dim_patient  |  SCD Type 2
# Source  : medical_insurance.silver.patient_silver
# Tracked : phone, street, city, emergency_contact
# SCD2 / audit columns default as per project standard
# =============================================================================
spark.sql("""
    CREATE TABLE IF NOT EXISTS medical_insurance.gold.dim_patient_scd2 (
        patient_key         BIGINT,
        patient_id          STRING,
        national_id         STRING,
        patient_name        STRING,
        gender              STRING,
        birth_date          DATE,
        age_group           STRING,
        patient_phone       STRING,
        street              STRING,
        city                STRING,
        governorate         STRING,
        blood_type          STRING,
        emergency_contact   STRING,
        scd_start_date      DATE,
        scd_end_date        DATE,
        scd_is_current      INT,
        scd_version         INT,
        dwh_created_at      TIMESTAMP,
        dwh_updated_at      TIMESTAMP
    )
    USING DELTA
""")
# ── STEP 1: stage source ─────────────────────────────────────────────────────
query = """
    SELECT
        patient_id,
        national_id,
        patient_name,
        gender,
        birth_date,
        CASE
            WHEN DATEDIFF(current_date(), birth_date) / 365.25 < 18  THEN 'Child'
            WHEN DATEDIFF(current_date(), birth_date) / 365.25 < 60  THEN 'Adult'
            ELSE 'Senior'
        END         AS age_group,
        patient_phone,
        street,
        city,
        governorate,
        blood_type,
        emergency_contact
    FROM medical_insurance.silver.patient_silver
"""

df_src = spark.sql(query)
df_src.createOrReplaceTempView("stg_patient")

# ── STEP 2: expire changed current records ───────────────────────────────────
spark.sql("""
    MERGE INTO medical_insurance.gold.dim_patient_scd2 AS tgt
    USING (
        SELECT s.*
        FROM stg_patient s
        INNER JOIN medical_insurance.gold.dim_patient_scd2 d
            ON  d.patient_id     = s.patient_id
            AND d.scd_is_current = 1
        WHERE
            COALESCE(d.patient_phone,             '') <> COALESCE(s.patient_phone,             '') OR
            COALESCE(d.street,            '') <> COALESCE(s.street,            '') OR
            COALESCE(d.city,              '') <> COALESCE(s.city,              '') OR
            COALESCE(d.emergency_contact, '') <> COALESCE(s.emergency_contact, '')
    ) AS src
    ON  tgt.patient_id     = src.patient_id
    AND tgt.scd_is_current = 1
    WHEN MATCHED THEN UPDATE SET
        tgt.scd_end_date   = DATE_SUB(current_date(), 1),
        tgt.scd_is_current = 0,
        tgt.dwh_updated_at = current_timestamp()
""")

# ── STEP 3: insert new version rows + new patients ────────────────────────────
query_insert = """
    SELECT
        CAST(Row_Number() OVER (ORDER BY s.patient_id) AS BIGINT)  AS patient_key,
        s.patient_id,
        CAST(s.national_id AS STRING) AS national_id,
        s.patient_name,
        s.gender,
        s.birth_date,
        s.age_group,
        s.patient_phone,
        s.street,
        s.city,
        s.governorate,
        s.blood_type,
        s.emergency_contact,
        current_date()                      AS scd_start_date,
        CAST(NULL AS DATE)                  AS scd_end_date,
        1                                   AS scd_is_current,
        COALESCE(mv.max_version, 0) + 1     AS scd_version,
        current_timestamp()                 AS dwh_created_at,
        current_timestamp()                 AS dwh_updated_at
    FROM stg_patient s
    LEFT JOIN medical_insurance.gold.dim_patient_scd2 active
        ON  active.patient_id    = s.patient_id
        AND active.scd_is_current = 1
    LEFT JOIN (
        SELECT patient_id, MAX(scd_version) AS max_version
        FROM medical_insurance.gold.dim_patient_scd2
        GROUP BY patient_id
    ) mv ON mv.patient_id = s.patient_id
    WHERE active.patient_id IS NULL
"""

df_insert = spark.sql(query_insert)

df_insert.write \
    .mode("append") \
    .saveAsTable("medical_insurance.gold.dim_patient_scd2")
