# =============================================================================
# UHIP DWH — fact_visits  |  Incremental Append
# Source  : silver.visits, visit_procedures, prescriptions, patient_feedback,
#           referrals
# Strategy: Load visits with visit_date > watermark. SCD2 keys resolved
#           by date-bracketing against scd_start_date / scd_end_date.
# Audit   : dwh_loaded_at defaults to current_timestamp()
# =============================================================================

# ── STEP 1: read watermark ────────────────────────────────────────────────────
wm = spark.sql("""
    SELECT last_loaded_date
    FROM medical_insurance.gold.etl_watermark
    WHERE table_name = 'fact_visits'
""").collect()[0]["last_loaded_date"]

# ── STEP 2: build incremental fact query ──────────────────────────────────────
query = f"""
    SELECT
        v.visit_id,

        -- Dimension keys
        CAST(DATE_FORMAT(v.visit_date, 'yyyyMMdd') AS INT)       AS date_key,
        dp.patient_key,
        dh.hospital_key,
        ddoc.doctor_key,
        ddep.department_key,
        ddiag.diagnosis_key,
        v.visit_type,

        -- Measures
        v.total_amount                                            AS total_cost,
        v.waiting_time,
        COALESCE(proc_agg.procedure_count, 0)                    AS procedure_count,
        CASE WHEN rx.visit_id IS NOT NULL THEN 1 ELSE 0 END      AS prescription_flag,
        fb.avg_rating                                             AS feedback_rating,
        v.visit_status,

        -- Degenerate dimension
        ref.referral_reason,

        -- Visit date for watermark tracking
        v.visit_date,

        -- Audit
        current_timestamp()                                       AS dwh_loaded_at

    FROM medical_insurance.silver.visit_silver v

    -- dim_patient: version active at visit_date (SCD2 bracket)
    INNER JOIN medical_insurance.gold.dim_patient_scd2 dp
        ON  dp.patient_id     = v.patient_id
        and scd_is_current = 1

    -- dim_hospital: version active at visit_date (SCD2 bracket)
    INNER JOIN medical_insurance.gold.dim_hospital_scd2 dh
        ON  dh.hospital_id    = v.hospital_id
        and scd_is_current = 1

    -- dim_doctor: version active at visit_date (SCD2 bracket)
    INNER JOIN medical_insurance.gold.dim_doctor_scd2 ddoc
        ON  ddoc.doctor_id    = v.doctor_id
       and scd_is_current = 1

    -- dim_department: version active at visit_date (SCD2 bracket)
    INNER JOIN medical_insurance.gold.dim_department_scd2 ddep
        ON  ddep.department_id  = v.department_id
        and scd_is_current = 1

    -- dim_diagnosis (SCD1 — always current)
    INNER JOIN medical_insurance.gold.dim_diagnosis_scd1 ddiag
        ON ddiag.diagnosis_code = v.diagnosis_code

    -- Procedure count per visit
    LEFT JOIN (
        SELECT visit_id, COUNT(*) AS procedure_count
        FROM medical_insurance.silver.visit_procedure_silver
        GROUP BY visit_id
    ) proc_agg ON proc_agg.visit_id = v.visit_id

    -- Prescription flag
    LEFT JOIN (
        SELECT DISTINCT visit_id
        FROM medical_insurance.silver.prescription_silver
    ) rx ON rx.visit_id = v.visit_id

    -- Average feedback rating per visit
    LEFT JOIN (
        SELECT patient_id, hospital_id, doctor_id, AVG(CAST(rating AS DECIMAL(3,1))) AS avg_rating
        FROM medical_insurance.silver.patient_feedback_silver
        GROUP BY patient_id, hospital_id, doctor_id
    ) fb ON fb.patient_id = v.patient_id
        AND fb.hospital_id = v.hospital_id
        AND fb.doctor_id = v.doctor_id

    -- Referral reason (first referral linked to this visit)
    LEFT JOIN (
        SELECT patient_id, from_hospital_id, FIRST(referral_reason) AS referral_reason
        FROM medical_insurance.silver.referral_silver
        GROUP BY patient_id, from_hospital_id
    ) ref ON ref.patient_id = v.patient_id
        AND ref.from_hospital_id = v.hospital_id

    WHERE v.visit_date > '{wm}'
"""

df = spark.sql(query)

# ── STEP 3: write to target table ─────────────────────────────────────────────
df.write \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable("medical_insurance.gold.fact_visits_incremental")

# ── STEP 4: advance watermark ONLY if data was loaded ─────────────────────────
# Get the max visit_date from what was actually loaded (not from source table)
loaded_count = df.count()

if loaded_count > 0:
    max_loaded_date = df.agg({"visit_date": "max"}).collect()[0][0]
    
    spark.sql(f"""
    UPDATE medical_insurance.gold.etl_watermark
    SET last_loaded_date = '{max_loaded_date}'
    WHERE table_name = 'fact_visits'
    """)
    
    print(f"✓ Loaded {loaded_count} visits. Watermark updated to {max_loaded_date}")
else:
    print("⚠ No new visits to load. Watermark unchanged.")
