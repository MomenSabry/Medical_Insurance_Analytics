# =============================================================================
# UHIP DWH — fact_claims  |  Incremental MERGE
# Source  : silver.claims, claim_items, claim_approvals
# Strategy: MERGE on claim_id — new claims inserted, Pending claims updated
#           when their approval decision arrives.
# Audit   : dwh_loaded_at / dwh_updated_at default to current_timestamp()
# =============================================================================

# ── STEP 0: create gold table if not exists ───────────────────────────────────
spark.sql("""
    CREATE TABLE IF NOT EXISTS medical_insurance.gold.fact_claims_incremental (
        claim_id            STRING,
        visit_id            STRING,
        date_key            INT,
        patient_key         BIGINT,
        hospital_key        BIGINT,
        procedure_code      STRING,
        drug_key            INT,
        claim_status_key    INT,
        claim_amount        DOUBLE,
        approved_amount     DOUBLE,
        approval_gap        DOUBLE,
        days_to_decision    INT,
        item_count          INT,
        reviewed_by         STRING,
        dwh_loaded_at       TIMESTAMP,
        dwh_updated_at      TIMESTAMP
    )
    USING DELTA
""")

# ── STEP 1: read watermark ────────────────────────────────────────────────────
wm = spark.sql("""
    SELECT last_loaded_date
    FROM medical_insurance.gold.etl_watermark
    WHERE table_name = 'fact_claims'
""").collect()[0]["last_loaded_date"]

# ── STEP 2: stage incoming claims (new + still-Pending from prior runs) ────────
query = f"""
    SELECT
        c.claim_id,
        c.visit_id,
        CAST(DATE_FORMAT(c.claim_date, 'yyyyMMdd') AS INT)      AS date_key,
        dp.patient_key,
        dh.hospital_key,
        proc_item.procedure_code,
        drug_item.drug_key,
        dcs.claim_status_key,
        c.claim_amount,
        c.approved_amount,
        c.claim_amount - COALESCE(c.approved_amount, 0)          AS approval_gap,
        DATEDIFF(ca.approval_date, c.claim_date)                 AS days_to_decision,
        COALESCE(item_agg.item_count, 0)                         AS item_count,
        ca.reviewed_by,
        current_timestamp()                                      AS dwh_loaded_at,
        current_timestamp()                                      AS dwh_updated_at,
        c.claim_date                                             AS claim_date -- for watermark advancement

    FROM medical_insurance.silver.claim_silver c

    -- dim_patient: version active at claim_date (SCD2 bracket)
    INNER JOIN medical_insurance.gold.dim_patient_scd2 dp
        ON  dp.patient_id     = c.patient_id
        and dp.scd_is_current = 1
      
    -- dim_hospital: version active at claim_date (SCD2 bracket)
    INNER JOIN medical_insurance.gold.dim_hospital_scd2 dh
        ON  dh.hospital_id    = c.hospital_id
        and dh.scd_is_current = 1

    -- dim_claim_status (SCD1 — always current)
    INNER JOIN medical_insurance.gold.dim_claim_status_scd1 dcs
        ON dcs.claim_status = c.claim_status
        
    -- Approval outcome
    LEFT JOIN medical_insurance.silver.claim_approval_silver ca
        ON ca.claim_id = c.claim_id

    -- Primary billed procedure (highest cost line item per claim)
    LEFT JOIN (
        SELECT
            ci.claim_id,
            dp2.procedure_code,
            Row_Number() OVER (PARTITION BY ci.claim_id ORDER BY ci.item_amount DESC) AS rn
        FROM medical_insurance.silver.claim_items_silver ci
        INNER JOIN medical_insurance.silver.medical_procedure_silver dp2
            ON dp2.procedure_code = ci.procedure_code
        WHERE ci.procedure_code IS NOT NULL
    ) proc_item ON proc_item.claim_id = c.claim_id AND proc_item.rn = 1

    -- Primary billed drug (highest cost line item per claim)
    LEFT JOIN (
        SELECT
            ci.claim_id,
            dd.drug_key,
            Row_Number() OVER (PARTITION BY ci.claim_id ORDER BY ci.item_amount DESC) AS rn
        FROM medical_insurance.silver.claim_items_silver ci
        INNER JOIN medical_insurance.gold.dim_drug_scd2 dd
            ON  dd.drug_id       = ci.drug_id
            AND dd.scd_is_current = 1
        WHERE ci.drug_id IS NOT NULL
    ) drug_item ON drug_item.claim_id = c.claim_id AND drug_item.rn = 1

    -- Total item count per claim
    LEFT JOIN (
        SELECT claim_id, COUNT(*) AS item_count
        FROM medical_insurance.silver.claim_items_silver
        GROUP BY claim_id
    ) item_agg ON item_agg.claim_id = c.claim_id

    -- Incremental: new claims + any still Pending from previous loads
    WHERE c.claim_date > '{wm}'
       OR c.claim_status = 'Pending Review'
"""

df_stg = spark.sql(query)
df_stg.createOrReplaceTempView("stg_claims")

# ── STEP 3: MERGE — insert new rows, update Pending that now have a decision ──
spark.sql("""
    MERGE INTO medical_insurance.gold.fact_claims_incremental AS tgt
    USING stg_claims AS src
    ON tgt.claim_id = src.claim_id

    -- Update previously Pending claims that now have an approval outcome
    WHEN MATCHED AND tgt.approved_amount IS NULL AND src.approved_amount IS NOT NULL
    THEN UPDATE SET
        tgt.approved_amount  = src.approved_amount,
        tgt.approval_gap     = src.approval_gap,
        tgt.days_to_decision = src.days_to_decision,
        tgt.claim_status_key = src.claim_status_key,
        tgt.reviewed_by      = src.reviewed_by,
        tgt.dwh_updated_at   = src.dwh_updated_at

    WHEN NOT MATCHED THEN INSERT (
        claim_id,
        visit_id,
        date_key,
        patient_key,
        hospital_key,
        procedure_code,
        drug_key,
        claim_status_key,
        claim_amount,
        approved_amount,
        approval_gap,
        days_to_decision,
        item_count,
        reviewed_by,
        dwh_loaded_at,
        dwh_updated_at
    ) VALUES (
        src.claim_id,
        src.visit_id,
        src.date_key,
        src.patient_key,
        src.hospital_key,
        src.procedure_code,
        src.drug_key,
        src.claim_status_key,
        src.claim_amount,
        src.approved_amount,
        src.approval_gap,
        src.days_to_decision,
        src.item_count,
        src.reviewed_by,
        src.dwh_loaded_at,
        src.dwh_updated_at
    )
""")

# ── STEP 4: advance watermark ONLY if data was loaded ─────────────────────────
loaded_df = spark.sql("""
    SELECT claim_date
    FROM stg_claims
""")
loaded_count = loaded_df.count()

if loaded_count > 0:
    max_loaded_date = loaded_df.agg({"claim_date": "max"}).collect()[0][0]
    
    spark.sql(f"""
    UPDATE medical_insurance.gold.etl_watermark
    SET last_loaded_date = '{max_loaded_date}'
    WHERE table_name = 'fact_claims'
    """)
    
    print(f"✓ Loaded {loaded_count} claims. Watermark updated to {max_loaded_date}")
else:
    print("⚠ No new claims to load. Watermark unchanged.")