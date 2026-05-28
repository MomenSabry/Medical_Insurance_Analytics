# =============================================================================
# UHIP DWH — fact_pharmacy  |  Incremental Append
# Source  : silver.drug_transactions, drug_inventory, suppliers
# Strategy: Append new transactions beyond watermark. Fraud flag
#           (dispensing with no matching prescription) computed at load time.
# Audit   : dwh_loaded_at defaults to current_timestamp()
# =============================================================================

# ── STEP 1: read watermark ────────────────────────────────────────────────────
wm = spark.sql("""
    SELECT last_loaded_date
    FROM medical_insurance.gold.etl_watermark
    WHERE table_name = 'fact_pharmacy'
""").collect()[0]["last_loaded_date"]

# ── STEP 2: build incremental fact query ──────────────────────────────────────
query = f"""
    SELECT
        dt.transaction_id,

        -- Dimension keys
        CAST(DATE_FORMAT(dt.transaction_date, 'yyyyMMdd') AS INT) AS date_key,
        dd.drug_key,
        dh.hospital_key,
        ds.supplier_key,

        -- Measures
        dt.transaction_type,
        dt.quantity,
        dr.unit_price,
        dt.quantity * dr.unit_price                               AS total_value,

        -- Stockout flag: stock falls to 0 after this dispensing/wastage
        CASE
            WHEN dt.transaction_type IN ('Dispensing', 'Wastage', 'Return')
             AND inv.quantity_available - dt.quantity <= 0
            THEN 1 ELSE 0
        END                                                       AS stockout_flag,

        -- Near-expiry flag: batch expires within 60 days of transaction
        CASE
            WHEN DATEDIFF(inv.expiration_date, dt.transaction_date) <= 60
            THEN 1 ELSE 0
        END                                                       AS near_expiry_flag,

        -- Fraud flag: dispensing with no matching prescription on same day
        CASE
            WHEN dt.transaction_type = 'Dispensing'
             AND NOT EXISTS (
                 SELECT 1
                 FROM medical_insurance.silver.prescription_items_silver pi
                 INNER JOIN medical_insurance.silver.prescription_silver px
                     ON px.prescription_id = pi.prescription_id
                 INNER JOIN olist_workspace.silver.visits v
                     ON v.visit_id = px.visit_id
                 WHERE pi.drug_id    = dt.drug_id
                   AND v.hospital_id = dt.hospital_id
                   AND v.visit_date  = dt.transaction_date
             )
            THEN 1 ELSE 0
        END                                                       AS no_prescription_flag,

        -- Degenerate dimension
        dt.performed_by,

        -- Audit
        current_timestamp()                                       AS dwh_loaded_at

    FROM medical_insurance.silver.drug_transaction_silver dt

    -- dim_drug: version active at transaction_date (SCD2 bracket)
    INNER JOIN olist_workspace.gold.Dim_drug dd
        ON  dd.drug_id        = dt.drug_id
        AND dd.scd_start_date <= dt.transaction_date
        AND (dd.scd_end_date  >= dt.transaction_date OR dd.scd_end_date IS NULL)

    -- Current drug price from silver (price at time of transaction)
    INNER JOIN medical_insurance.silver.drug_silver dr
        ON dr.drug_id = dt.drug_id

    -- dim_hospital: version active at transaction_date (SCD2 bracket)
    INNER JOIN olist_workspace.gold.Dim_hospital dh
        ON  dh.hospital_id    = dt.hospital_id
        AND dh.scd_start_date <= dt.transaction_date
        AND (dh.scd_end_date  >= dt.transaction_date OR dh.scd_end_date IS NULL)

    -- drug_inventory snapshot for stock/expiry flag computation
    LEFT JOIN medical_insurance.silver.drug_inventory_silver inv
        ON  inv.hospital_id = dt.hospital_id
        AND inv.drug_id     = dt.drug_id

    -- Incremental filter + idempotency guard
    WHERE dt.transaction_date > '{wm}'
"""

df = spark.sql(query)

df.write \
    .mode("append") \
    .saveAsTable("medical_insurance.gold.fact_pharmacy_incremental")

# ── STEP 3: advance watermark ─────────────────────────────────────────────────
spark.sql("""
    MERGE INTO medical_insurance.gold.etl_watermark AS tgt
    USING (
        SELECT
            'fact_pharmacy'          AS table_name,
            MAX(transaction_date)    AS last_loaded_date
        FROM medical_insurance.silver.drug_transaction_silver
    ) AS src
    ON tgt.table_name = src.table_name
    WHEN MATCHED AND src.last_loaded_date IS NOT NULL THEN UPDATE SET
        tgt.last_loaded_date = src.last_loaded_date
""")
