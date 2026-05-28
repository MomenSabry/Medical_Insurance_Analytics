# =============================================================================
# UHIP DWH — fact_pharmacy  |  Incremental Append
# =============================================================================

# ── STEP 1: Read watermark ────────────────────────────────────────────────────
wm_row = spark.sql("""
    SELECT last_loaded_date
    FROM medical_insurance.gold.etl_watermark
    WHERE table_name = 'fact_pharmacy'
""").collect()

wm = wm_row[0]["last_loaded_date"] if wm_row else "1900-01-01"
print(f"📌 Watermark: {wm}")

# ── STEP 2: Build & execute incremental fact query ────────────────────────────
df = spark.sql(f"""
    SELECT
        dt.transaction_id,
        dt.transaction_date,                                           -- ✅ kept

        -- Dimension keys
        CAST(DATE_FORMAT(dt.transaction_date, 'yyyyMMdd') AS INT)  AS date_key,
        dd.drug_key,
        dh.hospital_key,

        -- Measures
        dt.transaction_type,
        dt.quantity,
        dr.unit_amount,
        dt.quantity * dr.unit_amount                               AS total_value,

        -- Stockout flag
        CASE
            WHEN dt.transaction_type IN ('Dispensing', 'Wastage', 'Return')
             AND inv.quantity_available - dt.quantity <= 0
            THEN 1 ELSE 0
        END                                                        AS stockout_flag,

        -- Near-expiry flag
        CASE
            WHEN DATEDIFF(inv.expiration_date, dt.transaction_date) <= 60
            THEN 1 ELSE 0
        END                                                        AS near_expiry_flag,

        -- Fraud flag
        CASE
            WHEN dt.transaction_type = 'Dispensing'
             AND NOT EXISTS (
                 SELECT 1
                 FROM medical_insurance.silver.prescription_items_silver pi
                 INNER JOIN medical_insurance.silver.prescription_silver  px
                     ON px.prescription_id = pi.prescription_id
                 INNER JOIN medical_insurance.silver.visit_silver         v
                     ON v.visit_id = px.visit_id
                 WHERE pi.drug_id    = dt.drug_id
                   AND v.hospital_id = dt.hospital_id
                   AND v.visit_date  = dt.transaction_date
             )
            THEN 1 ELSE 0
        END                                                        AS no_prescription_flag,

        dt.performed_by,
        current_timestamp()                                        AS dwh_loaded_at

    FROM medical_insurance.silver.drug_transaction_silver dt

    INNER JOIN medical_insurance.gold.dim_drug_scd2 dd
        ON  dd.drug_id        = dt.drug_id
        and dd.scd_is_current = 1

    INNER JOIN medical_insurance.silver.drug_silver dr
        ON dr.drug_id = dt.drug_id

    INNER JOIN medical_insurance.gold.dim_hospital_scd2 dh
        ON  dh.hospital_id    = dt.hospital_id
        and dh.scd_is_current = 1

    LEFT JOIN medical_insurance.silver.drug_inventory_silver inv
        ON  inv.hospital_id = dt.hospital_id
        AND inv.drug_id     = dt.drug_id

    WHERE dt.transaction_date > '{wm}'
""")

# ── STEP 3: Recreate table & write ───────────────────────────────────────────
loaded_count = df.count()

if loaded_count == 0:
    print("⚠️  No new transactions found. Watermark unchanged.")
else:
    max_loaded_date = df.agg({"transaction_date": "max"}).collect()[0][0]

    # ✅ Recreate table from scratch on first load, append on subsequent runs
    spark.sql("DROP TABLE IF EXISTS medical_insurance.gold.fact_pharmacy_incremental")

    df.write \
      .mode("overwrite") \
      .option("overwriteSchema", "true") \
      .saveAsTable("medical_insurance.gold.fact_pharmacy_incremental")

    # ── STEP 4: Advance watermark ─────────────────────────────────────────────
    spark.sql(f"""
        UPDATE medical_insurance.gold.etl_watermark
        SET   last_loaded_date = '{max_loaded_date}'
        WHERE  table_name       = 'fact_pharmacy'
    """)

    print(f"✅ Loaded {loaded_count:,} transactions. Watermark → {max_loaded_date}")

   