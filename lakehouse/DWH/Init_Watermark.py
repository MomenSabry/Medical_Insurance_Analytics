# =============================================================================
# UHIP DWH — etl_watermark  |  One-time initialisation
# Run this ONCE before any fact script executes on a fresh warehouse.
# =============================================================================

# ── Create watermark control table ───────────────────────────────────────────
spark.sql("""
    CREATE TABLE IF NOT EXISTS medical_insurance.gold.etl_watermark (
        table_name       STRING  NOT NULL,
        last_loaded_date DATE    NOT NULL
    )
    USING DELTA
""")

# ── Seed initial watermarks (day before study period start) ──────────────────
query = """
    SELECT table_name, last_loaded_date
    FROM (
        VALUES
            ('fact_visits',   DATE '2024-05-08'),
            ('fact_claims',   DATE '2024-05-08'),
            ('fact_pharmacy', DATE '2024-05-08')
    ) AS t(table_name, last_loaded_date)
"""

df = spark.sql(query)

df.write \
    .mode("append") \
    .saveAsTable("medical_insurance.gold.etl_watermark")

df.display()