# =============================================================================
# UHIP DWH — dim_date  |  SCD Type 1  |  Full Reload
# Source : Generated date spine (no OLTP source table)
# Audit  : dwh_created_at / dwh_updated_at default to current_timestamp()
# =============================================================================

query = """
    SELECT
        CAST(DATE_FORMAT(full_date, 'yyyyMMdd') AS INT)          AS date_key,
        full_date,
        YEAR(full_date)                                          AS year,
        QUARTER(full_date)                                       AS quarter,
        MONTH(full_date)                                         AS month,
        DATE_FORMAT(full_date, 'MMMM')                          AS month_name,
        WEEKOFYEAR(full_date)                                    AS week,
        DAY(full_date)                                           AS day_of_month,
        CASE DAYOFWEEK(full_date)
            WHEN 1 THEN 7
            ELSE DAYOFWEEK(full_date) - 1
        END                                                      AS day_of_week,
        DATE_FORMAT(full_date, 'EEEE')                           AS day_name,
        CASE WHEN DAYOFWEEK(full_date) IN (1, 7) THEN 1 ELSE 0
        END                                                      AS is_weekend,
        CASE
            WHEN MONTH(full_date) IN (12, 1, 2)  THEN 'Winter'
            WHEN MONTH(full_date) IN (3,  4, 5)  THEN 'Spring'
            WHEN MONTH(full_date) IN (6,  7, 8)  THEN 'Summer'
            ELSE                                       'Autumn'
        END                                                      AS season,
        current_timestamp()                                      AS dwh_created_at,
        current_timestamp()                                      AS dwh_updated_at
    FROM (
        SELECT EXPLODE(
            SEQUENCE(DATE '2024-01-01', DATE '2026-12-31', INTERVAL 1 DAY)
        ) AS full_date
    )
"""

df = spark.sql(query)

df.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("medical_insurance.gold.dim_date_scd1")
