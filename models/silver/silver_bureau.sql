{{
    config(
        materialized='incremental',
        unique_key='current_app_id',
        incremental_strategy='merge'
    )
}}

WITH bronze_bureau AS (
    -- Read from the Bronze Model (Medallion Architecture)
    SELECT * FROM {{ ref('bronze_bureau') }}
),

-- 1. CLEANING STEP
cleaned_bureau AS (
    SELECT
        CAST(SK_ID_CURR AS INTEGER) AS current_app_id,
        CAST(SK_ID_BUREAU AS INTEGER) AS bureau_loan_id,
        CREDIT_ACTIVE AS loan_status, -- Values like 'Active', 'Closed'
        
        -- Handling NULLs in financial columns (Senior Logic: NULL + 100 = NULL)
        -- We treat NULL debt as 0 to keep math safe.
        COALESCE(CAST(AMT_CREDIT_SUM AS DECIMAL(18,2)), 0) AS credit_amount,
        COALESCE(CAST(AMT_CREDIT_SUM_DEBT AS DECIMAL(18,2)), 0) AS current_debt
    FROM bronze_bureau
),

-- 2. AGGREGATION STEP (The "Squash")
aggregated_bureau AS (
    SELECT
        current_app_id,
        
        -- Metric 1: How many loans has he taken in the past?
        COUNT(bureau_loan_id) AS total_bureau_loans_count,
        
        -- Metric 2: How many are currently Active? (Risk Signal)
        -- We use conditional aggregation (CASE WHEN)
        SUM(CASE WHEN loan_status = 'Active' THEN 1 ELSE 0 END) AS total_active_loans,
        
        -- Metric 3: Total Debt across market
        SUM(current_debt) AS total_bureau_debt,
        
        -- Metric 4: Total Credit Limit (Exposure)
        SUM(credit_amount) AS total_bureau_credit_limit

    FROM cleaned_bureau
    GROUP BY current_app_id -- This ensures 1 row per Customer
)

SELECT * FROM aggregated_bureau

{% if is_incremental() %}
  -- Upsert Logic
  -- WHERE current_app_id NOT IN (SELECT current_app_id from {{ this }})
{% endif %}