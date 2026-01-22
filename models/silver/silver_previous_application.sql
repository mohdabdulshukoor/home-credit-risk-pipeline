{{
    config(
        materialized='incremental',
        unique_key='current_app_id',
        incremental_strategy='merge'
    )
}}

WITH bronze_prev AS (
    SELECT * FROM {{ ref('bronze_previous_application') }}
),

cleaned_prev AS (
    SELECT
        CAST(SK_ID_CURR AS INTEGER) AS current_app_id,
        CAST(SK_ID_PREV AS INTEGER) AS prev_app_id,
        NAME_CONTRACT_STATUS AS contract_status, -- 'Approved', 'Refused', 'Canceled'
        CAST(AMT_APPLICATION AS DECIMAL(18,2)) AS amt_applied,
        CAST(AMT_CREDIT AS DECIMAL(18,2)) AS amt_granted
    FROM bronze_prev
),

-- AGGREGATION (Squashing History)
aggregated_prev AS (
    SELECT
        current_app_id,
        
        -- Metric 1: How many times have they tried to borrow from us?
        COUNT(prev_app_id) AS total_prev_attempts,
        
        -- Metric 2: The "Desperation" Metric (GOAT Logic)
        -- High refusals = High Risk.
        SUM(CASE WHEN contract_status = 'Refused' THEN 1 ELSE 0 END) AS total_refused_apps,
        
        -- Metric 3: The "Loyalty" Metric
        SUM(CASE WHEN contract_status = 'Approved' THEN 1 ELSE 0 END) AS total_approved_apps,
        
        -- Metric 4: How much money have we given them in the past?
        SUM(CASE WHEN contract_status = 'Approved' THEN amt_granted ELSE 0 END) AS total_internal_debt_granted

    FROM cleaned_prev
    GROUP BY current_app_id
)

SELECT * FROM aggregated_prev

{% if is_incremental() %}
  -- Upsert Logic
  -- WHERE current_app_id NOT IN (SELECT current_app_id from {{ this }})
{% endif %}