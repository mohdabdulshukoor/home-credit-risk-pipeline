{{
    config(
        materialized='incremental',
        unique_key='current_app_id',
        incremental_strategy='merge'
    )
}}

WITH bronze_data AS (
    SELECT * FROM {{ ref('bronze_application_train') }}
),

cleaned_data AS (
    SELECT
        CAST(SK_ID_CURR AS INTEGER) AS current_app_id,
        CAST(TARGET AS INTEGER) AS target,
        
        -- FIX: Adding this missing column back
        NAME_CONTRACT_TYPE AS contract_type,
        
        -- Macro Magic
        {{ fix_anomaly('CODE_GENDER', 'XNA') }} AS gender,
        
        -- Financials
        CAST(AMT_INCOME_TOTAL AS DECIMAL(18,2)) AS total_income,
        CAST(AMT_CREDIT AS DECIMAL(18,2)) AS loan_amount,
        
        -- Macro Magic
        {{ fix_anomaly('DAYS_EMPLOYED', 365243) }} AS days_employed
        
    FROM bronze_data
)

SELECT 
    *,
    -- GOAT Metric
    (loan_amount / NULLIF(total_income, 0)) AS credit_to_income_ratio
    
FROM cleaned_data

{% if is_incremental() %}
  -- Upsert Logic
  -- WHERE current_app_id NOT IN (SELECT current_app_id from {{ this }})
{% endif %}