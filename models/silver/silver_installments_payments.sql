{{
    config(
        materialized='incremental',
        unique_key='current_app_id',
        incremental_strategy='merge'
    )
}}

WITH bronze_installments AS (
    SELECT * FROM {{ ref('bronze_installments_payments') }}
),

calc_lateness AS (
    SELECT
        CAST(SK_ID_CURR AS INTEGER) AS current_app_id,
        CAST(DAYS_INSTALMENT AS DECIMAL(18,2)) AS date_due,
        CAST(DAYS_ENTRY_PAYMENT AS DECIMAL(18,2)) AS date_paid,
        CAST(AMT_INSTALMENT AS DECIMAL(18,2)) AS amount_due,
        CAST(AMT_PAYMENT AS DECIMAL(18,2)) AS amount_paid,
        
        -- ROW LEVEL CALCULATION:
        -- Did they pay late? (Paid Date > Due Date)
        CASE 
            WHEN DAYS_ENTRY_PAYMENT > DAYS_INSTALMENT THEN 1 
            ELSE 0 
        END AS is_late_payment,
        
        -- Did they pay less than required?
        CASE 
            WHEN AMT_PAYMENT < AMT_INSTALMENT THEN 1 
            ELSE 0 
        END AS is_underpayment

    FROM bronze_installments
),

-- AGGREGATION
payment_behavior AS (
    SELECT
        current_app_id,
        
        -- Metric 1: Total Late Payments (The "Red Flag")
        SUM(is_late_payment) AS total_late_payments,
        
        -- Metric 2: Total Underpayments
        SUM(is_underpayment) AS total_underpayments,
        
        -- Metric 3: Total Missed/Underpaid Amount
        -- (Amount Due - Amount Paid)
        SUM(amount_due - amount_paid) AS total_outstanding_balance

    FROM calc_lateness
    GROUP BY current_app_id
)

SELECT * FROM payment_behavior

{% if is_incremental() %}
  -- Upsert Logic
  -- WHERE current_app_id NOT IN (SELECT current_app_id from {{ this }})
{% endif %}