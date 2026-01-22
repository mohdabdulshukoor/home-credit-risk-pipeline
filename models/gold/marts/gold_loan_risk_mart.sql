{{
    config(
        materialized='table',
        schema='gold'
    )
}}

/* 
   ======================================================================
   GOAT LEVEL: METADATA-DRIVEN PIPELINE (Jinja + SQL)
   ======================================================================
   Instead of writing "COALESCE(x, 0)" 30 times, we define columns in lists.
   The code writes itself.
*/

-- 1. Metadata for Bureau Metrics (External Risk)
{% set bureau_cols = [
    'total_bureau_loans_count',
    'total_active_loans',
    'total_bureau_debt',
    'total_bureau_credit_limit'
] %}

-- 2. Metadata for Previous Application Metrics (Internal History)
{% set prev_app_cols = [
    'total_prev_attempts',
    'total_refused_apps',
    'total_approved_apps',
    'total_internal_debt_granted'
] %}

-- 3. Metadata for Payment Behavior (The "Red Flag" metrics)
{% set payment_cols = [
    'total_late_payments',
    'total_underpayments',
    'total_outstanding_balance'
] %}

WITH 
-- Import CTEs (referencing our Silver Models)
app_train AS ( SELECT * FROM {{ ref('silver_application_train') }} ),
bureau    AS ( SELECT * FROM {{ ref('silver_bureau') }} ),
prev_app  AS ( SELECT * FROM {{ ref('silver_previous_application') }} ),
payments  AS ( SELECT * FROM {{ ref('silver_installments_payments') }} ),

final_dataset AS (
    SELECT
        -- A. Identifiers & Demographics (From Base Table)
        app.current_app_id,
        app.target,
        app.contract_type,
        app.gender,
        app.total_income,
        app.loan_amount,
        app.credit_to_income_ratio,
        app.days_employed,
        
        -- B. Bureau Loop (Metadata Driven)
        -- Auto-generates: COALESCE(bur.col, 0) AS col
        {% for col in bureau_cols %}
            COALESCE(bur.{{ col }}, 0) AS {{ col }},
        {% endfor %}

        -- C. Previous App Loop
        {% for col in prev_app_cols %}
            COALESCE(prev.{{ col }}, 0) AS {{ col }},
        {% endfor %}

        -- D. Payment Loop
        -- 'loop.last' logic prevents a trailing comma error
        {% for col in payment_cols %}
            COALESCE(pay.{{ col }}, 0) AS {{ col }}
            {% if not loop.last %},{% endif %}
        {% endfor %}

    FROM app_train AS app
    
    -- LEFT JOINS allow us to keep customers even if they have no history
    LEFT JOIN bureau AS bur 
        ON app.current_app_id = bur.current_app_id
        
    LEFT JOIN prev_app AS prev 
        ON app.current_app_id = prev.current_app_id
        
    LEFT JOIN payments AS pay 
        ON app.current_app_id = pay.current_app_id
)

SELECT * FROM final_dataset