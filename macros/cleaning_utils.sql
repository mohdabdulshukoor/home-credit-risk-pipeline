{% macro fix_anomaly(column_name, anomaly_value) %}
    /* 
       GOAT LEVEL LOGIC:
       This macro checks a column for a specific "bad value" (anomaly).
       If found, it turns it into a real NULL.
       If not, it keeps the original data.
       
       Args:
       - column_name: The column to clean
       - anomaly_value: The value that represents 'bad data' (e.g., 365243 or 'XNA')
    */
    CASE 
        -- We cast to string to safely compare both numbers and text
        WHEN CAST({{ column_name }} AS STRING) = '{{ anomaly_value }}' THEN NULL 
        ELSE {{ column_name }} 
    END
{% endmacro %}