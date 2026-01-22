# 📘 The Engineering Handbook: Home Credit Risk Pipeline

> **"From Raw CSVs to Executive Dashboard: A Deep Dive into the Architecture"**

This document serves as the technical documentation for the End-to-End ELT pipeline. It details the journey of data from a raw, messy state to a production-grade, tested, and automated analytical dataset.

---

## 🏗️ Phase 1: Infrastructure & Cloud Setup (The Foundation)

Before writing a single line of SQL, we established a secure and scalable cloud infrastructure using **AWS** and **Snowflake**.

### 1.1 The Data Source (Kaggle)
*   **Dataset:** Home Credit Default Risk.
*   **Challenge:** The data is highly relational (7 tables) and granular. A single loan application links to millions of rows of bureau history and payment transactions.
*   **Goal:** Predict `TARGET` (1 = Defaulter, 0 = Good Payer).

### 1.2 AWS S3 (The Data Lake)
We uploaded the raw CSV files to an **AWS S3 Bucket**.
*   **Why S3?** It acts as a durable, cheap storage layer ("Data Lake"). Snowflake acts as the compute engine. Decoupling storage (S3) and compute (Snowflake) is a modern architectural best practice.
*   **Security (IAM):** Instead of using "Root Keys" (which is dangerous), we created an **AWS IAM Role** with a specific policy that allows access *only* to this specific bucket.

### 1.3 Snowflake Configuration
We configured Snowflake to read directly from S3.
*   **Storage Integration:** We created a secure "handshake" object in Snowflake that references the AWS IAM Role.
*   **External Stage:** A pointer object (`@staging`) that looks at the S3 bucket.
*   **File Format:** Configured to parse CSVs, handle comma delimiters, and skip header rows.

---

## 💻 Phase 2: Local Development Environment

We avoided "GUI-based" development in favor of a professional **CI/CD-ready** local workflow.

*   **IDE:** VS Code.
*   **Language:** SQL + Jinja (Python templating).
*   **Dependency Management:** Python Virtual Environment (`venv`) to isolate libraries.
*   **Tool:** `dbt-snowflake` adapter.
*   **Connectivity:** Configured `profiles.yml` to securely connect local `dbt` to the remote Snowflake instance using multiple threads for parallel processing.

---

## 🏛️ Phase 3: The Architecture (Medallion Pattern)

We implemented the industry-standard **Bronze-Silver-Gold** architecture to organize data flow.

| Layer | Schema | Materialization | Purpose |
| :--- | :--- | :--- | :--- |
| **Bronze** | `BRONZE` | Table | **Raw Ingestion.** Direct copy of source. No transformations. |
| **Silver** | `SILVER` | Incremental | **Clean & Aggregate.** Deduplication, Macros, Logic. |
| **Gold** | `GOLD` | Table | **Consumption.** Star Schema / OBT for Power BI. |

---

## 🥉 Phase 4: Bronze Layer (Ingestion)

**Objective:** Create a clean mirror of the raw data.

*   **Source Mapping (`sources.yml`):** We mapped the raw Snowflake tables to dbt. This creates an abstraction layer. If the raw table name changes, we fix it in one YAML file, not in 50 SQL queries.
*   **Models:** Simple `SELECT *` transformations to bring data into the dbt ecosystem.

---

## 🥈 Phase 5: Silver Layer (The Engineering Heavy Lifting)

**Objective:** Turn "Technically Correct" data into "Business Valuable" data. This is where 80% of the engineering complexity lies.

### 5.1 Solving the "Granularity Mismatch" (Fan-Out)
*   **The Problem:** The `Application` table is at the *Customer* level (1 row per customer). The `Bureau` and `Payments` tables are at the *Transaction* level (many rows per customer).
*   **The Risk:** If you join these directly, you get a **Cartesian Product (Fan-out)**. A customer with 50 past payments will appear 50 times. Summing their income will result in 50x their actual income.
*   **The Solution (Aggregation):** We performed "Squashing" logic in the Silver layer using `GROUP BY`.
    *   **Bureau Table:** Aggregated to calculate `Total_Active_Loans` and `Total_Debt`.
    *   **Payments Table:** Aggregated to calculate `Total_Late_Payments`.
    *   **Result:** The child tables are compressed to 1 row per customer *before* joining.

### 5.2 Jinja Macros (DRY Principle)
*   **The Problem:** The dataset used a legacy system code `365243` to represent "Unemployed" in the `DAYS_EMPLOYED` column. This number (approx 1,000 years) destroys average calculations.
*   **The Solution:** We wrote a reusable Jinja Macro called `fix_anomaly`.
    ```sql
    {% macro fix_anomaly(column_name, value) %}
        CASE WHEN {{ column_name }} = {{ value }} THEN NULL ELSE {{ column_name }} END
    {% endmacro %}
    ```
*   **Impact:** We applied this macro across multiple tables. If the logic changes, we update it in one place.

### 5.3 Incremental Loading (Cost Optimization)
*   **The Problem:** Truncating and reloading 10 million rows daily wastes compute credits and takes hours.
*   **The Solution:** We configured Silver models as `incremental` with `merge` strategy.
    ```sql
    config(materialized='incremental', unique_key='current_app_id', incremental_strategy='merge')
    ```
*   **Impact:** dbt checks the Primary Key. It inserts **only new records** and updates **only changed records**.

---

## 🥇 Phase 6: Gold Layer (Metadata-Driven Pipelines)

**Objective:** Create a single, wide, highly performant table for Power BI.

### 6.1 The "OBT" (One Big Table) Strategy
We chose OBT over a traditional Star Schema in Snowflake to optimize Power BI performance. By doing the joins in the warehouse, Power BI imports a single flat table, resulting in sub-second query response times.

### 6.2 Metadata-Driven Automation
*   **The Problem:** The Gold table requires selecting 30+ columns from 4 different tables and handling `NULL` values for every single one (using `COALESCE`). Writing this manually is tedious and error-prone.
*   **The Solution:** We used **Jinja Python Logic** inside SQL.
    1.  We defined lists of columns (Metadata) at the top of the file.
    2.  We iterated through these lists using a `{% for %}` loop to write the SQL automatically.
    *   **Code Snippet:**
        ```sql
        {% for col in payment_cols %}
            COALESCE(pay.{{ col }}, 0) AS {{ col }},
        {% endfor %}
        ```

---

## 🛡️ Phase 7: Quality Assurance (Automated Testing)

We treat data pipelines like software. We pushed code that tests itself.

*   **Contract Tests (`schema.yml`):**
    *   `unique`: Enforced on Primary Keys. Prevents duplicate rows.
    *   `not_null`: Enforced on critical metrics (e.g., `Total_Income`).
    *   `accepted_values`: Enforced on categorical columns (e.g., Loan Type must be 'Cash' or 'Revolving').
*   **Execution:** Every time `dbt test` runs, Snowflake scans the data. If a test fails, the pipeline halts, preventing bad data from entering the dashboard.

---

## 📊 Phase 8: Business Intelligence (Power BI)

The final step was visualizing the value.

*   **Connection:** Import Mode via Snowflake Connector.
*   **Data Model:** Single Table (OBT) - No complex relationships required in Power BI.
*   **Key DAX Measures:**
    *   `Default Rate % = DIVIDE(Count(Defaulters), Count(Total Applicants))`
*   **Insights:** The dashboard visually correlates "Past Payment Behavior" (from Silver) with "Current Default Risk" (from Gold), proving the hypothesis that **behavioral history is the strongest predictor of risk.**