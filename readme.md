# Snowflake ETL Case (Python + SQL)

## 📌 Project Overview
This project builds an **ETL/ELT pipeline** in Snowflake using Python as the orchestration layer.
We integrate four data sources:
- **Purchases (CSV)** – 41 monthly purchase order files.
- **Invoices (XML)** – supplier transaction data.
- **Suppliers (Postgres)** – supplier_case table exported and staged.
- **Weather (Snowflake Marketplace)** – NOAA weather timeseries from Cybersyn.

All transforms follow a **Snowflake-first** approach:
- Pushdown via `cs.execute()` (SQL inside Python).
- No `SELECT *` (explicit column selection).
- Right-sized warehouse (`XSMALL`, auto-suspend).
- Lineage enforced: **RAW → STAGE → CORE → MART**.

---

## ⚙️ Setup Instructions

### 1. Clone Repo
```bash
git clone git@github.com:<your-username>/<your-repo>.git
cd <your-repo>

### 2. Configure .env

        SNOWFLAKE_USER=your_username
        SNOWFLAKE_PASSWORD=your_password
        SNOWFLAKE_ACCOUNT=your_account
        SNOWFLAKE_WAREHOUSE=WH_ETL_XS
        SNOWFLAKE_DATABASE=SQL_ETL_DB
        SNOWFLAKE_SCHEMA=CORE
        SNOWFLAKE_ROLE=ETL_ROLE

🚫 Do not commit .env. Use the provided .env.example as a template.
▶️ How to Run
    python final_etl.py
            This script executes all pipeline steps:
            load_purchases_csvs – stage + load purchases CSVs.
            load_invoices_xml – shred invoice XML into table.
            build_purchase_orders_and_invoices – join + calc invoiced_vs_quoted.
            load_suppliers_from_postgres – extract supplier data from Postgres → Snowflake.
            build_supplier_zip_code_weather – map supplier ZIPs to weather station temps.
            build_final_mart – final join across purchases, invoices, suppliers, and weather.

SQL_PROJECT/
├── final_etl.py          # 🚀 single Python deliverable (BLUE43... renamed cleanly)
├── xmltocsv.py           # helper script if still needed (optional to include)
├── README.md             # project overview + run instructions
├── .gitignore            # excludes .env, cache, logs
├── .env.example          # safe template for credentials
└── Data/
    ├── csv/
    ├── Monthly PO Data/
    ├── step6/
    ├── xml/
    ├── 2021_Gaz_zcta...txt
    └── supplier_case.psql
