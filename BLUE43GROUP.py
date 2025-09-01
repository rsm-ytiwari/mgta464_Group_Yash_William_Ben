"""
============================================================
ETL/ELT Case — Final Python Deliverable (Snowflake-first) --
============================================================
Goal: Build RAW→STAGE→CORE→MART pipeline for purchases, invoices, suppliers, and NOAA weather,
then produce the joined artifact required by the assignment.


Snowflake-first principles:
- Pushdown all transforms to Snowflake (use SQL via cs.execute).
- No SELECT * (explicit column selection for governance & cost).
- Right-size warehouses (XSMALL, auto-suspend).
- Enforce lineage (RAW → STAGE → CORE → MART).
- Masking/secrets via env vars (no hardcoded credentials).


📌‼️‼️‼️‼️ SINCE THIS USES OTHER RELATIVE FILE PATH, This file won't work in isolation, for complete file go to GitHub Repo: https://github.com/rsm-ytiwari/mgta464_Group_Yash_William_Ben    ‼️‼️‼️‼️
"""

# ============================================================
# Imports and Connection Helpers
# ============================================================
import os
import csv
from glob import glob
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()
# ============================================================
# STEP 0 — Session context (role, WH, DB, schemas)

# ============================================================
csv_dir = "Data/Monthly PO Data"  # folder with 2019-1.csv ... 2022-5.csv
LOCAL_XML_DIR = "Data/xml"  # folder with *.xml invoices

# Object names (fully qualified to avoid session state)
PUR_STAGE = "SQL_ETL_DB.RAW.PURCHASES_STG"
PUR_FF = "SQL_ETL_DB.RAW.FF_CSV_V1"
PUR_RAW = "SQL_ETL_DB.RAW.PURCHASES_RAW"  # or CORE.PURCHASES if you type on load

# XML / invoices
INV_STAGE = "SQL_ETL_DB.RAW.XML_INVOICE_STG"
INV_FF = "SQL_ETL_DB.RAW.FF_XML_V1"
INV_RAW = "SQL_ETL_DB.RAW.RAW_XML_INV"  # if you have this landing table
INV_TGT = "SQL_ETL_DB.CORE.SUPPLIER_INVOICES"  # curated/typed target

# Views (curated layer)
VW_PO_TOTALS = "SQL_ETL_DB.CORE.VW_PO_TOTALS"
VW_PO_INVOICE_JOIN = "SQL_ETL_DB.CORE.VW_PO_INVOICE_JOIN"


# Views for Task 4
VW_PO_TOTALS = "VW_PO_TOTALS"
VW_PO_INVOICE_JOIN = "VW_PO_INVOICE_JOIN"


def connect():
    """Open a Snowflake connection with least-privilege defaults."""
    conn = snowflake.connector.connect(
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        role=os.environ.get("SNOWFLAKE_ROLE", "SYSADMIN"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "WH_ETL_XS"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "SQL_ETL_DB"),
    )
    return conn, conn.cursor()


def ensure_warehouse_db(cs):
    """Right-size warehouse; create DB & schemas for lineage."""
    cs.execute("USE ROLE sysadmin")
    cs.execute("""
    CREATE WAREHOUSE IF NOT EXISTS WH_ETL_XS
      WAREHOUSE_SIZE = 'XSMALL'
      AUTO_SUSPEND = 60
      AUTO_RESUME = TRUE
      MIN_CLUSTER_COUNT = 1
      MAX_CLUSTER_COUNT = 1
      INITIALLY_SUSPENDED = TRUE
    """)
    cs.execute("USE WAREHOUSE WH_ETL_XS")

    cs.execute("CREATE DATABASE IF NOT EXISTS SQL_ETL_DB")
    cs.execute("USE DATABASE SQL_ETL_DB")
    for s in ("RAW", "STAGE", "CORE", "MART"):
        cs.execute(f"CREATE SCHEMA IF NOT EXISTS {s}")
    cs.execute("SHOW SCHEMAS IN SQL_ETL_DB")
    print("schemas:", [r[1] for r in cs.fetchall()])


def create_stages_formats(cs):
    """Create internal stages and typed file formats."""
    cs.execute("CREATE STAGE IF NOT EXISTS RAW.PURCHASES_STG")
    cs.execute("CREATE STAGE IF NOT EXISTS RAW.SUPPLIER_STG")
    cs.execute("CREATE STAGE IF NOT EXISTS RAW.ZIP_GEO_STG")
    cs.execute("CREATE STAGE IF NOT EXISTS RAW.XML_INVOICE_STG")
    cs.execute("SHOW STAGES IN SCHEMA SQL_ETL_DB.RAW")
    print("stages:", [r[1] for r in cs.fetchall()])

    cs.execute("""
    CREATE OR REPLACE FILE FORMAT RAW.FF_CSV_PREVIEW
      TYPE=CSV FIELD_DELIMITER=',' SKIP_HEADER=0
    """)
    cs.execute("""
    CREATE OR REPLACE FILE FORMAT RAW.FF_CSV_V1
      TYPE=CSV FIELD_DELIMITER=',' SKIP_HEADER=1 TRIM_SPACE=TRUE
      NULL_IF=('','NULL','null') EMPTY_FIELD_AS_NULL=TRUE
    """)
    cs.execute("""
    CREATE OR REPLACE FILE FORMAT RAW.FF_TSV_V1
      TYPE=CSV FIELD_DELIMITER='\\t' SKIP_HEADER=1 TRIM_SPACE=TRUE
      NULL_IF=('','NULL','null') EMPTY_FIELD_AS_NULL=TRUE
    """)
    cs.execute("""
    CREATE OR REPLACE FILE FORMAT RAW.FF_XML_V1
      TYPE=XML STRIP_OUTER_ELEMENT=FALSE
    """)
    cs.execute("SHOW FILE FORMATS IN SCHEMA SQL_ETL_DB.RAW")
    print("file_formats:", [r[1] for r in cs.fetchall()])


# ============================================================
# STEP 7 A— supplier_zip_code_weather (assignment-required name)-> NOAA Marketplace access (manual subscription done in UI)


# ============================================================
def grant_marketplace_access_smoketest(cs):
    """Grant imported privileges and do a quick read to verify access."""
    cs.execute(
        "GRANT IMPORTED PRIVILEGES ON DATABASE WEATHER__ENVIRONMENT TO ROLE SYSADMIN"
    )
    cs.execute("USE ROLE SYSADMIN")
    cs.execute("USE DATABASE WEATHER__ENVIRONMENT")
    cs.execute("SHOW SCHEMAS")
    print(cs.fetchall())

    cs.execute("""
    SELECT
        NOAA_WEATHER_STATION_ID, VARIABLE, DATE, VALUE, UNIT
    FROM WEATHER__ENVIRONMENT.CYBERSYN.NOAA_WEATHER_METRICS_TIMESERIES
    WHERE VARIABLE = 'maximum_temperature'
    LIMIT 10
    """)
    print(cs.fetchall())


def load_zip_geo(
    cs,
    local_path="/home/jovyan/MSBA/05_MISC/SQL_Project/Data/2021_Gaz_zcta_national.txt",
):
    """
    Load ZIP (ZCTA) centroids from a TSV into RAW.ZIP_GEO.
    Keeps only: ZIP, LAT, LON.
    """
    cs.execute("USE ROLE SYSADMIN")
    cs.execute("USE WAREHOUSE WH_ETL_XS")
    cs.execute("USE DATABASE SQL_ETL_DB")
    cs.execute("USE SCHEMA RAW")

    cs.execute("""
    CREATE OR REPLACE TABLE RAW.ZIP_GEO (
      ZIP STRING,
      LAT DOUBLE,
      LON DOUBLE
    )
    """)
    print("created RAW.ZIP_GEO")

    cs.execute(f"""
    PUT 'file://{local_path}'
      @RAW.ZIP_GEO_STG
      AUTO_COMPRESS=TRUE
    """)

    cs.execute("LIST @RAW.ZIP_GEO_STG")
    print(cs.fetchall())

    cs.execute("TRUNCATE TABLE RAW.ZIP_GEO")
    cs.execute("""
    COPY INTO RAW.ZIP_GEO (ZIP, LAT, LON)
    FROM (
      SELECT
        SUBSTR($1, 1, 5)  AS ZIP,   -- GEOID (first 5 chars)
        TRY_TO_DOUBLE($6) AS LAT,   -- INTPTLAT
        TRY_TO_DOUBLE($7) AS LON    -- INTPTLONG
      FROM @RAW.ZIP_GEO_STG (FILE_FORMAT => RAW.FF_TSV_V1)
    )
    ON_ERROR = 'ABORT_STATEMENT'
    """)

    cs.execute("""
    SELECT
      MIN(LAT), MAX(LAT),
      MIN(LON), MAX(LON),
      COUNT_IF(LAT IS NULL OR LON IS NULL) AS null_coords
    FROM RAW.ZIP_GEO
    """)
    print(cs.fetchall())


def build_zip_station_map(cs):
    """
    Map each ZIP to the nearest *eligible* station (one that reports maximum_temperature).
    Uses:
      - ±1° bounding box to prune candidates (cheap)
      - Haversine distance to pick the closest
    """
    cs.execute("USE ROLE SYSADMIN")
    cs.execute("USE WAREHOUSE WH_ETL_XS")
    cs.execute("USE DATABASE SQL_ETL_DB")
    cs.execute("USE SCHEMA CORE")

    # Inspect station index (optional; keeps the code deterministic if columns change)
    cs.execute("""
    DESCRIBE TABLE WEATHER__ENVIRONMENT.CYBERSYN.NOAA_WEATHER_STATION_INDEX
    """)
    print("station index columns:", [r[0] for r in cs.fetchall()])

    # Eligible stations = present in maximum_temperature timeseries
    cs.execute("""
    CREATE OR REPLACE TEMP VIEW CORE__STATIONS_ELIGIBLE AS
    WITH elig AS (
      SELECT DISTINCT NOAA_WEATHER_STATION_ID
      FROM WEATHER__ENVIRONMENT.CYBERSYN.NOAA_WEATHER_METRICS_TIMESERIES
      WHERE VARIABLE='maximum_temperature'
      -- AND DATE >= DATEADD(year,-10,CURRENT_DATE())  -- optional perf tweak
    )
    SELECT
      i.NOAA_WEATHER_STATION_ID AS STATION_ID,
      TRY_TO_DOUBLE(i.LATITUDE) AS ST_LAT,
      TRY_TO_DOUBLE(i.LONGITUDE) AS ST_LON
    FROM WEATHER__ENVIRONMENT.CYBERSYN.NOAA_WEATHER_STATION_INDEX i
    JOIN elig e ON i.NOAA_WEATHER_STATION_ID = e.NOAA_WEATHER_STATION_ID
    WHERE ST_LAT IS NOT NULL AND ST_LON IS NOT NULL
    """)

    # Primary nearest mapping (±1°)
    cs.execute("""
    CREATE OR REPLACE TABLE CORE.ZIP_NEAREST_STATION AS
    WITH CAND AS (
      SELECT
        z.ZIP, z.LAT AS ZIP_LAT, z.LON AS ZIP_LON,
        s.STATION_ID, s.ST_LAT, s.ST_LON
      FROM SQL_ETL_DB.RAW.ZIP_GEO z
      JOIN CORE__STATIONS_ELIGIBLE s
        ON s.ST_LAT BETWEEN z.LAT - 1 AND z.LAT + 1
       AND s.ST_LON BETWEEN z.LON - 1 AND z.LON + 1
    ),
    DIST AS (
      SELECT
        ZIP, STATION_ID,
        2 * 6371 * ASIN(
          SQRT(
            POWER(SIN(RADIANS((ST_LAT - ZIP_LAT)/2)),2) +
            COS(RADIANS(ZIP_LAT)) * COS(RADIANS(ST_LAT)) *
            POWER(SIN(RADIANS((ST_LON - ZIP_LON)/2)),2)
          )
        ) AS KM
      FROM CAND
    )
    SELECT ZIP, STATION_ID, KM
    FROM (
      SELECT ZIP, STATION_ID, KM,
             ROW_NUMBER() OVER (PARTITION BY ZIP ORDER BY KM) AS rn
      FROM DIST
    )
    WHERE rn = 1
    """)

    # Fallback for unmapped ZIPs (±2° only for those)
    cs.execute("""
    CREATE OR REPLACE TEMP TABLE TMP_ZIPS_MISSING AS
    SELECT z.ZIP, z.LAT, z.LON
    FROM SQL_ETL_DB.RAW.ZIP_GEO z
    LEFT JOIN CORE.ZIP_NEAREST_STATION n USING (ZIP)
    WHERE n.ZIP IS NULL
    """)

    cs.execute("""
    CREATE OR REPLACE TEMP TABLE TMP_ZIP_NEAREST_FALLBACK AS
    WITH CAND AS (
      SELECT z.ZIP, z.LAT AS ZIP_LAT, z.LON AS ZIP_LON,
             s.STATION_ID, s.ST_LAT, s.ST_LON
      FROM TMP_ZIPS_MISSING z
      JOIN CORE__STATIONS_ELIGIBLE s
        ON s.ST_LAT BETWEEN z.LAT - 2 AND z.LAT + 2
       AND s.ST_LON BETWEEN z.LON - 2 AND z.LON + 2
    ),
    DIST AS (
      SELECT ZIP, STATION_ID,
             2*6371*ASIN(SQRT(
               POWER(SIN(RADIANS((ST_LAT - ZIP_LAT)/2)),2) +
               COS(RADIANS(ZIP_LAT))*COS(RADIANS(ST_LAT))*
               POWER(SIN(RADIANS((ST_LON - ZIP_LON)/2)),2)
             )) AS KM
      FROM CAND
    )
    SELECT ZIP, STATION_ID, KM
    FROM (
      SELECT ZIP, STATION_ID, KM,
             ROW_NUMBER() OVER (PARTITION BY ZIP ORDER BY KM) rn
      FROM DIST
    )
    WHERE rn = 1
    """)

    cs.execute("""
    MERGE INTO CORE.ZIP_NEAREST_STATION t
    USING TMP_ZIP_NEAREST_FALLBACK s
    ON t.ZIP = s.ZIP
    WHEN NOT MATCHED THEN INSERT (ZIP, STATION_ID, KM) VALUES (s.ZIP, s.STATION_ID, s.KM)
    """)

    cs.execute("""
    SELECT
      (SELECT COUNT(DISTINCT ZIP) FROM RAW.ZIP_GEO)  AS zips,
      (SELECT COUNT(*) FROM CORE.ZIP_NEAREST_STATION) AS mapped
    """)
    print("coverage after realignment:", cs.fetchall())  # expect ~33790


def build_zip_daily_tmax(cs):
    """Materialize last--years daily TMAX by ZIP using the mapping."""
    cs.execute("USE SCHEMA CORE")
    cs.execute("DROP TABLE IF EXISTS CORE.ZIP_DAILY_TMAX")
    cs.execute("""
    CREATE TABLE CORE.ZIP_DAILY_TMAX AS
    SELECT
      m.ZIP,
      t.DATE,
      t.VALUE AS TMAX_C,
      t.UNIT
    FROM WEATHER__ENVIRONMENT.CYBERSYN.NOAA_WEATHER_METRICS_TIMESERIES t
    JOIN CORE.ZIP_NEAREST_STATION m
      ON t.NOAA_WEATHER_STATION_ID = m.STATION_ID
    WHERE t.VARIABLE = 'maximum_temperature'
    """)

    # Validations
    cs.execute("""
    SELECT COUNT(*) AS row_count,
           MIN(DATE) AS min_date,
           MAX(DATE) AS max_date,
           COUNT(DISTINCT ZIP) AS distinct_zips,
           COUNT(DISTINCT ZIP || '-' || DATE) AS unique_zip_dates
    FROM CORE.ZIP_DAILY_TMAX
    """)
    print(cs.fetchall())

    cs.execute("""
    SELECT ZIP, DATE, TMAX_C, UNIT
    FROM CORE.ZIP_DAILY_TMAX
    ORDER BY DATE DESC, ZIP
    LIMIT 10
    """)
    print(cs.fetchall())

    cs.execute("""
    SELECT
      COUNT_IF(TMAX_C IS NULL) AS null_tmax,
      COUNT_IF(UNIT <> 'Degrees Celsius') AS non_celsius
    FROM CORE.ZIP_DAILY_TMAX
    """)
    print(cs.fetchall())


# ============================================================
# STEP 7B build_supplier_zip_code_weather and the MATERIALIZED VIEW

# ============================================================


def build_supplier_zip_code_weather(
    supplier_table="SQL_ETL_DB.CORE.SUPPLIERS_PGSQL",
    supplier_zip_col="POSTALPOSTALCODE",  # the column that holds supplier ZIPs
):
    """
    Create CORE.SUPPLIER_ZIP_CODE_WEATHER with 3 cols:
      ZIP_CODE, DATE, HIGH_TEMPERATURE_C

    Idempotent:
      - Drops any existing TABLE/VIEW/MATERIALIZED VIEW with the target name
      - Attempts MATERIALIZED VIEW; if Snowflake rejects (joins), falls back to TABLE
    """
    import os
    from snowflake.connector.errors import ProgrammingError

    conn, cs = connect()
    try:
        DB = os.environ.get("SNOWFLAKE_DATABASE", "SQL_ETL_DB")
        CORE = f"{DB}.CORE"
        cs.execute(f"USE DATABASE {DB}")
        cs.execute(f"USE SCHEMA {CORE}")

        tgt = f"{CORE}.SUPPLIER_ZIP_CODE_WEATHER"

        # 1) Normalize supplier ZIPs to 5-digit strings (digits only), keep uniques
        cs.execute(f"""
          CREATE OR REPLACE TEMP VIEW SUPPLIER_ZIPS_5 AS
          SELECT DISTINCT
            LPAD(SUBSTR(REGEXP_REPLACE({supplier_zip_col}, '[^0-9]', ''), 1, 5), 5, '0') AS ZIP
          FROM {supplier_table}
          WHERE {supplier_zip_col} IS NOT NULL
            AND LENGTH(REGEXP_REPLACE({supplier_zip_col}, '[^0-9]', '')) >= 5
        """)

        # 2) Common SELECT for both MV/TABLE
        select_sql = """
          SELECT
            s.ZIP   AS ZIP_CODE,
            z.DATE  AS DATE,
            z.TMAX_C AS HIGH_TEMPERATURE_C
          FROM SUPPLIER_ZIPS_5 s
          JOIN CORE.ZIP_DAILY_TMAX z
            ON z.ZIP = s.ZIP
        """

        # 3) Drop whatever exists with that name (order matters: MV, VIEW, TABLE)
        for obj_type in ("MATERIALIZED VIEW", "VIEW", "TABLE"):
            try:
                cs.execute(f"DROP {obj_type} IF EXISTS {tgt}")
            except ProgrammingError:
                pass  # ignore if not present or not applicable

        # 4) Try MV first, then fallback to TABLE (Snowflake MV disallows joins)
        created_as = None
        try:
            cs.execute(f"CREATE MATERIALIZED VIEW {tgt} AS {select_sql}")
            created_as = "MATERIALIZED VIEW"
        except ProgrammingError:
            cs.execute(f"CREATE TABLE {tgt} AS {select_sql}")
            created_as = "TABLE"

        # 5) Bounded checks
        cs.execute(f"SELECT COUNT(*) FROM {tgt}")
        n = cs.fetchone()[0]
        cs.execute(f"SELECT MIN(DATE), MAX(DATE) FROM {tgt}")
        dmin, dmax = cs.fetchone()

        print(f"✓ {created_as} created → {tgt}")
        print(f"Rows: {n}; date range: {dmin} … {dmax}")

    finally:
        cs.close()
        conn.close()


## ============================================================
# STEP 1-2 load_purchases_csvs
# The 41 files are located in path "Data/csv"
## ============================================================


def load_purchases_csvs():
    """Stage + COPY many CSVs → CORE.PURCHASES_RAW (typed). Snowflake-first & sessionless."""

    # --- Config (edit these to match your DB/WH naming) ---
    # Use repo-wide defaults so this function aligns with other modules
    DB_NAME = os.environ.get("SNOWFLAKE_DATABASE", "SQL_ETL_DB")
    WH_NAME = os.environ.get("SNOWFLAKE_WAREHOUSE", "WH_ETL_XS")
    STAGE_SCHEMA = "RAW"  # stage/schema used by other scripts (RAW.PURCHASES_STG)
    CORE_SCHEMA = "CORE"  # curated/typed target schema

    # Fully-qualified object names (no session dependency)
    PUR_STAGE = f"{DB_NAME}.{STAGE_SCHEMA}.PURCHASES_STG"
    PUR_FF = f"{DB_NAME}.{STAGE_SCHEMA}.FF_CSV_V1"
    PUR_RAW = f"{DB_NAME}.{CORE_SCHEMA}.PURCHASES_RAW"

    # --- Discover CSVs locally ---
    files = sorted(glob(os.path.join(csv_dir, "*.csv")))
    if not files:
        raise SystemExit(f"No CSV files found in {csv_dir}/")

    # Detect header from first file to find column indexes robustly
    first = files[0]
    with open(first, "r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        header = next(reader)
    norm = {h.strip().lower(): i for i, h in enumerate(header, start=1)}

    need_names = [
        "PurchaseOrderID",
        "PurchaseOrderLineID",  # instead of LineNumber
        "ReceivedOuters",
        "ExpectedUnitPricePerOuter",
        "OrderDate",
        "SupplierID",
    ]
    need = {}
    for name in need_names:
        idx = norm.get(name.lower())
        if not idx:
            raise SystemExit(f"Column not found in header: {name}\nHeader is: {header}")
        need[name] = idx

    # --- Snowflake connection ---
    conn, cs = connect()  # expects your connect() -> (conn, cursor)

    # Always select/justify a warehouse (small, auto-suspend via account defaults/policy)
    cs.execute(f"USE WAREHOUSE {WH_NAME}")

    # Create stage + file format (fully qualified)
    cs.execute(f"CREATE STAGE IF NOT EXISTS {PUR_STAGE}")
    cs.execute(f"""
        CREATE OR REPLACE FILE FORMAT {PUR_FF}
          TYPE=CSV FIELD_DELIMITER=',' SKIP_HEADER=1
          NULL_IF=('\\N','NULL','') EMPTY_FIELD_AS_NULL=TRUE
    """)

    # Upload CSVs to internal stage
    for p in files:
        abs_path = os.path.abspath(p).replace("\\", "/")
        cs.execute(f"PUT 'file://{abs_path}' @{PUR_STAGE} OVERWRITE=TRUE")

    # Target typed table lives in CORE (governed, query-ready)
    cs.execute(f"""
        CREATE OR REPLACE TABLE {PUR_RAW} (
          PurchaseOrderID            STRING,
          PurchaseOrderLineID        INT,
          ReceivedOuters             NUMBER(18,4),
          ExpectedUnitPricePerOuter  NUMBER(18,4),
          OrderDate                  DATE,
          SupplierID                 STRING
        )
    """)

    # COPY with positional selects + casting
    sql = f"""
    COPY INTO {PUR_RAW}
    FROM (
      SELECT
        ${need["PurchaseOrderID"]}::STRING               AS PurchaseOrderID,
        ${need["PurchaseOrderLineID"]}::INT              AS PurchaseOrderLineID,
        TRY_TO_DECIMAL(${need["ReceivedOuters"]})        AS ReceivedOuters,
        TRY_TO_DECIMAL(${need["ExpectedUnitPricePerOuter"]})
                                                         AS ExpectedUnitPricePerOuter,
        TRY_TO_DATE(${need["OrderDate"]})                AS OrderDate,
        ${need["SupplierID"]}::STRING                    AS SupplierID
      FROM @{PUR_STAGE} (FILE_FORMAT => {PUR_FF})
    )
    ON_ERROR='ABORT_STATEMENT'
    """
    cs.execute(sql)

    # Small checks (no SELECT *; bounded)
    cs.execute(f"SELECT COUNT(*) FROM {PUR_RAW}")
    print("Rows in purchases_raw:", cs.fetchone()[0])

    # Simple PO view (in CORE)
    cs.execute(f"""
        CREATE OR REPLACE VIEW {DB_NAME}.{CORE_SCHEMA}.PURCHASES_PO AS
        SELECT
          PurchaseOrderID,
          SUM(ReceivedOuters * ExpectedUnitPricePerOuter) AS POAmount
        FROM {PUR_RAW}
        GROUP BY 1
    """)
    cs.execute(
        f"SELECT PurchaseOrderID, POAmount FROM {DB_NAME}.{CORE_SCHEMA}.PURCHASES_PO ORDER BY PurchaseOrderID LIMIT 5"
    )
    print("Sample POAmount rows:", cs.fetchmany(5))

    cs.close()
    conn.close()
    print("✓ purchases loaded →", PUR_RAW)


## ============================================================
# STEP 3  Extract and load the supplier invoice XML data
## ============================================================


def load_supplier_invoices_csv_typed(
    csv_path: str = "/home/jovyan/MSBA/05_MISC/SQL_Project/Data/csv/invoices_extracted.csv",
    *,
    warehouse: str = "WH_ETL_XS",
    db: str = "SQL_ETL_DB",
    raw_schema: str = "RAW",
    core_schema: str = "CORE",
    stage_name: str = "INVOICE_CSV_STG",
    ff_name: str = "FF_INV_CSV_V1",
    landing_table: str = "INVOICE_CSV_LANDING",
    core_table: str = "SUPPLIER_INVOICES",
):
    """
    Upload exactly one invoices CSV and push typed rows into CORE.SUPPLIER_INVOICES.

    CSV columns expected (8):
      INVOICE_ID,SUPPLIER_ID,PURCHASE_ORDER_ID,INVOICE_DATE,
      AMOUNT_EXCLUDING_TAX,TAX_AMOUNT,TOTAL_AMOUNT,CURRENCY_CODE
    """
    import os

    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    INV_STAGE = f"{db}.{raw_schema}.{stage_name}"
    INV_FF = f"{db}.{raw_schema}.{ff_name}"
    INV_LAND = f"{db}.{raw_schema}.{landing_table}"
    INV_CORE = f"{db}.{core_schema}.{core_table}"

    conn, cs = connect()
    try:
        # Cost control: small WH, compute in Snowflake
        cs.execute(f"USE WAREHOUSE {warehouse}")
        cs.execute(f"USE DATABASE {db}")

        # Idempotent infra
        cs.execute(f"CREATE SCHEMA IF NOT EXISTS {raw_schema}")
        cs.execute(f"CREATE SCHEMA IF NOT EXISTS {core_schema}")

        cs.execute(
            f"CREATE OR REPLACE FILE FORMAT {INV_FF} "
            "TYPE=CSV SKIP_HEADER=1 FIELD_OPTIONALLY_ENCLOSED_BY='\"' "
            "TRIM_SPACE=TRUE NULL_IF=('','NULL','null') EMPTY_FIELD_AS_NULL=TRUE"
        )
        cs.execute(f"CREATE STAGE IF NOT EXISTS {INV_STAGE} FILE_FORMAT={INV_FF}")

        cs.execute(f"""
        CREATE TABLE IF NOT EXISTS {INV_LAND} (
          INVOICE_ID            STRING,
          SUPPLIER_ID           STRING,
          PURCHASE_ORDER_ID     STRING,
          INVOICE_DATE          STRING,    -- cast later
          AMOUNT_EXCLUDING_TAX  STRING,
          TAX_AMOUNT            STRING,
          TOTAL_AMOUNT          STRING,
          CURRENCY_CODE         STRING,
          FILENAME              STRING,
          LOAD_TS               TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
        """)

        # Typed target (same types as XML path)
        cs.execute(f"""
        CREATE OR REPLACE TABLE {INV_CORE} (
          INVOICE_ID            STRING,
          SUPPLIER_ID           STRING,
          PURCHASE_ORDER_ID     STRING,
          INVOICE_DATE          DATE,
          AMOUNT_EXCLUDING_TAX  NUMBER(18,2),
          TAX_AMOUNT            NUMBER(18,2),
          TOTAL_AMOUNT          NUMBER(18,2),
          CURRENCY_CODE         STRING,
          CREATED_TS            TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
        """)

        # Keep the stage clean so COPY can't pick up the wrong file
        cs.execute(f"REMOVE @{INV_STAGE} PATTERN='.*'")

        # 1) PUT just this file (compress = faster network/scan)
        abs_path = os.path.abspath(csv_path).replace("\\", "/")
        cs.execute(
            f"PUT file://{abs_path} @{INV_STAGE} AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
        )

        # 2) COPY from stage → RAW landing (no FILES clause = copy the only staged file)
        cs.execute(f"""
        COPY INTO {INV_LAND}
        (INVOICE_ID,SUPPLIER_ID,PURCHASE_ORDER_ID,INVOICE_DATE,
         AMOUNT_EXCLUDING_TAX,TAX_AMOUNT,TOTAL_AMOUNT,CURRENCY_CODE,FILENAME)
        FROM (
          SELECT t.$1,t.$2,t.$3,t.$4,t.$5,t.$6,t.$7,t.$8, METADATA$FILENAME
          FROM @{INV_STAGE} t
        )
        FILE_FORMAT=(FORMAT_NAME={INV_FF})
        ON_ERROR='ABORT_STATEMENT'
        """)

        # 3) Insert typed rows → CORE (TRY_* matches XML cast rules)
        cs.execute(f"TRUNCATE TABLE {INV_CORE}")
        cs.execute(f"""
        INSERT INTO {INV_CORE}
        (INVOICE_ID, SUPPLIER_ID, PURCHASE_ORDER_ID, INVOICE_DATE,
         AMOUNT_EXCLUDING_TAX, TAX_AMOUNT, TOTAL_AMOUNT, CURRENCY_CODE)
        SELECT
          INVOICE_ID::STRING                              AS INVOICE_ID,
          SUPPLIER_ID::STRING                             AS SUPPLIER_ID,
          NULLIF(PURCHASE_ORDER_ID, '')                   AS PURCHASE_ORDER_ID,
          TRY_TO_DATE(INVOICE_DATE)                       AS INVOICE_DATE,
          TRY_TO_DECIMAL(AMOUNT_EXCLUDING_TAX, 18, 2)     AS AMOUNT_EXCLUDING_TAX,
          TRY_TO_DECIMAL(TAX_AMOUNT, 18, 2)               AS TAX_AMOUNT,
          TRY_TO_DECIMAL(TOTAL_AMOUNT, 18, 2)             AS TOTAL_AMOUNT,
          NULLIF(CURRENCY_CODE, '')                       AS CURRENCY_CODE
        FROM {INV_LAND}
        """)

        # Diagnostics (bounded)
        cs.execute(f"SELECT COUNT(*) FROM {INV_LAND}")
        print("RAW landing rows:", cs.fetchone()[0])
        cs.execute(f"SELECT COUNT(*) FROM {INV_CORE}")
        print("CORE typed rows:", cs.fetchone()[0])

        print(f"✓ Loaded & typed → {INV_CORE}")

    finally:
        cs.close()
        conn.close()


## ============================================================
# STEP 4 creating a function to Join the purchases data from step 2 and the supplier invoices data from step 3 (only include matching rows); assuming that step 2 was completed correctly, you can assume the following relationships among the four tables (the other two tables are discussed below):
## ============================================================
def make_task4_join(latest_invoice_only=True):
    """Build VW_PO_TOTALS (POAmount by PO+Supplier) and VW_PO_INVOICE_JOIN (inner join matches only)."""
    conn, cs = connect()  # expects your connect() -> (conn, cursor)
    DB_NAME = os.environ.get("SNOWFLAKE_DATABASE", "SQL_ETL_DB")
    # ensure session has a current schema so CREATE VIEW without fully-qualified names works
    cs.execute(f"USE DATABASE {DB_NAME}")
    cs.execute(f"USE SCHEMA {DB_NAME}.CORE")

    # ensure we reference the purchases table that load_purchases_csvs created in CORE
    PUR_RAW_CORE = f"{DB_NAME}.CORE.PURCHASES_RAW"

    # totals per PO+Supplier (note use unquoted (uppercased) column names)
    cs.execute(f"""
      CREATE OR REPLACE VIEW {VW_PO_TOTALS} AS
      SELECT
        p.PURCHASEORDERID            AS purchase_order_id,
        p.SUPPLIERID                 AS supplier_id,
        MIN(p.ORDERDATE)             AS order_date,
        SUM(p.RECEIVEDOUTERS * p.EXPECTEDUNITPRICEPEROUTER) AS POAmount
      FROM {PUR_RAW_CORE} p
      GROUP BY 1,2;
    """)

    # join to invoices (uses uppercase columns in SUPPLIER_INVOICES)
    if latest_invoice_only:
        cs.execute(f"""
          CREATE OR REPLACE VIEW {VW_PO_INVOICE_JOIN} AS
          WITH R AS (
            SELECT
              INVOICE_ID, SUPPLIER_ID, PURCHASE_ORDER_ID,
              AMOUNT_EXCLUDING_TAX, TOTAL_AMOUNT, INVOICE_DATE, CURRENCY_CODE,
              ROW_NUMBER() OVER (
                PARTITION BY PURCHASE_ORDER_ID, SUPPLIER_ID
                ORDER BY INVOICE_DATE DESC, INVOICE_ID DESC
              ) rn
            FROM {INV_TGT}
          )
          SELECT
            po.purchase_order_id, po.supplier_id, po.order_date, po.POAmount,
            inv.INVOICE_ID, inv.INVOICE_DATE, inv.AMOUNT_EXCLUDING_TAX, inv.TOTAL_AMOUNT,
            inv.CURRENCY_CODE AS invoice_currency
          FROM {VW_PO_TOTALS} po
          JOIN R inv
            ON inv.PURCHASE_ORDER_ID = po.purchase_order_id
           AND inv.SUPPLIER_ID       = po.supplier_id
          WHERE rn = 1;
        """)
    else:
        cs.execute(f"""
          CREATE OR REPLACE VIEW {VW_PO_INVOICE_JOIN} AS
          SELECT
            po.purchase_order_id, po.supplier_id, po.order_date, po.POAmount,
            inv.INVOICE_ID, inv.INVOICE_DATE, inv.AMOUNT_EXCLUDING_TAX, inv.TOTAL_AMOUNT,
            inv.CURRENCY_CODE AS invoice_currency
          FROM {VW_PO_TOTALS} po
          JOIN {INV_TGT} inv
            ON inv.PURCHASE_ORDER_ID = po.purchase_order_id
           AND inv.SUPPLIER_ID       = po.supplier_id;
        """)

    # quick count
    cs.execute(f"SELECT COUNT(*) FROM {VW_PO_INVOICE_JOIN}")
    print("Rows in VW_PO_INVOICE_JOIN:", cs.fetchone()[0])

    cs.close()
    conn.close()
    print("✓ Task 4 views created →", VW_PO_TOTALS, "and", VW_PO_INVOICE_JOIN)


## ============================================================
# STEP 5->  Using the joined data from step 4, we are creating a calculated field that shows the difference between AmountExcludingTax and POAmount, name this field invoiced_vs_quoted, and save the result as a materialized view named purchase_orders_and_invoices
# The PURCHASE_ORDERS_AND_INVOICES can be located in SQL_ETL_DB /CORE /PURCHASE_ORDERS_AND_INVOICES in snowflake
## ============================================================
def build_purchase_orders_and_invoices():
    """
    Create CORE.PURCHASE_ORDERS_AND_INVOICES as a TABLE from VW_PO_INVOICE_JOIN
    with calculated column invoiced_vs_quoted = AMOUNT_EXCLUDING_TAX - POAmount.

    Snowflake does not support materialized views over joins/multiple tables,
    so we create a TABLE (idempotent).
    """
    import os
    from snowflake.connector.errors import ProgrammingError

    conn, cs = connect()
    try:
        DB = os.environ.get("SNOWFLAKE_DATABASE", "SQL_ETL_DB")
        CORE = f"{DB}.CORE"
        cs.execute(f"USE DATABASE {DB}")
        cs.execute(f"USE SCHEMA {CORE}")

        VW_JOIN = f"{CORE}.VW_PO_INVOICE_JOIN"
        TGT = f"{CORE}.PURCHASE_ORDERS_AND_INVOICES"

        # Drop existing table (safe noop if missing) and recreate from the view
        cs.execute(f"DROP TABLE IF EXISTS {TGT}")

        cs.execute(f"""
          CREATE OR REPLACE TABLE {TGT} AS
          SELECT
            purchase_order_id,
            supplier_id,
            order_date,
            POAmount,
            INVOICE_ID,
            INVOICE_DATE,
            AMOUNT_EXCLUDING_TAX,
            TOTAL_AMOUNT,
            invoice_currency,
            (AMOUNT_EXCLUDING_TAX - POAmount) AS invoiced_vs_quoted
          FROM {VW_JOIN}
        """)

        # Bounded checks
        cs.execute(f"SELECT COUNT(*) FROM {TGT}")
        n = cs.fetchone()[0]
        cs.execute(
            f"SELECT MIN(invoiced_vs_quoted), MAX(invoiced_vs_quoted) FROM {TGT}"
        )
        mn, mx = cs.fetchone()

        print(f"✓ TABLE created → {TGT}")
        print(f"Rows: {n}; invoiced_vs_quoted (min, max): ({mn}, {mx})")

    finally:
        cs.close()
        conn.close()


## ============================================================
# STEP 6 Loading the suppliers information in raw
## ============================================================
def load_step6_csv_dynamic(
    csv_path="/home/jovyan/MSBA/05_MISC/SQL_Project/Data/step6/PostgreSQL_step6.csv",
    *,
    warehouse="WH_ETL_XS",
    db="SQL_ETL_DB",
    raw_schema="RAW",
    core_schema="CORE",
    stage_name="suppliers_pgsql_STG",
    ff_name="FF_STEP6_CSV_V1",
    core_table="suppliers_pgsql",
):
    """
    Infer Snowflake column types from a CSV, create a typed table in CORE, and load it.

    Cost/perf/governance:
      - Small WH + auto-suspend assumed (keep costs down).
      - Compute happens inside Snowflake (COPY + casting).
      - No SELECT *; bounded diagnostics.
      - Fully-qualified object names (no session state drift).

    The function prints:
      - The inferred DDL (so you can review).
      - Row counts after load.
    """
    import re
    from datetime import datetime
    from decimal import Decimal, InvalidOperation

    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    # ---- Helpers ----
    NULL_TOKENS = {"", "NULL", "null", "\\N", "NaN", "nan", "None", "none"}

    # Simple Snowflake-safe, unquoted identifiers (UPPERCASE, [A-Z0-9_], no leading digit)
    def to_ident(name: str) -> str:
        base = re.sub(r"[^A-Za-z0-9_]", "_", name.strip())
        if not base:
            base = "COL"
        if base[0].isdigit():
            base = "_" + base
        return base.upper()

    # Try parse patterns
    ts_patterns = [
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
    ]

    def is_date(s: str) -> bool:
        try:
            datetime.strptime(s, "%Y-%m-%d")
            return True
        except Exception:
            return False

    def is_timestamp(s: str) -> bool:
        for p in ts_patterns:
            try:
                datetime.strptime(s, p)
                return True
            except Exception:
                continue
        return False

    BOOL_SET = {"true", "false", "t", "f", "yes", "no"}

    def is_boolean_token(s: str) -> bool:
        return s.lower() in BOOL_SET

    def parse_numeric_meta(s: str):
        # Returns (is_numeric, is_integer, int_digits, scale)
        # Strip sign and commas
        t = s.replace(",", "")
        try:
            d = Decimal(t)
        except (InvalidOperation, ValueError):
            return (False, False, 0, 0)
        tup = d.as_tuple()
        scale = -tup.exponent if tup.exponent < 0 else 0
        # total digits excluding sign and decimal
        digits = len(tup.digits)
        # int digits = digits - scale (but not negative)
        int_digits = max(0, digits - scale)
        is_integer = scale == 0
        return (True, is_integer, int_digits, scale)

    # ---- Scan CSV header + sample rows to infer types ----
    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        rdr = csv.reader(f)
        header = next(rdr)
        raw_cols = [h.strip() for h in header]
        cols = [to_ident(h) for h in raw_cols]

        # Stats per column
        seen_bool_only = [True] * len(cols)
        seen_ts_only = [True] * len(cols)
        seen_date_only = [True] * len(cols)
        seen_num_only = [True] * len(cols)

        # Numeric precision/scale trackers
        max_int_digits = [0] * len(cols)
        max_scale = [0] * len(cols)

        non_null_seen = [False] * len(cols)

        # Limit scan to avoid huge local work; Snowflake does the heavy lifting later
        MAX_SCAN = 10000
        for i, row in enumerate(rdr, start=1):
            if i > MAX_SCAN:
                break
            for j, val in enumerate(row[: len(cols)]):
                v = (val or "").strip()
                if v in NULL_TOKENS:
                    continue
                non_null_seen[j] = True

                # Try boolean (strict; don't treat '0'/'1' alone as boolean)
                if not is_boolean_token(v.lower()):
                    seen_bool_only[j] = False

                # Timestamp vs Date
                if not is_timestamp(v):
                    seen_ts_only[j] = False
                if not is_date(v):
                    seen_date_only[j] = False

                # Numeric
                is_num, is_int, int_dig, sc = parse_numeric_meta(v)
                if not is_num:
                    seen_num_only[j] = False
                else:
                    if int_dig > max_int_digits[j]:
                        max_int_digits[j] = int_dig
                    if sc > max_scale[j]:
                        max_scale[j] = sc

        # Decide each column type with conservative precedence:
        # Timestamp > Date > Number > Boolean > String
        inferred_types = []
        for k in range(len(cols)):
            if not non_null_seen[k]:
                # No signal: default to STRING
                inferred_types.append(("STRING", None))
                continue

            if seen_ts_only[k]:
                inferred_types.append(("TIMESTAMP_NTZ", None))
            elif seen_date_only[k]:
                inferred_types.append(("DATE", None))
            elif seen_num_only[k]:
                prec = max(1, min(38, max_int_digits[k] + max_scale[k]))
                sc = min(12, max_scale[k])  # keep scale reasonable
                # Prefer a little headroom on precision
                prec = min(38, max(10, prec))
                inferred_types.append((f"NUMBER({prec},{sc})", (prec, sc)))
            elif seen_bool_only[k]:
                inferred_types.append(("BOOLEAN", None))
            else:
                inferred_types.append(("STRING", None))

    # ---- Build DDL ----
    column_defs = []
    for raw, col, (typ, meta) in zip(raw_cols, cols, inferred_types):
        column_defs.append(f"  {col} {typ}")

    CORE = f"{db}.{core_schema}"
    RAW = f"{db}.{raw_schema}"
    STAGE = f"{RAW}.{stage_name}"
    FF = f"{RAW}.{ff_name}"
    TGT = f"{CORE}.{core_table}"

    ddl = f"CREATE OR REPLACE TABLE {TGT} (\n" + ",\n".join(column_defs) + "\n)"

    # ---- Execute in Snowflake ----
    conn, cs = connect()
    try:
        cs.execute(f"USE WAREHOUSE {warehouse}")
        cs.execute(f"USE DATABASE {db}")
        cs.execute(f"CREATE SCHEMA IF NOT EXISTS {raw_schema}")
        cs.execute(f"CREATE SCHEMA IF NOT EXISTS {core_schema}")

        # File format + stage (idempotent)
        cs.execute(
            f"""CREATE OR REPLACE FILE FORMAT {FF}
                TYPE=CSV SKIP_HEADER=1 FIELD_OPTIONALLY_ENCLOSED_BY='\"'
                TRIM_SPACE=TRUE NULL_IF=('','NULL','null','\\N') EMPTY_FIELD_AS_NULL=TRUE
            """
        )
        cs.execute(f"CREATE STAGE IF NOT EXISTS {STAGE} FILE_FORMAT={FF}")

        # Keep stage clean so COPY selects only the intended file
        cs.execute(f"REMOVE @{STAGE} PATTERN='.*'")

        # Create/replace typed target table
        print("Inferred DDL:\n" + ddl)
        cs.execute(ddl)

        # PUT file → stage (gzip = faster scan)
        abs_path = os.path.abspath(csv_path).replace("\\", "/")
        cs.execute(f"PUT file://{abs_path} @{STAGE} AUTO_COMPRESS=TRUE OVERWRITE=TRUE")

        # COPY directly into typed table; Snowflake handles conversion
        cs.execute(f"""
          COPY INTO {TGT}
          FROM @{STAGE}
          FILE_FORMAT=(FORMAT_NAME={FF})
          ON_ERROR='ABORT_STATEMENT'
        """)

        # Diagnostics (bounded)
        cs.execute(f"SELECT COUNT(*) FROM {TGT}")
        n = cs.fetchone()[0]
        print(f"✓ Loaded {n} rows into {TGT}")

        # lineage hint (which staged file)
        cs.execute(f"LIST @{STAGE}")
        staged = cs.fetchall()
        if staged:
            print("Staged file:", staged[0][0])

    finally:
        cs.close()
        conn.close()


## ============================================================
# STEP 8- build_supplier_zip_code_weather-> and building our MART
## ============================================================
def build_step8_final_join(
    *,
    warehouse: str = "WH_ETL_XS",
    db: str = "SQL_ETL_DB",
    core_schema: str = "CORE",
    mart_schema: str = "MART",
    po_inv_table: str = "PURCHASE_ORDERS_AND_INVOICES",  # from Step 5
    supplier_table: str = "SUPPLIERS_PGSQL",  # your Step 6 table
    supplier_id_col: str = "SUPPLIERID",  # adjust if named differently
    supplier_zip_col: str = "POSTALPOSTALCODE",  # adjust if named differently
    weather_table: str = "SUPPLIER_ZIP_CODE_WEATHER",  # from Step 7/“MV or TABLE”
    target_table: str = "PURCHASES_SUPPLIER_WEATHER",  # final artifact in MART
):
    """
    Create MART.PURCHASES_SUPPLIER_WEATHER by joining:
      CORE.PURCHASE_ORDERS_AND_INVOICES (transactions),
      CORE.SUPPLIER_CASE (supplier ZIPs),
      CORE.SUPPLIER_ZIP_CODE_WEATHER (zip-date temperatures).

    Join keys:
      - Supplier ZIP (normalized to 5 digits)
      - Transaction date = INVOICE_DATE (only rows with matching weather kept)

    Result columns (common, compact):
      purchase_order_id, supplier_id, invoice_id, transaction_date,
      order_date, amount_excluding_tax, POAmount, invoiced_vs_quoted,
      zip_code, high_temperature_c
    """
    import os

    conn, cs = connect()
    try:
        DB = db
        CORE = f"{DB}.{core_schema}"
        MART = f"{DB}.{mart_schema}"
        POI = f"{CORE}.{po_inv_table}"
        SUP = f"{CORE}.{supplier_table}"
        WTH = f"{CORE}.{weather_table}"
        TGT = f"{MART}.{target_table}"

        ## ============================================================
        #  build_final_mart
        ## ============================================================
        # Session & schemas
        cs.execute(f"USE WAREHOUSE {warehouse}")
        cs.execute(f"CREATE SCHEMA IF NOT EXISTS {MART}")
        cs.execute(f"USE DATABASE {DB}")
        cs.execute(f"USE SCHEMA {MART}")

        # Build final table (idempotent). Inner joins ensure only rows with weather exist.
        cs.execute(f"DROP TABLE IF EXISTS {TGT}")
        cs.execute(f"""
        CREATE TABLE {TGT} AS
        WITH SUP_ZIPS AS (
          SELECT DISTINCT
            {supplier_id_col} AS SUPPLIER_ID,
            LPAD(SUBSTR(REGEXP_REPLACE({supplier_zip_col}, '[^0-9]', ''), 1, 5), 5, '0') AS ZIP_CODE
          FROM {SUP}
          WHERE {supplier_zip_col} IS NOT NULL
        ),
        TXN AS (
          SELECT
            PURCHASE_ORDER_ID,
            SUPPLIER_ID,
            ORDER_DATE,
            INVOICE_ID,
            INVOICE_DATE,             -- transaction date we match to weather
            AMOUNT_EXCLUDING_TAX,
            POAmount,
            INVOICED_VS_QUOTED
          FROM {POI}
          WHERE INVOICE_DATE IS NOT NULL
        )
        SELECT
          t.PURCHASE_ORDER_ID,
          t.SUPPLIER_ID,
          t.INVOICE_ID,
          t.INVOICE_DATE AS TRANSACTION_DATE,
          t.ORDER_DATE,
          t.AMOUNT_EXCLUDING_TAX,
          t.POAmount,
          t.INVOICED_VS_QUOTED,
          z.ZIP_CODE,
          w.HIGH_TEMPERATURE_C
        FROM TXN t
        JOIN SUP_ZIPS z
          ON z.SUPPLIER_ID = t.SUPPLIER_ID
        JOIN {WTH} w
          ON w.ZIP_CODE = z.ZIP_CODE
         AND w.DATE = t.INVOICE_DATE
        """)

        # Bounded diagnostics (no SELECT *)
        cs.execute(f"SELECT COUNT(*) FROM {TGT}")
        n = cs.fetchone()[0]
        cs.execute(f"""
          SELECT MIN(TRANSACTION_DATE), MAX(TRANSACTION_DATE),
                 MIN(HIGH_TEMPERATURE_C), MAX(HIGH_TEMPERATURE_C)
          FROM {TGT}
        """)
        dmin, dmax, tmin, tmax = cs.fetchone()

        print(f"✓ Final table created → {TGT}")
        print(f"Rows: {n}; date range: {dmin} … {dmax}; temp range: {tmin} … {tmax}")

    finally:
        cs.close()
        conn.close()


## ============================================================
# Calling all the functions
## ============================================================
def main():
    conn, cs = connect()
    try:
        ensure_warehouse_db(cs)
        create_stages_formats(cs)
        grant_marketplace_access_smoketest(cs)
        load_zip_geo(cs)  # path can be overridden if needed
        build_zip_station_map(cs)
        build_zip_daily_tmax(cs)

        print("✅ task 7A complete- Weather ETL complete: CORE.ZIP_DAILY_TMAX ready.")
        load_purchases_csvs()  # 2019-*.csv … 2022-*.csv → purchases_raw
        print("✅ Task 1 complete-loaded all the 41 csvs")

        load_supplier_invoices_csv_typed()
        print("✅ Task 3 complete- Loaded the parsed XML FILE> into snowflake")

        make_task4_join(True)
        print("✅ task 4 -> join complete")
        build_purchase_orders_and_invoices()
        print("✅ step 5 complete-Built PURCHASE_ORDER_AND_INVOICES in snowflake")
        load_step6_csv_dynamic()
        print("✅ step 6 completed")
        build_supplier_zip_code_weather()
        print("✅ step 7B COMPLETE  built supplier_zip_code_weather")
        build_step8_final_join()
        print("✅ STEP 8- build_supplier_zip_code_weather-> and building our MART")

    finally:
        cs.close()
        conn.close()


if __name__ == "__main__":
    main()
