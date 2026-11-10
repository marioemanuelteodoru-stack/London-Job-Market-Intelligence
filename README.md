[london_job_market_db_pipeline_summary.md](https://github.com/user-attachments/files/34652532/london_job_market_db_pipeline_summary.md)
# London Job Market Intelligence (LJMI): Database Architecture, Data Model, and ETL Pipeline Specification

The London Job Market Intelligence (LJMI) database platform is a specialized analytical system engineered to ingest, clean, disambiguate, classify, and model raw labor market job alerts across Greater London. The core objective of the platform is to transform unorganized, high-volume job alert feeds from sources such as Indeed and LinkedIn into a structured, multidimensional data warehouse. 

By resolving raw employer names against official Companies House registry records, the architecture separates direct corporate hiring from third-party recruitment agency postings. Furthermore, the platform standardizes job titles using the UK Standard Occupational Classification (SOC) framework, assigns domain-specific occupational categories, geocodes postal locations down to exact geographic coordinates, and exposes clean relational views for downstream multidimensional analytics across London boroughs, micro-industries, occupational categories, and time horizons.

---

## 1. System Architecture & Technology Stack

The LJMI platform relies on a modular, local-first data architecture. The database engine, geocoding microservices, and Python execution environments operate locally to maintain high throughput, data privacy, and deterministic processing.

### Tech Stack Component Breakdown

* **Database Engine**: PostgreSQL 14+ with the **PostGIS** spatial extension enabled (`CREATE EXTENSION IF NOT EXISTS postgis;`).
* **Database Interface & ORM**: **SQLAlchemy** (`create_engine`, `text`) for query execution and DataFrame streaming, and **psycopg2-binary** (`psycopg2.extras.execute_batch`) for bulk idempotent database insertions.
* **Data Manipulation & Transformation**: **pandas** for structured tabular operations, data cleaning, string normalization, and date parsing.
* **Fuzzy Entity Resolution Engine**: **RapidFuzz** (`rapidfuzz.process`, `fuzz.WRatio`, `fuzz.token_set_ratio`) for high-speed string matching across $30,000+$ registered companies.
* **Geocoding Microservice**: Local **postcodes.io** REST API deployed inside a **Docker container** listening on `http://localhost:8000/postcodes`, providing batch UK postcode resolution without rate limits or external network latency.
* **Configuration & Environment**: `python-dotenv` managing database connection credentials (`DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`) via a local `.env` configuration file.

---

## 2. PostgreSQL Database Schema & Relational Structure

The database schema, defined in `london_job_market`, consists of four core tables designed to store company dimension data, industrial classifications, raw job postings, and geographic fallback mappings.

```
                              +-----------------------+
                              |       companies       |
                              +-----------------------+
                              | company_number (PK)   |<---+
                              | company_name          |    |
                              | incorporation_date    |    |
                              | full_address          |    |
                              | postal_code           |    |
                              | outcode               |    |
                              | latitude, longitude   |    |
                              | admin_district        |    |
                              +-----------------------+    |
                                  |              ^         |
                                  | (1:N)        | (1:N)   |
                                  v              |         |
      +-------------------------------+   +-------------------------------+
      |          company_sic          |   |          job_alerts           |
      +-------------------------------+   +-------------------------------+
      | id (PK SERIAL)                |   | alert_id (PK SERIAL)          |
      | company_number (FK -> comp)   |   | source, message_id            |
      | sic_code_num                  |   | job_title, company_name_raw   |
      | sic_text                      |   | location_raw, date_received   |
      +-------------------------------+   | matched_company_number (FK)---|
                                          | match_confidence, match_status|
                                          | soc_code, soc_title           |
                                          | occ_category, job_category    |
                                          | is_repeat_flag                |
                                          +-------------------------------+
                                                          |
                                                          | (Fallback Join)
                                                          v
                                          +-------------------------------+
                                          |       location_mappings       |
                                          +-------------------------------+
                                          | id (PK SERIAL)                |
                                          | source, raw_pattern (UNIQUE)  |
                                          | mapped_location               |
                                          | latitude, longitude           |
                                          +-------------------------------+
```

### Table Definitions & Specifications

#### 1. `companies` (Primary Company Dimension Table)
Stores active London companies extracted from the official Companies House registry, geocoded with latitude, longitude, and administrative borough assignments.

```sql
CREATE TABLE IF NOT EXISTS companies (
    company_number VARCHAR(20) PRIMARY KEY,
    company_name TEXT NOT NULL,
    incorporation_date DATE,
    full_address TEXT,
    postal_code TEXT,
    outcode TEXT,
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    admin_district TEXT
);
```

#### 2. `company_sic` (Industrial Classification Bridge Table)
Stores Standard Industrial Classification (SIC) codes for registered companies. A company can hold up to four SIC codes, represented as a one-to-many relationship with cascading deletes.

```sql
CREATE TABLE IF NOT EXISTS company_sic (
    id SERIAL PRIMARY KEY,
    company_number VARCHAR(20) REFERENCES companies(company_number) ON DELETE CASCADE,
    sic_code_num INTEGER NOT NULL,
    sic_text TEXT,
    CONSTRAINT company_sic_unique UNIQUE (company_number, sic_code_num)
);
```

#### 3. `job_alerts` (Core Fact Table)
Stores raw and processed job postings collected from email alerts and web feeds. It maintains foreign key links to `companies` and holds classified occupational domains and matching confidence scores.

```sql
CREATE TABLE IF NOT EXISTS job_alerts (
    alert_id SERIAL PRIMARY KEY,
    source VARCHAR(50) NOT NULL,
    date_received DATE,
    ingested_at TIMESTAMPTZ DEFAULT now(),
    job_title TEXT,
    company_name_raw TEXT,
    location_raw TEXT,
    message_id VARCHAR(100),
    job_key TEXT,
    matched_company_number VARCHAR(20) REFERENCES companies(company_number),
    match_confidence SMALLINT,
    match_status VARCHAR(20) DEFAULT 'unmatched',
    soc_code VARCHAR(4),
    soc_title TEXT,
    occ_category VARCHAR(100),
    job_category VARCHAR(100),
    is_repeat_flag BOOLEAN DEFAULT FALSE,
    CONSTRAINT unique_source_message_id UNIQUE (source, message_id)
);
```

#### 4. `location_mappings` (Geographic Fallback Lookup Table)
Contains spatial reference coordinates for broad, non-specific location strings (e.g., "Central London", "Remote - UK") to ensure alerts without explicit postcodes can still be mapped.

```sql
CREATE TABLE IF NOT EXISTS location_mappings (
    id SERIAL PRIMARY KEY,
    source VARCHAR(50),
    raw_pattern TEXT UNIQUE NOT NULL,
    mapped_location TEXT NOT NULL,
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8)
);
```

### Seed Location Patterns
The schema setup initializes `location_mappings` with nine fallback London coordinate centroids:

| Source | Raw Pattern | Mapped Location | Latitude | Longitude |
| :--- | :--- | :--- | :--- | :--- |
| Indeed | Central London | London | 51.51550000 | -0.14140000 |
| Indeed | East London | London | 51.52860000 | -0.03250000 |
| Indeed | West London | London | 51.51170000 | -0.25680000 |
| Indeed | North London | London | 51.56200000 | -0.11810000 |
| Indeed | South London | London | 51.46670000 | -0.11670000 |
| Indeed | London | London | 51.50740000 | -0.12780000 |
| Indeed | Remote - UK | London | 51.50740000 | -0.12780000 |
| LinkedIn | London | London | 51.50740000 | -0.12780000 |
| LinkedIn | London, UK | London | 51.50740000 | -0.12780000 |

---

## 3. Analytical SQL Views & Semantic Model

To simplify querying and support downstream analytics, PostgreSQL implements an enriched unified view along with dimensional views used by Power BI.

### Primary Analytical View: `vw_job_alerts_enriched`

`vw_job_alerts_enriched` joins `job_alerts` with `companies`, `location_mappings`, and `company_sic`. It resolves final geographic coordinates using a `COALESCE` priority waterfall (company postcode coordinates first, fallback pattern coordinates second) and tags the coordinate provenance in `geo_source`.

```sql
CREATE OR REPLACE VIEW vw_job_alerts_enriched AS
SELECT
    ja.alert_id,
    ja.source,
    ja.date_received,
    ja.ingested_at,
    ja.job_title,
    ja.company_name_raw,
    ja.location_raw,
    ja.message_id,
    ja.match_confidence,
    ja.match_status,
    c.company_name AS matched_company_name,
    c.postal_code,
    c.outcode,
    c.admin_district,
    COALESCE(c.latitude, lm.latitude) AS final_latitude,
    COALESCE(c.longitude, lm.longitude) AS final_longitude,
    CASE
        WHEN c.latitude IS NOT NULL THEN 'company_match'
        WHEN lm.latitude IS NOT NULL THEN 'location_fallback'
        ELSE 'unresolved'
    END AS geo_source,
    sic.sic_code_num,
    sic.sic_text
FROM job_alerts ja
LEFT JOIN companies c ON ja.matched_company_number = c.company_number
LEFT JOIN location_mappings lm
    ON ja.location_raw ILIKE '%' || lm.raw_pattern || '%'
    AND ja.source = lm.source
LEFT JOIN company_sic sic ON c.company_number = sic.company_number;
```

### Business Dimensional & Disambiguation Views

* **`v_company_dim`**: Exposes cleaned company dimension records, active status, postal code outcodes, administrative district boundaries, and geocoded coordinates.
* **`v_direct_employers`**: Filters `job_alerts` to isolate job postings originating from verified direct corporate employers. Excludes recruitment agencies using agency flags, SIC classification, and match status filters (`match_status = 'auto_matched'` OR `match_confidence >= 80`).
* **`v_recruitment_agencies`**: Filters job postings originating from third-party recruitment staffing agencies, allowing separate analysis of agency hiring activity versus direct corporate demand.
* **`v_company_industry` & `fact_company_industry_bridge`**: Bridge tables connecting company profiles to macro-industry sectors (e.g., Financial Services, Technology, Professional Services) based on numeric SIC code ranges.

---

## 4. End-to-End Execution Pipeline (Scripts 00–05 & Reclassification)

The ETL workflow consists of seven automated execution stages written in Python. Each script performs a specific, idempotent transformation step.

```
 [Raw Companies House CSV]               [Raw Job Alert CSVs/TSVs]
            |                                        |
            v                                        v
   01_prepare_companies.py                     04_extract_raw.py
  (Filter Active London,                      (Standardize Headers,
   Clean Postcodes, SIC)                      Deduplicate 30d Window)
            |                                        |
            v                                        v
   02_geocode_companies.py                   raw_jobs_processed.csv
  (Batch Postcodes.io API                            |
   Docker -> Lat/Lon/District)                       |
            |                                        |
            v                                        v
   03_load_companies.py                      05_process_categories.py
  (Bulk PostgreSQL Insert                     (Tier 0-3 Entity Matcher,
   companies & company_sic)                    5-Tier SOC Classifier,
            |                                  Custom Category & Domain)
            +-------------------+--------------------+
                                |
                                v
                    [PostgreSQL: job_alerts]
                                ^
                                |
                    reclassify_alerts.py
                   (In-Place Bulk Updater)
```

### Script Execution Summary

| Script Name | Primary Input | Primary Output | Operational Purpose |
| :--- | :--- | :--- | :--- |
| **`00_schema_setup.py`** | Environment Variables (`.env`) | PostgreSQL Schema | Initializes PostGIS, creates DDL tables, constraints, seed mappings, and builds `vw_job_alerts_enriched`. |
| **`01_prepare_companies.py`** | Raw Companies House CSV | `companies_ready_for_geo.csv`, `sic_codes_clean.csv` | Filters active London companies, normalizes UK postcodes using regex, builds full address strings, and unpivots SIC codes. |
| **`02_geocode_companies.py`** | `companies_ready_for_geo.csv` | `companies_geocoded.csv` | Batches unique postcodes into chunks of 100 and fetches latitude, longitude, and borough district from local Docker `postcodes.io` API. |
| **`03_load_companies.py`** | `companies_geocoded.csv`, `sic_codes_clean.csv` | PostgreSQL Tables (`companies`, `company_sic`) | Idempotent bulk loader using `psycopg2.extras.execute_batch`. Replaces NaN values with SQL `NULL`s. |
| **`04_extract_raw.py`** | Email Job Alert CSVs/TSVs | `raw_jobs_processed.csv` | Normalizes alert headers, performs exact `(source, message_id)` dedup, executes a 30-day sliding window title similarity dedup ($\ge 85\%$), and filters against existing DB records. |
| **`05_process_categories.py`** | `raw_jobs_processed.csv`, PostgreSQL Index | PostgreSQL Table (`job_alerts`) | Executes 4-tier company entity resolution, 5-tier SOC classification, custom occupational domain mapping, and upserts alerts. |
| **`reclassify_alerts.py`** | Existing PostgreSQL `job_alerts` | Updated `job_alerts` Records | In-place bulk updater. Re-runs multi-tier entity matching and occupational classification on existing DB alerts without re-ingesting raw files. |

---

## 5. Detailed Pipeline Implementation & Algorithms

### Step 00: Schema Initialization (`00_schema_setup.py`)
Executes DDL statements against PostgreSQL, enables PostGIS, applies unique constraints (`unique_source_message_id` on `job_alerts` and `company_sic_unique` on `company_sic`), inserts seed location mappings, and compiles `vw_job_alerts_enriched`.

### Step 01: Companies House Ingestion & Cleaning (`01_prepare_companies.py`)
Reads `BasicCompanyDataAsOneFile-2026-09-01.csv`, filtering records where `CompanyStatus == 'Active'` and `RegAddress.PostTown == 'LONDON'`. Postcodes are normalized via regex:

$$\text{UK\_PC} = \text{\textasciicircum}([A\text{-}Z]\{1,2\}[0\text{-}9][0\text{-}9A\text{-}Z]?)\backslash s*([0\text{-}9][A\text{-}Z]\{2\})?\$Span$$

Addresses are concatenated into `full_address`, dates are standardized to `YYYY-MM-DD`, and four SIC text columns are unpivoted into long-format records (`company_number`, `sic_code_num`, `sic_text`).

### Step 02: Batch Geocoding (`02_geocode_companies.py`)
Extracts unique normalized postcodes from active London companies and sends HTTP POST requests to `http://localhost:8000/postcodes` in batches of 100:

```json
{ "postcodes": ["EC1A 1BB", "E1 7LS", "WC2H 9JQ"] }
```

The response returns `latitude`, `longitude`, and `admin_district` (e.g., "City of London", "Westminster", "Camden"). Unresolvable postcodes receive `NULL` values.

### Step 03: Database Loading (`03_load_companies.py`)
Reads `companies_geocoded.csv` and `sic_codes_clean.csv`, converting NaN float values to Python `None` so psycopg2 inserts genuine SQL `NULL`s. Executes batched inserts:

```sql
INSERT INTO companies (
    company_name, company_number, incorporation_date, full_address,
    postal_code, outcode, latitude, longitude, admin_district
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (company_number) DO NOTHING;
```

### Step 04: Raw Alert Extraction & Deduplication (`04_extract_raw.py`)
Scans all CSV/TSV alert files in `C:/LJMI/data/raw/job_alerts`. Automatically detects delimiter tabs vs. commas and normalizes variations in header naming (`Job Title` vs `job_title`, `Date` vs `date_received`).

#### Deduplication Logic
1. **Exact Deduplication**: Drops duplicate records sharing the same `(source, message_id)`.
2. **30-Day Sliding Window Fuzzy Deduplication**: Sorts postings chronologically by `date_received`. For alerts sharing the same `source` and normalized company name, if an alert occurs within 30 days of a previously kept alert AND their job titles have a token sort ratio $\ge 85\%$ (`fuzz.token_sort_ratio`), the newer alert is flagged as a duplicate and dropped.
3. **Database Delta Filtering**: Queries `SELECT source, message_id FROM job_alerts` and drops any alert key already stored in PostgreSQL, exporting only net-new rows to `raw_jobs_processed.csv`.

### Step 05: Multi-Tier Entity Matching & Classification (`05_process_categories.py`)
Reads `raw_jobs_processed.csv` and applies entity matching and job classification.

#### 1. High-Performance Multi-Tier Entity Matching Engine
To match raw employer names (e.g., "Amazon UK Services Ltd", "Goldman Sachs Int") against $30,000+$ registered London companies, the script employs a four-tier matching pipeline:

* **Tier 0 (Curated Company Aliases)**: Checks `company_aliases.json` for known brand-to-company-number overrides (e.g., `"amazon"` $\rightarrow$ `"03223028"`).
* **Tier 1 (Exact Dictionary Lookup)**: Performs $O(1)$ exact string lookup against normalized company names in `companies`.
* **Inverted Token Index Candidate Pruning**: To avoid $O(N \times M)$ comparisons, an inverted keyword index maps three-letter non-stopword tokens to company indices. For any raw employer name, candidate matches are pruned down to top candidate companies sharing key tokens.
* **Tier 2 (Token Set Ratio)**: Evaluates `fuzz.token_set_ratio` against candidate company names (cutoff score $\ge 90$).
* **Tier 3 (WRatio Fuzzy Fallback)**: Evaluates `fuzz.WRatio` against candidate names (cutoff score $\ge 80$, stored in `MATCH_THRESHOLD`).

```
[Raw Company String: "Amazon UK Services"]
              |
              v
  (Tier 0: company_aliases.json) -------> Match Found? --> [Return Company #, Conf: 100]
              | (No)
              v
  (Tier 1: Exact Dict Lookup) ---------> Match Found? --> [Return Company #, Conf: 100]
              | (No)
              v
  (Inverted Token Index Pruning) ------> Prunes 30,000+ Companies to ~100 Candidates
              |
              v
  (Tier 2: Token Set Ratio >= 90) ------> Match Found? --> [Return Company #, Conf: Score]
              | (No)
              v
  (Tier 3: WRatio >= 80) --------------> Match Found? --> [Return Company #, Conf: Score]
              | (No)
              v
      [Unmatched / Null, Conf: 0]
```

#### 2. Five-Tier Standard Occupational Classification (SOC) Waterfall
Standardizes raw job titles into official 4-digit SOC codes and SOC titles using `soc_lookup.csv`, `soc_synonyms.json`, and `soc_fuzzy_index.json`:

1. **Exact Match**: Checks raw normalized title against `soc_lookup.csv`.
2. **Synonym Lookup**: Resolves industry synonyms via `soc_synonyms.json`.
3. **Noise-Stripped Match**: Strips noise words (`noise_words.json`) and retries exact/synonym matching.
4. **WRatio Fuzzy Search**: Performs fuzzy matching (`fuzz.WRatio`, cutoff $\ge 70$) against canonical SOC titles in `soc_fuzzy_index.json`.
5. **Keyword Fallback**: Matches domain keywords (`keyword_fallback.json`) for specialized roles.

#### 3. Occupational Domain Assignment (`assign_occupational_domain`)
Maps job titles and 2-digit SOC major groups into broad macro-level occupational domains:

```python
def assign_occupational_domain(row):
    title = str(row.get('job_title', '')).lower()
    soc = str(row.get('soc_code', ''))[:2]
    assigned_cat = row.get('job_category', '')

    # 1. Scraper Noise Quarantine
    if 'indeed job alert' in title:
        return 'Scraper Noise', 'Feed Header'

    # 2. Creative & Design Category Preservation
    if assigned_cat not in ['Non-Design', 'Other', '']:
        return 'Creative & Design', assigned_cat

    # 3. SOC-Code Fallback Mapping for Non-Creative Roles
    soc_domain_map = {
        '21': ('Technology & Engineering', 'Software & IT Engineering'),
        '22': ('Healthcare & Science', 'Clinical & Life Sciences'),
        '23': ('Education & Teaching', 'Education & Academic'),
        '24': ('Business, Legal & Finance', 'Corporate Professional'),
        '34': ('Creative & Design', 'General Creative'),
        '35': ('Business & Public Services', 'Associate Professional'),
        '41': ('Administration & Operations', 'Office Administration'),
        '42': ('Customer Support', 'Customer Care'),
        '54': ('Hospitality & Catering', 'Food & Beverage Service'),
        '61': ('Education & Childcare', 'Childcare & Teaching Assistant'),
        '71': ('Sales & Retail', 'Retail & Commercial Sales'),
        '72': ('Customer Services', 'Customer Care Rep'),
        '92': ('Elementary Services', 'Support & Facilities')
    }

    return soc_domain_map.get(soc, ('General Industry', 'General Employment'))
```

#### 4. Foreign Key Safeguards & PostgreSQL Upsert
Before inserting payloads into `job_alerts`, script `05` validates that all `matched_company_number` values exist in the `companies` table. Any orphan company number is converted to `NULL` with `match_status = 'unmatched'`, preventing foreign key constraint failures (`psycopg2.errors.ForeignKeyViolation`).

Alerts are inserted using an `ON CONFLICT (job_key)` upsert clause:

```sql
INSERT INTO job_alerts
    (source, date_received, job_title, company_name_raw, location_raw,
     message_id, job_key, matched_company_number, match_confidence, match_status,
     soc_code, soc_title, occ_category, job_category, is_repeat_flag)
VALUES
    (:source, :date_received, :job_title, :company_name_raw, :location_raw,
     :message_id, :job_key, :matched_company_number, :match_confidence, :match_status,
     :soc_code, :soc_title, :occ_category, :job_category,
     (SELECT EXISTS (SELECT 1 FROM job_alerts WHERE job_key = :job_key))
    )
ON CONFLICT (job_key) DO UPDATE SET
    date_received = EXCLUDED."date_received",
    message_id = EXCLUDED."message_id",
    matched_company_number = EXCLUDED."matched_company_number",
    match_confidence = EXCLUDED."match_confidence",
    match_status = EXCLUDED."match_status",
    soc_code = EXCLUDED."soc_code",
    soc_title = EXCLUDED."soc_title",
    occ_category = EXCLUDED."occ_category",
    job_category = EXCLUDED."job_category",
    is_repeat_flag = TRUE
WHERE job_alerts.date_received < EXCLUDED."date_received" - INTERVAL '30 days';
```

### In-Place Bulk Reclassification (`reclassify_alerts.py`)
`reclassify_alerts.py` provides an in-place maintenance pipeline for existing PostgreSQL records. Whenever company alias lists (`company_aliases.json`), fuzzy match thresholds, or occupational domain rules are updated, `reclassify_alerts.py` recalculates company matching and SOC domains across all existing rows in `job_alerts` and updates the database in chunks of 2,000 records using `sqlalchemy.text()`:

```sql
UPDATE job_alerts
SET matched_company_number = :c_num,
    match_confidence = :score,
    match_status = :status,
    occ_category = :occ_cat,
    job_category = :job_cat
WHERE alert_id = :alert_id;
```

This allows continuous refinement of data quality without wiping the database, truncating tables, or re-processing raw CSV source files.

---

## 6. Data Quality, Deduplication, and Disambiguation Principles

### 1. Agency vs. Direct Employer Disambiguation
Recruitment agencies post high volumes of overlapping job ads that skew labor market demand metrics. The LJMI platform isolates direct corporate hiring through a two-step filtering mechanism:
* **Registry Matching**: Job alerts whose raw company names match registered active companies in `companies` with a confidence score $\ge 80$ (`match_status = 'auto_matched'`) are classified as direct corporate hiring.
* **Agency Flagging & View Isolation**: Unmatched postings or postings matching known recruitment agency company numbers are routed to `v_recruitment_agencies`, while verified corporate postings are exposed via `v_direct_employers`.

### 2. Multi-Level Deduplication & Repeat Flagging
* **Exact Duplicate Prevention**: Unique constraint `unique_source_message_id` on `job_alerts` prevents duplicate alert ingestion from identical email message IDs.
* **Sliding 30-Day Window Deduplication**: In script `04_extract_raw.py`, postings from the same source and company with job title similarity $\ge 85\%$ within 30 days are dropped as feed noise.
* **Historical Repeat Tracking**: Re-posted listings that occur outside the 30-day window trigger the `ON CONFLICT (job_key)` clause in `05_process_categories.py`, setting `is_repeat_flag = TRUE` to enable hiring velocity and repeat-posting analysis.

### 3. Null Handling & Referential Integrity
* All NaN values in pandas DataFrames are converted to Python `None` prior to database execution, ensuring proper SQL `NULL` storage in DECIMAL, DATE, and VARCHAR columns.
* Active foreign key validation in script `05` and `reclassify_alerts.py` prevents orphan company numbers from breaking referential integrity, ensuring reliable joins across `vw_job_alerts_enriched` and downstream Power BI semantic models.
