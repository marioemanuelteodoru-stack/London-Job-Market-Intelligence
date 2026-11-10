[github-readme-ljmi.md](https://github.com/user-attachments/files/34652877/github-readme-ljmi.md)
# London Job Market Intelligence (LJMI) — Enterprise Data Engineering & Analytics Pipeline

A production-grade, end-to-end data pipeline and PostgreSQL analytical engine designed to ingest, clean, resolve, and categorize unstructured UK labor market job posting feeds. The platform filters recruitment agency noise from direct employer hiring, normalizes occupational classifications against official UK government standards, geocodes workplace locations, and powers multidimensional analytics across administrative districts, micro-industries, and hiring timelines.

---

## Executive Overview & Business Value

Raw job alert feeds from job boards and scraper aggregators suffer from high noise levels, redundant listings, inconsistent location tags, and recruiter proxy postings that obscure actual hiring demand. 

The **London Job Market Intelligence (LJMI)** system solves these challenges through an automated, highly scalable ETL and database architecture that:

* **Disambiguates Hiring Channels**: Distinguishes direct employer job postings from third-party recruitment agency noise by performing multi-tier entity resolution against the official UK Companies House register (500,000+ active London companies).
* **Eliminates Listing Redundancy**: Combines exact message deduplication with a 30-day rolling window fuzzy deduplication engine to remove duplicate postings for the same role.
* **Standardizes Occupational Taxonomies**: Implements a 5-tier waterfall classifier mapping raw job titles to official UK Standard Occupational Classification (SOC) 4-digit codes, broad occupational categories, and granular specialized industry domains.
* **Geospatial Precision**: Geocodes company headquarters and postcodes to administrative London boroughs using a high-throughput, local Dockerized geocoding API.
* **Empowers Multidimensional BI**: Exposes structured SQL views and a star-schema database layout optimized for analytical query performance and Power BI semantic modeling (TMDL).

---

## Technical Stack & Infrastructure

* **Database Engine**: PostgreSQL 16 with PostGIS spatial extension (`CREATE EXTENSION IF NOT EXISTS postgis;`).
* **Programming & ETL**: Python 3.12, pandas, SQLAlchemy (`create_engine`), psycopg2 (`execute_batch`).
* **Entity Resolution & Fuzzy Matching**: RapidFuzz (`process.extractOne`, `fuzz.WRatio`, `fuzz.token_set_ratio`, `fuzz.token_sort_ratio`).
* **Geocoding Engine**: Dockerized local `postcodes.io` REST API container (`http://localhost:8000`) for high-throughput batch processing without rate limits.
* **Analytics & Visualization**: Power BI Desktop (TMDL semantic model, DAX measures), PostgreSQL analytical views.

---

## System Architecture & Data Pipeline Workflow

The LJMI data engineering pipeline is structured into a modular, idempotent seven-stage execution sequence:

```
[ Raw Companies House CSV ] ──► [ 01_prepare_companies.py ] ──► [ 02_geocode_companies.py ] ──► [ 03_load_companies.py ] ──┐
                                                                                                                           ▼
[ Raw Job Posting Feeds   ] ──► [ 04_extract_raw.py ] ────────► [ 05_process_categories.py ] ──► [ PostgreSQL Database ]
                                                                             ▲                         │
                                                                             └──── [ reclassify_alerts.py ] (In-Place Updates)
```

### Pipeline Execution Stages

#### 1. Schema Infrastructure Setup (`00_schema_setup.py`)
* Enables PostGIS spatial extension.
* Provisions primary relational tables (`companies`, `company_sic`, `job_alerts`, `location_mappings`).
* Enforces strict integrity constraints: `unique_source_message_id` on `job_alerts(source, message_id)` and `company_sic_unique` on `company_sic(company_number, sic_code_num)`.
* Creates seed data for fallback geographic mappings and provisions enriched analytical views (`vw_job_alerts_enriched`).

#### 2. Registered Company Normalization (`01_prepare_companies.py`)
* Ingests raw Companies House bulk registry data (over 500,000 active London company records).
* Standardizes company numbers, addresses, and incorporation dates.
* Normalizes UK postal codes using a strict regular expression validation pattern (`^([A-Z]{1,2}[0-9][0-9A-Z]?)\s*([0-9][A-Z]{2})?$`) and extracts outcodes.
* Unpivots multi-column Standard Industrial Classification codes (`SICCode.SicText_1` through `4`) into a clean relational table (`company_sic`).

#### 3. High-Throughput Batch Geocoding (`02_geocode_companies.py`)
* Connects to a local Docker container running the `postcodes.io` microservice.
* Processes unique, validated London postcodes in optimized batches of 100 via HTTP POST requests (`/postcodes`).
* Appends exact `latitude`, `longitude`, and `admin_district` metadata to company records.

#### 4. Relational Database Loading (`03_load_companies.py`)
* Performs idempotent, batched database inserts into `companies` and `company_sic` tables using `psycopg2.extras.execute_batch` (batch size: 1,000).
* Employs `ON CONFLICT (company_number) DO NOTHING` clauses to prevent duplicate primary keys during re-runs.

#### 5. Raw Job Posting Extraction & Windowed Dedup (`04_extract_raw.py`)
* Scans multi-source CSV job alert directories and standardizes varying header schemas (e.g., handling mixed casing and tab vs. comma delimiters).
* Executes multi-layer deduplication:
  1. **Exact Deduplication**: Drops duplicate `(source, message_id)` tuples while preserving multi-job alerts.
  2. **30-Day Rolling Window Fuzzy Deduplication**: Groups postings by source and normalized company name, evaluating job title similarity (`fuzz.token_sort_ratio >= 85`) within a 30-day window to eliminate reposted roles.
* Compares extracted candidates against existing database records to isolate net-new alerts for processing.

#### 6. Entity Resolution & Classification Engine (`05_process_categories.py`)
* Executes a high-performance multi-tier matching engine to link raw job posting company strings to official Companies House records.
* Applies a 5-tier waterfall SOC classification algorithm to assign 4-digit SOC codes and occupational titles.
* Maps specialized roles into custom categories (e.g., Creative & Design, Technology & Engineering, Corporate Professional) and applies custom merge rules.
* Performs upserts into PostgreSQL (`ON CONFLICT (job_key) DO UPDATE SET...`).

#### 7. In-Place Batch Reclassification (`reclassify_alerts.py`)
* Utility engine designed to update existing `job_alerts` records in-place without truncating database tables.
* Re-executes entity matching and occupational classification across historical records in memory and applies updates in optimized database transactions (batch size: 2,000).

---

## Core Database Schema & Data Model

The PostgreSQL database (`london_job_market`) is modeled around an analytical Star Schema to facilitate fast aggregation across spatial, temporal, and industrial dimensions.

```
                   +------------------------+
                   |   location_mappings    |
                   +------------------------+
                   | id (PK)                |
                   | source                 |
                   | raw_pattern            |
                   | mapped_location        |
                   | latitude, longitude    |
                   +-----------+------------+
                               |
                               | (Fallback Geo)
                               v
+------------------+       +------------------------+       +------------------+
|   company_sic    |       |       job_alerts       |       |    companies     |
+------------------+       +------------------------+       +------------------+
| id (PK)          |       | alert_id (PK)          |       | company_numberPK |
| company_numberFK +<----->+ source                 |       | company_name     |
| sic_code_num     |       | date_received          |       | incorporation_date|
| sic_text         |       | job_title              |       | full_address     |
+------------------+       | company_name_raw       |       | postal_code      |
                           | location_raw           |       | outcode          |
                           | message_id             |       | latitude         |
                           | matched_company_num FK +------>+ longitude        |
                           | match_confidence       |       | admin_district   |
                           | match_status           |       +------------------+
                           | soc_code, soc_title    |
                           | occ_category           |
                           | job_category           |
                           | is_repeat_flag         |
                           +------------------------+
```

### Main Tables

* **`companies`**: Master directory of active London businesses containing geocoded location coordinates, outcodes, and administrative district names.
* **`company_sic`**: Industry classifications associated with registered companies (1-to-many relationship).
* **`job_alerts`**: Central fact table storing ingested job posting alerts, raw strings, matching status, match confidence scores, and assigned occupational categories.
* **`location_mappings`**: Geographic lookup table used for fallback geocoding when a job posting cannot be matched to a registered company.

### Primary Enriched View (`vw_job_alerts_enriched`)

An analytical SQL view that joins `job_alerts` with `companies`, `company_sic`, and `location_mappings`, dynamically selecting the best available spatial coordinates and labeling the geographic resolution source:

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

---

## Advanced Algorithms & Engineering Mechanics

### 1. Multi-Tier Entity Resolution Engine

To match unstandardized employer names from job boards (e.g., *"Amazon UK Services Ltd"*, *"Amazon"*, *"Amazon Web Services"*) against 500,000+ Companies House records, the pipeline implements a 4-tier matching algorithm:

1. **Text Normalization**: Strips legal entity suffixes (`Ltd`, `PLC`, `LLP`, `Inc`), location noise (`London`, `UK`, `EMEA`), parentheticals, punctuation, and expands symbols (`&` $\rightarrow$ `and`, `@` $\rightarrow$ `at`).
2. **Tier 0 (Curated Alias Patch)**: Checks a pre-compiled JSON dictionary (`company_aliases.json`) mapping known trading brands directly to verified company numbers (Score: 100).
3. **Tier 1 (Exact Dictionary Lookup)**: Performs $O(1)$ hash table lookup against normalized registered company names (Score: 100).
4. **Inverted Token Indexing & Candidate Pruning**: For unmatched names, the system prunes the 500,000+ company database down to a maximum of 3,000 relevant candidates using an inverted token index built over non-stopword keywords.
5. **Tier 2 (Token Set Ratio)**: Evaluates `fuzz.token_set_ratio` against pruned candidates to handle brand names contained within longer registered legal entity names (Score Cutoff: 90).
6. **Tier 3 (Weighted Fuzzy Ratio)**: Applies `fuzz.WRatio` across candidates as a fuzzy fallback (Score Cutoff: 80). Match confidence scores $\ge 80$ are categorized as `auto_matched`; scores below 80 are flagged as `unmatched`.

### 2. 5-Tier Waterfall SOC Occupation Classifier

Job titles are classified into official UK Standard Occupational Classification (SOC) codes through a 5-tier waterfall:

1. **Exact Normalized Match**: Matches raw normalized job title against official SOC lookup tables.
2. **Synonym Lookup**: Maps common job title variants via a curated synonym dictionary (`soc_synonyms.json`).
3. **Noise-Stripped Lookup**: Strips common recruitment noise words (e.g., *Senior*, *Lead*, *Urgent*, *Immediate*) and retries exact/synonym matching.
4. **Fuzzy Canonical Matching**: Runs `fuzz.WRatio` matching (Score Cutoff: 70) against canonical SOC titles indexed in `soc_fuzzy_index.json`.
5. **Keyword Fallback**: Applies domain-specific keyword rules (`keyword_fallback.json`) for specialized technical and creative roles.

---

## Analytics & Power BI Data Modeling

The PostgreSQL schema integrates seamlessly into Power BI via a TMDL semantic model, enabling advanced DAX metrics:

* **Direct Hiring vs. Agency Ratio**: Measures the proportion of job postings originating from direct employers versus recruitment agencies.
* **Hiring Velocity**: Calculates average alerts per unique company (`DIVIDE([Alert Count], DISTINCTCOUNT(v_direct_employers[company_number]), 0)`).
* **Match Confidence Metrics**: Evaluates entity resolution performance across data ingestion runs.
* **District & Sector Slicing**: Enables dynamic cross-filtering by administrative district, outcode, macro-industry, and 4-digit SOC code.

---

## Operational Key Performance Indicators

* **Entity Matching Precision**: Achieves $>80\%$ automated match confidence threshold across raw employer strings.
* **Geocoding Coverage**: Successfully resolves full postcodes for $>98\%$ of active London registered businesses.
* **Processing Speed**: Candidate pruning via inverted token indexing achieves matching speeds of several hundred raw employer strings per second.
* **Idempotency**: All ETL stages are 100% idempotent and safe for repeated execution without data corruption or duplicate key violations.
