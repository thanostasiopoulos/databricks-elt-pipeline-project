# databricks-elt-pipeline-project


End-to-end cloud-native data platform built on Databricks. Ingests the
[Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
(~100k orders across 9 relational CSVs), processes it through a medallion architecture
using Delta Lake, and serves a business-ready Gold layer.

---

## Architecture

```
[Source: Kaggle CSVs]
        │
        ▼
┌─────────────────┐
│   Bronze Layer  │  Raw Delta tables · schema-on-read · metadata columns
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Silver Layer  │  Cleaned · typed · deduplicated · star schema dims + facts
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│    Gold Layer   │  Business aggregates · KPI tables · analytics-ready
└────────┬────────┘
         │
         ▼
  Databricks SQL
```

**Compute:** Serverless (Databricks Free Edition)  
**Storage:** Delta Lake (ACID, time travel, schema enforcement)  
**Catalog:** Unity Catalog — `olist` catalog → `bronze` / `silver` / `gold` schemas  
**Orchestration:** Databricks Workflows (notebook task DAG)  
**Testing:** `pytest` executed as a Workflow task  
**Version control:** Databricks Repos → GitHub

---

## Dataset

[Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

| Source file | Description |
|---|---|
| `olist_orders_dataset.csv` | Order lifecycle and status |
| `olist_order_items_dataset.csv` | Line items per order |
| `olist_order_payments_dataset.csv` | Payment method and value |
| `olist_order_reviews_dataset.csv` | Customer review scores and comments |
| `olist_customers_dataset.csv` | Customer location |
| `olist_sellers_dataset.csv` | Seller location |
| `olist_products_dataset.csv` | Product metadata |
| `product_category_name_translation.csv` | Category name translations (PT → EN) |
| `olist_geolocation_dataset.csv` | Zip code geolocation |

Incremental ingestion is simulated by partitioning on `order_purchase_timestamp` and
loading one month at a time via a Workflow parameter.

---

## Medallion Layers

### Bronze
- One Delta table per source CSV, written as-is
- Metadata columns appended: `_ingested_at` (timestamp), `_source_file` (string)
- Partitioned by `_ingested_month` where applicable
- Full history retained — no deletions

### Silver
- **Dimensions (Type 1):** `dim_customer`, `dim_seller`, `dim_product`, `dim_date`
- **Facts:** `fact_orders`, `fact_order_items`, `fact_reviews`
- Transformations: null handling, type casting, deduplication, English category names joined in
- `fact_reviews` includes a `sentiment` column: `negative` (score 1–2), `neutral` (score 3), `positive` (score 4–5)
- Data quality assertions run at each Bronze → Silver boundary (null rates, row counts, referential integrity)

> **Note:** SCD Type 2 implementation on `dim_customer` and `dim_seller` is planned for a future phase.

### Gold
| Table | Description |
|---|---|
| `gld_gmv_monthly` | Gross Merchandise Value by month and product category |
| `gld_delivery_performance` | Actual vs. estimated delivery delta by state and seller |
| `gld_seller_scorecard` | Per-seller GMV, order count, avg review score, on-time rate |
| `gld_review_summary` | Sentiment distribution and avg score over time |

---

## Repo Structure

```
olist-databricks/
├── notebooks/
│   ├── bronze/
│   │   └── ingest_raw.py               # Parameterised Bronze ingestion
│   ├── silver/
│   │   ├── dim_customer.py
│   │   ├── dim_seller.py
│   │   ├── dim_product.py
│   │   ├── dim_date.py
│   │   ├── fact_orders.py
│   │   ├── fact_order_items.py
│   │   └── fact_reviews.py             # Includes sentiment bucketing
│   ├── gold/
│   │   ├── gld_gmv_monthly.py
│   │   ├── gld_delivery_performance.py
│   │   ├── gld_seller_scorecard.py
│   │   └── gld_review_summary.py
│   └── utils/
│       ├── catalog_setup.py            # Unity Catalog bootstrap
│       └── data_quality.py             # Assertion framework
├── tests/
│   └── test_silver_transforms.py       # pytest suite, runs as Workflow task
├── workflows/
│   └── olist_pipeline.yml              # Databricks Workflow definition (YAML)
├── sql/
│   └── gold_queries.sql                # Reference queries for each Gold table
├── docs/
└── README.md
```

---

## Pipeline DAG

```
ingest_bronze
     │
     ▼
transform_silver
     │
     ▼
build_gold
     │
     ▼
run_tests
     │
     ▼
notify_on_failure  (email via Databricks Workflow alert)
```

---

## Setup


### Steps

1. **Download the dataset** from Kaggle and upload CSVs to your Databricks workspace volume or DBFS path.

2. **Clone this repo** into Databricks Repos:
   - Workspace → Repos → Add Repo → paste this repo URL

3. **Run catalog setup:**
   - Open `notebooks/utils/catalog_setup.py` and run it once to create the `olist` catalog and all schemas.

4. **Configure the Workflow:**
   - Import `workflows/olist_pipeline.yml` via the Databricks Workflows UI or CLI.
   - Set the `ingestion_month` parameter (e.g. `2017-01`) for your first run.

5. **Trigger the pipeline** and monitor task progress in the Workflow UI.

---

## Design Decisions

- **Serverless-only compute** — Free Edition constraint; all notebooks are written to be stateless and re-runnable.
- **Delta Lake for all layers** — enables time travel, ACID guarantees, and `MERGE` semantics even in Bronze.
- **Sentiment via rule-based bucketing** — review scores are ordinal and reliable; ML-based NLP would add complexity without meaningful accuracy gain on this dataset.
- **SCD Type 2 deferred** — Olist source data is static; Type 2 will be introduced in a later phase with simulated change data.
- **No external orchestrator** — Databricks Workflows is the native, zero-config option on Free Edition; Airflow integration is architecturally straightforward to add later.

---

## Future Work

- [ ] SCD Type 2 on `dim_customer` and `dim_seller`
- [ ] Autoloader-based ingestion replacing manual CSV upload
- [ ] Great Expectations or Delta constraints for richer data quality
- [ ] Power BI connector integration
- [ ] CI/CD via GitHub Actions (lint, test on PR)
