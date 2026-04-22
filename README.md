# Olist E-Commerce ELT Pipeline

End-to-end cloud-native ELT pipeline built on Databricks Free Edition. Ingests the
[Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
(~100k orders across 9 relational CSVs), processes it through a medallion architecture
using Delta Lake and Unity Catalog, and serves a business-ready Gold layer of KPI tables.

---

## Architecture

```
[Source: Kaggle CSVs]
        │
        ▼
┌─────────────────┐
│   Bronze Layer  │  Raw Delta tables · schema-on-read · metadata columns
│                 │  Incremental by month (replaceWhere)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Silver Layer  │  Cleaned · typed · deduplicated
│                 │  Star schema: 4 dims + 3 facts
│                 │  Data quality assertions at every boundary
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│    Gold Layer   │  Business KPI aggregates · analytics-ready Delta tables
└─────────────────┘
```

**Compute:** Serverless (Databricks Free Edition)  
**Storage format:** Delta Lake — ACID transactions, time travel, schema enforcement  
**Catalog:** Unity Catalog — `olist` catalog → `bronze` / `silver` / `gold` schemas  
**Orchestration:** Databricks Workflows (notebook task DAG, defined as YAML)  
**Version control:** GitHub → Databricks Git folders  

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
loading one month at a time via an `ingestion_month` Workflow parameter. The pipeline
is demonstrated across 3 months (2017-01 → 2017-03).

---

## Medallion Layers

### Bronze
- One Delta table per source CSV, written as-is (schema-on-read)
- Metadata columns appended: `_ingested_at`, `_source_file`, `_ingestion_month`
- Reference tables (customers, sellers, products, geolocation, category translations) — full overwrite
- Order-related tables (orders, items, payments, reviews) — partitioned by `_ingestion_month`, idempotent via `replaceWhere`

### Silver

**Dimensions (Type 1):**

| Table | Key | Notes |
|---|---|---|
| `dim_customer` | `customer_unique_id` | Deduped from order-scoped `customer_id` |
| `dim_seller` | `seller_id` | State uppercased, whitespace trimmed |
| `dim_product` | `product_id` | Portuguese category names replaced with English |
| `dim_date` | `date_id` | Generated spine 2016-01-01 → 2018-12-31 |

**Facts:**

| Table | Grain | Notes |
|---|---|---|
| `fact_orders` | `order_id` | Delivery delta days, on-time flag, purchase-to-delivery cycle |
| `fact_order_items` | `order_id` + `order_item_id` | `total_item_value` = price + freight |
| `fact_reviews` | `review_id` | Rule-based sentiment: negative (1–2) / neutral (3) / positive (4–5) |

Data quality assertions run at every Bronze → Silver boundary:
- `assert_no_nulls` — key columns must not be null
- `assert_no_duplicates` — natural keys must be unique
- `assert_min_row_count` — minimum row threshold
- `assert_row_count_delta` — Silver must not drop more than N% of Bronze rows

### Gold

| Table | Description |
|---|---|
| `gld_gmv_monthly` | Gross Merchandise Value by month and product category (delivered orders only) |
| `gld_delivery_performance` | On-time rate, avg delivery delta, and fulfilment cycle time by state and month |
| `gld_seller_scorecard` | Per-seller GMV, order count, avg review score, on-time rate, sentiment breakdown |
| `gld_review_summary` | Sentiment distribution and avg review score by month |

---

## Pipeline DAG

```
silver_dim_customer
    ├── silver_dim_seller ──┐
    ├── silver_dim_product ─┼── bronze_2017_01 → fact_orders → fact_order_items
    └── silver_dim_date ────┘                              └── fact_reviews
                                    └── bronze_2017_02 → fact_orders → fact_order_items
                                                                   └── fact_reviews
                                            └── bronze_2017_03 → fact_orders → fact_order_items
                                                                           └── fact_reviews
                                                    └── gld_gmv_monthly
                                                        gld_delivery_performance
                                                        gld_seller_scorecard
                                                        gld_review_summary
```

Dims run once at the start (static, no month dependency). Facts run sequentially per
month. All 4 Gold tables run in parallel after all months are complete. Pipeline stops
on first failure.

---

## Repo Structure

```
databricks-elt-pipeline-project/
├── notebooks/
│   ├── bronze/
│   │   └── ingest_raw.py               # Parameterised Bronze ingestion (widget: ingestion_month)
│   ├── silver/
│   │   ├── dim_customer.py
│   │   ├── dim_seller.py
│   │   ├── dim_product.py              # Joins English category translations
│   │   ├── dim_date.py                 # Generated date spine, no source file needed
│   │   ├── fact_orders.py              # Delivery metrics, widget: ingestion_month
│   │   ├── fact_order_items.py         # Line-item grain, widget: ingestion_month
│   │   └── fact_reviews.py             # Sentiment bucketing, widget: ingestion_month
│   ├── gold/
│   │   ├── gld_gmv_monthly.py
│   │   ├── gld_delivery_performance.py
│   │   ├── gld_seller_scorecard.py
│   │   └── gld_review_summary.py
│   └── utils/
│       ├── config.py                   # Centralized configuration (env-aware, dot notation)
│       ├── data_quality.py             # Assertion framework (nulls, duplicates, row count deltas)
│       └── logging_utils.py            # Structured logging (PipelineLogger, timed operations, metrics)
├── workflows/
│   └── databricks-elt-pipeline.yml     # Databricks Workflow definition (DABs YAML format)
└── README.md
```

---

## Code Quality & Observability

### Centralized Configuration (`config.py`)
All notebooks reference a single `Config` class for table names, schemas, and catalog:
- **Environment-aware**: Supports dev/staging/prod modes (currently defaulting to prod)
- **Dot notation**: `config.catalog`, `config.silver.fact_orders`, `config.gold.gld_gmv_monthly`
- **No hardcoded strings**: Eliminates copy-paste errors and makes refactoring safe

### Structured Logging (`logging_utils.py`)
Production-grade `PipelineLogger` replaces all print statements:
- **Execution timing**: `with logger.timed_operation("Extract source tables"):`
- **DataFrame metrics**: `logger.log_dataframe_metrics(df, stage="Extract", table_name="fact_orders")`
- **Structured output**: JSON-serializable logs with timestamps, notebook context, and metric collection
- **Error context**: Automatic error enrichment with operation name and duration

### Data Quality Framework (`data_quality.py`)
Reusable assertion functions applied at Bronze → Silver boundaries:
- `assert_no_nulls(df, columns)` — validates required columns have no null values
- `assert_no_duplicates(df, columns)` — ensures uniqueness on natural keys
- `assert_min_row_count(df, min_count)` — catches empty or suspiciously small tables
- `assert_row_count_delta(bronze_df, silver_df, max_drop_pct)` — flags excessive row loss during transformation

---

## Design Decisions

- **ELT not ETL** — data lands raw in Bronze first; all transformation happens inside Databricks using Delta Lake and Spark. No transformation occurs before loading.
- **Serverless-only compute** — Free Edition constraint; all notebooks are stateless and re-runnable without cluster management.
- **Delta Lake for all layers** — enables ACID guarantees, time travel, and `replaceWhere` for idempotent partition-level overwrites at every layer.
- **Simulated incrementalism** — Olist is a static dataset; month-by-month ingestion is simulated via an `ingestion_month` widget parameter and `replaceWhere` partitioning to demonstrate production-realistic pipeline design.
- **Dims run once, facts run per month** — dimensions are static and have no month dependency; separating their execution from the incremental fact load avoids redundant full-overwrites on every pipeline run.
- **Single end-to-end Workflow** — appropriate for a single-developer setup; in a multi-team production environment this would be split into layer-scoped jobs with watermark-driven triggering rather than a hardcoded month list.
- **Modular utilities** — config, logging, and data quality are centralized in `notebooks/utils/` and imported via `%run` magic commands, eliminating code duplication across 13 notebooks.

---

## Known Data Quality Issues

| Issue | Affected table | Rows | Decision |
|---|---|---|---|
| `customer_id` values in `olist_orders` with no corresponding record in `olist_customers` | `fact_orders` | ~51 per month | Dropped — count logged at runtime |
| Malformed timestamp values in `review_answer_timestamp` caused by unescaped commas in review text corrupting CSV column alignment | `fact_reviews` | Small number | `try_cast` used — malformed values set to null |

---

## Future Work

- [ ] Notebook standardization (consistent cell titles, execution order documentation)
- [ ] Enhanced error handling (try-catch blocks, input validation, graceful degradation)
- [ ] SCD Type 2 on `dim_customer` and `dim_seller`
- [ ] Databricks Asset Bundles (DABs) for CI/CD and environment separation (dev/prod)
- [ ] Watermark-driven month detection replacing hardcoded month list
- [ ] CI/CD via GitHub Actions (lint, test on PR)
- [ ] Power BI connector integration
