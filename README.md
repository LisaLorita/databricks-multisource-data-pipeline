# Databricks Multi-Source Data Pipeline

A data engineering project focused on integrating data from multiple sources using **Databricks, PySpark and Delta Lake**.

## Objectives

- Ingest data from different sources and formats (CSV, JSON).
- Unify customers from different systems into a common (canonical) model.
- Implement parameterized and incremental data ingestion.
- Clean and transform data across Bronze, Silver and Gold layers.
- Validate data quality and quarantine invalid records with a rejection reason.
- Track pipeline executions and data processing results.

## Architecture

**Landing (Volume) → Bronze → Silver (+ quarantine) → Gold → Validated output**

Cross-cutting: audit table and watermarks in an `ops` schema.

## Technologies

- Databricks (Free Edition, serverless)
- Unity Catalog & Volumes
- PySpark
- Delta Lake
- SQL
- Git / GitHub

## Repository structure

```
docs/        Architecture documentation
notebooks/   Databricks notebooks (source .py format)
src/         Shared config and helpers
```

## Roadmap

- [x] 0. Workspace setup (catalog, schemas, volume, Git folder)
- [x] 1. Synthetic data generation
- [x] 2. Bronze ingestion (FULL)
- [ ] 3. Silver transformations
- [ ] 4. Data quality rules and quarantine
- [ ] 5. Parameterized runs
- [ ] 6. Incremental load with watermark and MERGE
- [ ] 7. Gold layer and JSON delivery
- [ ] 8. Audit table and end-to-end validation
- [ ] 9. Databricks Job orchestration
- [ ] 10. Documentation and architecture diagram

## Project Status

🚧 In development.

This project is designed to practice data integration, incremental processing, data cleansing and data quality validation in a Databricks environment.

## Disclaimer

All datasets used in this project are synthetic and intended for learning purposes.
