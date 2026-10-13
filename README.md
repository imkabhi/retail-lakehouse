# Retail Lakehouse Capstone

An end-to-end retail data pipeline on Databricks: daily POS, product and store files flow
through Bronze, Silver and Gold layers, with data quality checks, orchestration, CI/CD and governance.

## Layout (grows each week)

```
retail-lakehouse/
  generator/generate_data.py      # creates messy daily retail files (standard library only)
  notebooks/01_setup_and_bronze.py   # Unity Catalog setup + Auto Loader into Bronze
  .gitignore
  README.md
```

## Part 1: run it

```bash
# 1. generate 7 days of files locally (nothing to install)
python generator/generate_data.py --out ./landing

# 2. in Databricks: import notebooks/01_setup_and_bronze.py, run Steps 1-2,
#    upload day 1-3 files to the volume folders, then run Steps 3-5
```

## Status
- [x] Part 1: generator, landing volume, Bronze ingestion
- [ ] Part 2: Silver (dedupe, quality checks, quarantine, SCD2)
- [ ] Part 3: Gold star schema and KPI marts
- [ ] Part 4: Airflow, CI/CD, observability, BigQuery leg
