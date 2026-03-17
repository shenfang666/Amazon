# Amazon Finance ETL

This folder contains the Phase 1 ETL skeleton for the Amazon finance system.

## Scripts

- `00_init_db.py`: initialize the SQLite database schema
- `common.py`: shared config, logging, hashing, and SQLite helpers
- `schema.py`: SQL schema definitions
- `99_run_monthly.py`: placeholder monthly runner

## Usage

```powershell
python etl/00_init_db.py
```

By default, the database file is created at:

```text
amazon_finance.db
```

You can override it with:

```powershell
$env:AMAZON_FINANCE_DB="E:\path\to\amazon_finance.db"
python etl/00_init_db.py
```

- 13_load_manual_controls.py: loads manual SKU aliases, vine allocations, shared costs, and platform-base overrides from manual/*.csv.

- 17_run_month_close_checks.py: writes row-level blocker/warning details into monthly_close_issue_detail.

- 18_export_manual_worklists.py: exports month-close issue details and pending alias worklists into manual/.
