from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

from common import get_config, print_banner


SCRIPT_ORDER = [
    ('00_init_db.py', False),
    ('01_load_sku_master.py', False),
    ('02_load_sku_cost.py', False),
    ('03_load_order_lines.py', True),
    ('04_load_settlement_lines.py', True),
    ('05_build_order_settlement_bridge.py', True),
    ('06_load_review_orders.py', False),
    ('07_classify_order_types.py', True),
    ('08_load_advertising.py', True),
    ('09_load_storage_fees.py', True),
    ('10_load_removal_fees.py', True),
    ('11_load_compensations.py', True),
    ('12_load_platform_fees.py', True),
    ('13_load_manual_controls.py', False),
    ('14_load_platform_monthly_base.py', True),
    ('15_load_platform_receipts.py', True),
    ('16_build_monthly_finance_views.py', False),
    ('17_run_month_close_checks.py', True),
    ('18_export_manual_worklists.py', True),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description='Phase 1 monthly ETL runner for Amazon finance data.',
    )
    parser.add_argument('target_month', help='Target month in YYYY-MM format. Example: 2026-02')
    parser.add_argument('--skip-init', action='store_true', help='Skip database initialization step')
    return parser.parse_args()


def run_step(script_dir: Path, script_name: str, target_month: str | None) -> None:
    command = [sys.executable, str(script_dir / script_name)]
    if target_month is not None:
        command.append(target_month)
    print_banner(f'Running {script_name}')
    subprocess.run(command, check=True)


def main() -> int:
    args = parse_args()
    config = get_config()
    script_dir = Path(__file__).resolve().parent

    print_banner(f'Monthly ETL started for {args.target_month}')
    print(f'Database path: {config.db_path}')

    for script_name, needs_month in SCRIPT_ORDER:
        if script_name == '00_init_db.py' and args.skip_init:
            continue
        run_step(script_dir, script_name, args.target_month if needs_month else None)

    print_banner(f'Monthly ETL completed for {args.target_month}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
