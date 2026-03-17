from __future__ import annotations

import csv
from pathlib import Path

from common import (
    connect,
    ensure_parent_dir,
    finish_etl_run,
    get_config,
    print_banner,
    register_etl_run,
    utc_now_iso,
)


MANUAL_DIR_NAME = 'manual'
SKU_ALIAS_FILE = 'manual_sku_aliases.csv'
VINE_ALLOC_FILE = 'manual_vine_fee_allocations.csv'
SHARED_COST_FILE = 'manual_shared_costs.csv'
PLATFORM_BASE_FILE = 'manual_platform_monthly_base.csv'
REMOVAL_CONTROL_FILE = 'manual_removal_fee_controls.csv'
CSV_ENCODINGS = ['utf-8-sig', 'utf-8', 'gb18030', 'gbk', 'cp1252', 'latin-1']


def normalize_text(value: object) -> str:
    return str(value).strip() if value is not None else ''


def normalize_alias_value(value: object) -> str:
    return normalize_text(value).lower()


def parse_float(value: object, default: float | None = None) -> float | None:
    text = normalize_text(value)
    if not text:
        return default
    return float(text.replace(',', ''))


def ensure_csv_template(path: Path, headers: list[str]) -> bool:
    ensure_parent_dir(path)
    if path.exists():
        return False
    with path.open('w', encoding='utf-8-sig', newline='') as handle:
        writer = csv.writer(handle)
        writer.writerow(headers)
    return True


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    last_error: Exception | None = None
    for encoding in CSV_ENCODINGS:
        try:
            with path.open('r', encoding=encoding, newline='') as handle:
                reader = csv.DictReader(handle)
                return [dict(row) for row in reader]
        except UnicodeDecodeError as exc:
            last_error = exc
    if last_error:
        raise last_error
    return []


def resolve_manual_sku(conn, raw_value: str, field_name: str) -> str:
    value = normalize_text(raw_value)
    if not value:
        raise ValueError(f'{field_name} is empty')

    direct = conn.execute('SELECT sku FROM dim_sku WHERE sku = ?', (value,)).fetchall()
    if len(direct) == 1:
        return direct[0][0]

    by_name = conn.execute('SELECT sku FROM dim_sku WHERE product_name_cn = ?', (value,)).fetchall()
    unique_name = sorted({row[0] for row in by_name if row[0]})
    if len(unique_name) == 1:
        return unique_name[0]

    alias_value = value.lower()
    alias_rows = conn.execute(
        """
        SELECT sku
        FROM dim_sku_alias
        WHERE alias_type = 'product_name_cn'
          AND alias_value = ?
          AND is_unique_mapping = 1
        """,
        (alias_value,),
    ).fetchall()
    unique_alias = sorted({row[0] for row in alias_rows if row[0]})
    if len(unique_alias) == 1:
        return unique_alias[0]

    manual_alias_rows = conn.execute(
        """
        SELECT sku
        FROM manual_sku_alias
        WHERE alias_type = 'product_name_cn'
          AND alias_value = ?
          AND is_active = 1
        """,
        (alias_value,),
    ).fetchall()
    unique_manual_alias = sorted({row[0] for row in manual_alias_rows if row[0]})
    if len(unique_manual_alias) == 1:
        return unique_manual_alias[0]

    raise ValueError(f'Unable to resolve {field_name} to SKU: {value}')


def resolve_manual_vine_sku(conn, period_month: str, raw_value: str) -> str:
    value = normalize_text(raw_value)
    try:
        return resolve_manual_sku(conn, value, 'manual_vine_fee_allocations.sku')
    except ValueError:
        pass

    candidates = sorted({
        row[0]
        for row in conn.execute('SELECT sku FROM dim_sku WHERE product_name_cn = ?', (value,)).fetchall()
        if row[0]
    })
    if not candidates:
        raise ValueError(f'Unable to resolve manual_vine_fee_allocations.sku to SKU: {value}')

    vine_candidates = sorted({
        row[0]
        for row in conn.execute(
            """
            SELECT DISTINCT sku
            FROM fact_settlement_lines
            WHERE transaction_month = ?
              AND order_type = 'vine_sale'
              AND sku IN ({placeholders})
            """.format(placeholders=','.join('?' for _ in candidates)),
            (period_month, *candidates),
        ).fetchall()
        if row[0]
    })
    if len(vine_candidates) == 1:
        return vine_candidates[0]

    month_candidates = sorted({
        row[0]
        for row in conn.execute(
            """
            SELECT DISTINCT sku
            FROM fact_settlement_lines
            WHERE transaction_month = ?
              AND sku IN ({placeholders})
            """.format(placeholders=','.join('?' for _ in candidates)),
            (period_month, *candidates),
        ).fetchall()
        if row[0]
    })
    if len(month_candidates) == 1:
        return month_candidates[0]

    raise ValueError(f'Unable to resolve manual_vine_fee_allocations.sku to SKU: {value}')


def load_manual_sku_aliases(conn, path: Path) -> int:
    rows = read_csv_rows(path)
    conn.execute('DELETE FROM manual_sku_alias')
    insert_rows = []
    for row in rows:
        alias_type = normalize_text(row.get('alias_type'))
        alias_value = normalize_alias_value(row.get('alias_value'))
        sku = resolve_manual_sku(conn, normalize_text(row.get('sku')), 'manual_sku_alias.sku')
        if not alias_type or not alias_value or not sku:
            continue
        exists = conn.execute('SELECT 1 FROM dim_sku WHERE sku = ?', (sku,)).fetchone()
        if not exists:
            raise ValueError(f'Manual alias references unknown SKU: {sku}')
        source_note = normalize_text(row.get('source_note')) or None
        is_active = int(parse_float(row.get('is_active'), 1) or 0)
        insert_rows.append((alias_type, alias_value, sku, source_note, is_active, utc_now_iso(), utc_now_iso()))

    if insert_rows:
        conn.executemany(
            """
            INSERT INTO manual_sku_alias (
                alias_type,
                alias_value,
                sku,
                source_note,
                is_active,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            insert_rows,
        )
    return len(insert_rows)


def load_manual_vine_allocations(conn, path: Path) -> int:
    rows = read_csv_rows(path)
    conn.execute('DELETE FROM manual_vine_fee_allocations')
    insert_rows = []
    for row in rows:
        period_month = normalize_text(row.get('period_month'))
        sku = resolve_manual_vine_sku(conn, period_month, normalize_text(row.get('sku')))
        fee_amount = parse_float(row.get('fee_amount'))
        if fee_amount is not None:
            fee_amount = abs(fee_amount)
        if not period_month or not sku or fee_amount is None:
            continue
        source_note = normalize_text(row.get('source_note')) or f'manual_csv:{path.name}'
        insert_rows.append((period_month, sku, fee_amount, source_note, utc_now_iso()))

    if insert_rows:
        conn.executemany(
            """
            INSERT INTO manual_vine_fee_allocations (
                period_month,
                sku,
                fee_amount,
                source_note,
                created_at
            ) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(period_month, sku) DO UPDATE SET
                fee_amount = excluded.fee_amount,
                source_note = excluded.source_note
            """,
            insert_rows,
        )
    return len(insert_rows)


def load_manual_shared_costs(conn, path: Path) -> int:
    rows = read_csv_rows(path)
    conn.execute("DELETE FROM manual_shared_costs WHERE source_note LIKE 'manual_csv:%'")
    insert_rows = []
    for row in rows:
        period_month = normalize_text(row.get('period_month'))
        cost_type = normalize_text(row.get('cost_type'))
        total_amount = parse_float(row.get('total_amount'))
        if not period_month or not cost_type or total_amount is None:
            continue
        insert_rows.append(
            (
                period_month,
                cost_type,
                normalize_text(row.get('description')) or None,
                total_amount,
                normalize_text(row.get('currency')) or 'USD',
                normalize_text(row.get('platforms')) or 'all',
                normalize_text(row.get('allocation_method')) or 'revenue_share',
                resolve_manual_sku(conn, normalize_text(row.get('direct_sku')), 'manual_shared_costs.direct_sku') if normalize_text(row.get('direct_sku')) else None,
                normalize_text(row.get('custom_pct_json')) or None,
                f'manual_csv:{path.name}',
                utc_now_iso(),
            )
        )

    if insert_rows:
        conn.executemany(
            """
            INSERT INTO manual_shared_costs (
                period_month,
                cost_type,
                description,
                total_amount,
                currency,
                platforms,
                allocation_method,
                direct_sku,
                custom_pct_json,
                source_note,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            insert_rows,
        )
    return len(insert_rows)


def load_manual_removal_fee_controls(conn, path: Path) -> int:
    conn.execute("""
    CREATE TABLE IF NOT EXISTS manual_removal_fee_controls (
        control_id INTEGER PRIMARY KEY AUTOINCREMENT,
        period_month TEXT NOT NULL,
        order_id TEXT NOT NULL,
        sku TEXT,
        removal_category TEXT NOT NULL,
        accounting_treatment TEXT NOT NULL,
        source_note TEXT,
        created_at TEXT,
        UNIQUE(period_month, order_id)
    )
    """)
    rows = read_csv_rows(path)
    conn.execute('DELETE FROM manual_removal_fee_controls')
    insert_rows = []
    for row in rows:
        period_month = normalize_text(row.get('period_month'))
        order_id = normalize_text(row.get('order_id'))
        sku = normalize_text(row.get('sku')) or None
        if sku:
            sku = resolve_manual_sku(conn, sku, 'manual_removal_fee_controls.sku')
        removal_category = normalize_text(row.get('removal_category')).lower()
        accounting_treatment = normalize_text(row.get('accounting_treatment')).lower()
        if not period_month or not order_id or removal_category not in {'transfer', 'disposal'} or accounting_treatment not in {'expense', 'capitalize'}:
            continue
        insert_rows.append((period_month, order_id, sku, removal_category, accounting_treatment, normalize_text(row.get('source_note')) or f'manual_csv:{path.name}', utc_now_iso()))

    if insert_rows:
        conn.executemany(
            """
            INSERT INTO manual_removal_fee_controls (
                period_month, order_id, sku, removal_category, accounting_treatment, source_note, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            insert_rows,
        )
    return len(insert_rows)


def load_manual_platform_base(conn, path: Path) -> int:
    rows = read_csv_rows(path)
    conn.execute("DELETE FROM dim_platform_monthly_base WHERE source_type = 'manual_csv'")
    insert_rows = []
    for row in rows:
        period_month = normalize_text(row.get('period_month'))
        platform = normalize_text(row.get('platform'))
        net_sales = parse_float(row.get('net_sales'))
        if not period_month or not platform or net_sales is None:
            continue
        insert_rows.append(
            (
                period_month,
                platform,
                net_sales,
                parse_float(row.get('shipped_qty')),
                parse_float(row.get('order_line_count')),
                'manual_csv',
                normalize_text(row.get('source_note')) or f'manual_csv:{path.name}',
                utc_now_iso(),
            )
        )

    if insert_rows:
        conn.executemany(
            """
            INSERT INTO dim_platform_monthly_base (
                period_month,
                platform,
                net_sales,
                shipped_qty,
                order_line_count,
                source_type,
                source_note,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(period_month, platform) DO UPDATE SET
                net_sales = excluded.net_sales,
                shipped_qty = excluded.shipped_qty,
                order_line_count = excluded.order_line_count,
                source_type = excluded.source_type,
                source_note = excluded.source_note,
                created_at = excluded.created_at
            """,
            insert_rows,
        )
    return len(insert_rows)


def main() -> int:
    config = get_config()
    manual_dir = config.base_dir / MANUAL_DIR_NAME
    template_created = 0
    template_created += int(ensure_csv_template(manual_dir / SKU_ALIAS_FILE, ['alias_type', 'alias_value', 'sku', 'source_note', 'is_active']))
    template_created += int(ensure_csv_template(manual_dir / VINE_ALLOC_FILE, ['period_month', 'sku', 'fee_amount', 'source_note']))
    template_created += int(ensure_csv_template(manual_dir / SHARED_COST_FILE, ['period_month', 'cost_type', 'description', 'total_amount', 'currency', 'platforms', 'allocation_method', 'direct_sku', 'custom_pct_json', 'source_note']))
    template_created += int(ensure_csv_template(manual_dir / PLATFORM_BASE_FILE, ['period_month', 'platform', 'net_sales', 'shipped_qty', 'order_line_count', 'source_note']))
    template_created += int(ensure_csv_template(manual_dir / REMOVAL_CONTROL_FILE, ['period_month', 'order_id', 'sku', 'removal_category', 'accounting_treatment', 'source_note']))

    print_banner(f'Loading manual controls from {manual_dir}')
    conn = connect(config.db_path)
    run_id = register_etl_run(
        conn,
        script_name='13_load_manual_controls.py',
        run_type='load_manual_controls',
        status='started',
    )

    try:
        alias_count = load_manual_sku_aliases(conn, manual_dir / SKU_ALIAS_FILE)
        vine_count = load_manual_vine_allocations(conn, manual_dir / VINE_ALLOC_FILE)
        shared_cost_count = load_manual_shared_costs(conn, manual_dir / SHARED_COST_FILE)
        platform_base_count = load_manual_platform_base(conn, manual_dir / PLATFORM_BASE_FILE)
        removal_control_count = load_manual_removal_fee_controls(conn, manual_dir / REMOVAL_CONTROL_FILE)

        conn.commit()
        note = (
            f'templates_created={template_created}; '
            f'manual_aliases={alias_count}; '
            f'vine_allocations={vine_count}; '
            f'shared_costs={shared_cost_count}; '
            f'platform_base_rows={platform_base_count}; '
            f'removal_fee_controls={removal_control_count}'
        )
        finish_etl_run(conn, run_id, 'success', note)
        conn.commit()
        print_banner(note)
        return 0
    except Exception as exc:  # pragma: no cover
        conn.rollback()
        finish_etl_run(conn, run_id, 'failed', str(exc))
        conn.commit()
        raise
    finally:
        conn.close()


if __name__ == '__main__':
    raise SystemExit(main())
