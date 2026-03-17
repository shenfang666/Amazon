from __future__ import annotations

import argparse
import csv
from pathlib import Path

from common import (
    connect,
    finish_etl_run,
    get_config,
    print_banner,
    record_file_import,
    register_etl_run,
    sha256_text,
    stable_json,
    utc_now_iso,
)


FILE_PATTERNS = [
    '*Payout*Amazon*.csv',
    '*Payment*Amazon*.csv',
    '*Receipt*Amazon*.csv',
    '*Receivable*Amazon*.csv',
]
CSV_ENCODINGS = ['utf-8-sig', 'utf-8', 'gb18030', 'gbk', 'cp1252', 'latin-1']
FIELD_ALIASES = {
    'receipt_date': ['receipt-date', 'receipt_date', 'posted-date', 'posted_date', 'payment-date', 'payment_date', 'date'],
    'receipt_reference': ['receipt-reference', 'receipt_reference', 'reference', 'payment-reference', 'payment_reference', 'transaction-id', 'transaction_id'],
    'settlement_id': ['settlement-id', 'settlement_id', 'settlement-id(s)', 'settlement'],
    'currency': ['currency', 'currency-code', 'currency_code'],
    'receipt_amount': ['amount', 'receipt-amount', 'receipt_amount', 'payment-amount', 'payment_amount', 'net-amount', 'net_amount'],
    'receipt_type': ['type', 'receipt-type', 'receipt_type', 'transaction-type', 'transaction_type'],
    'memo': ['memo', 'description', 'notes', 'remark'],
}


def normalize_month(value: str) -> tuple[str, str]:
    text = value.strip()
    if len(text) == 7 and '-' in text:
        return text, text.replace('-', '')
    if len(text) == 6 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}", text
    raise ValueError('target_month must be YYYY-MM or YYYYMM')


def normalize_text(value: object) -> str:
    return str(value).strip() if value is not None else ''


def parse_float(value: object) -> float:
    text = normalize_text(value).replace(',', '')
    if not text:
        return 0.0
    if text.startswith('(') and text.endswith(')'):
        text = '-' + text[1:-1]
    return float(text)


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    last_error: Exception | None = None
    for encoding in CSV_ENCODINGS:
        try:
            with path.open('r', encoding=encoding, newline='') as handle:
                sample = handle.read(4096)
                handle.seek(0)
                try:
                    dialect = csv.Sniffer().sniff(sample)
                except csv.Error:
                    dialect = csv.excel
                reader = csv.DictReader(handle, dialect=dialect)
                return [dict(row) for row in reader]
        except UnicodeDecodeError as exc:
            last_error = exc
    if last_error:
        raise last_error
    return []


def pick_field(row: dict[str, str], logical_name: str) -> str:
    normalized_map = {normalize_text(k).lower(): v for k, v in row.items()}
    for candidate in FIELD_ALIASES[logical_name]:
        value = normalized_map.get(candidate.lower())
        if normalize_text(value):
            return normalize_text(value)
    return ''


def build_row_hash(source_file: str, row: dict[str, object]) -> str:
    return sha256_text(stable_json({'source_file': source_file, 'row': row}))


def discover_files(base_dir: Path) -> list[Path]:
    matches: list[Path] = []
    for pattern in FILE_PATTERNS:
        matches.extend(base_dir.glob(pattern))
    unique = sorted({path.resolve() for path in matches if path.is_file()})
    return [Path(path) for path in unique]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Load platform receipt files into fact_platform_receipts.')
    parser.add_argument('target_month', help='YYYY-MM or YYYYMM')
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    month_text, _ = normalize_month(args.target_month)
    config = get_config()
    source_files = discover_files(config.base_dir)

    print_banner(f'Loading platform receipts for {month_text}')
    conn = connect(config.db_path)
    run_id = register_etl_run(
        conn,
        script_name='15_load_platform_receipts.py',
        run_type='load_platform_receipts',
        target_month=month_text,
        status='started',
    )

    try:
        conn.execute('DELETE FROM fact_platform_receipts WHERE period_month = ?', (month_text,))
        insert_rows: list[tuple] = []
        loaded_files = 0
        for source_path in source_files:
            rows = read_csv_rows(source_path)
            eligible = []
            for row in rows:
                receipt_date = pick_field(row, 'receipt_date')
                if receipt_date[:7] != month_text:
                    continue
                receipt_amount = parse_float(pick_field(row, 'receipt_amount'))
                payload = {
                    'receipt_date': receipt_date,
                    'receipt_reference': pick_field(row, 'receipt_reference'),
                    'settlement_id': pick_field(row, 'settlement_id'),
                    'currency': pick_field(row, 'currency') or 'USD',
                    'receipt_amount': receipt_amount,
                    'receipt_type': pick_field(row, 'receipt_type') or 'payout',
                    'memo': pick_field(row, 'memo'),
                }
                row_hash = build_row_hash(str(source_path), payload)
                eligible.append((
                    str(source_path),
                    row_hash,
                    month_text,
                    payload['receipt_date'],
                    payload['receipt_reference'] or None,
                    payload['settlement_id'] or None,
                    'amazon',
                    '',
                    payload['currency'],
                    payload['receipt_amount'],
                    payload['receipt_type'],
                    payload['memo'] or None,
                    utc_now_iso(),
                ))
            if not eligible:
                continue
            loaded_files += 1
            record_file_import(
                conn,
                run_id=run_id,
                source_file=source_path,
                file_role='platform_receipts',
                import_status='loaded',
                row_count=len(eligible),
            )
            insert_rows.extend(eligible)

        if insert_rows:
            conn.executemany(
                """
                INSERT INTO fact_platform_receipts (
                    source_file,
                    source_row_hash,
                    period_month,
                    receipt_date,
                    receipt_reference,
                    settlement_id,
                    platform_code,
                    store_code,
                    currency,
                    receipt_amount,
                    receipt_type,
                    memo,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                insert_rows,
            )

        conn.commit()
        note = f'Loaded {len(insert_rows)} receipt rows for {month_text}; files={loaded_files}'
        finish_etl_run(conn, run_id, 'success', note)
        conn.commit()
        print_banner(note)
        return 0
    except Exception as exc:
        conn.rollback()
        finish_etl_run(conn, run_id, 'failed', str(exc))
        conn.commit()
        raise
    finally:
        conn.close()


if __name__ == '__main__':
    raise SystemExit(main())
