from __future__ import annotations

import argparse
import csv
from pathlib import Path

from common import connect, ensure_parent_dir, get_config, print_banner


def normalize_month(value: str) -> str:
    text = value.strip()
    if len(text) == 7 and '-' in text:
        return text
    if len(text) == 6 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}"
    raise ValueError('target_month must be YYYY-MM or YYYYMM')


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Export month-close worklists.')
    parser.add_argument('target_month', help='YYYY-MM or YYYYMM')
    return parser.parse_args()


def write_csv(path: Path, headers: list[str], rows: list[list[object]]) -> None:
    ensure_parent_dir(path)
    with path.open('w', encoding='utf-8-sig', newline='') as handle:
        writer = csv.writer(handle)
        writer.writerow(headers)
        writer.writerows(rows)


def find_candidate_texts(conn, ambiguous_value: str) -> tuple[str, str]:
    text = ambiguous_value.strip().lower()
    if not text:
        return '', ''
    rows = conn.execute(
        """
        select sku, product_name_cn
        from dim_sku
        where lower(product_name_cn) like ?
           or ? like '%' || lower(product_name_cn) || '%'
        order by sku
        limit 5
        """,
        (f'%{text}%', text),
    ).fetchall()
    skus = '|'.join(row['sku'] for row in rows)
    names = '|'.join(row['product_name_cn'] for row in rows)
    return skus, names


def main() -> int:
    args = parse_args()
    month_text = normalize_month(args.target_month)
    config = get_config()
    manual_dir = config.base_dir / 'manual'
    conn = connect(config.db_path)

    try:
        print_banner(f'Exporting worklists for {month_text}')

        close_rows = conn.execute(
            """
            select severity, issue_code, issue_key, issue_value, metric_value, source_table, source_ref, note
            from monthly_close_issue_detail
            where period_month = ?
            order by severity desc, issue_code, issue_key
            """,
            (month_text,),
        ).fetchall()
        close_export = [
            [
                row['severity'],
                row['issue_code'],
                row['issue_key'],
                row['issue_value'],
                row['metric_value'],
                row['source_table'],
                row['source_ref'],
                row['note'],
            ]
            for row in close_rows
        ]
        close_path = manual_dir / f'worklist_month_close_{month_text}.csv'
        write_csv(
            close_path,
            ['severity', 'issue_code', 'issue_key', 'issue_value', 'metric_value', 'source_table', 'source_ref', 'note'],
            close_export,
        )

        pending_groups = conn.execute(
            """
            select source_table, mapping_type, ambiguous_value, count(*) as pending_count, min(notes) as sample_note
            from pending_mapping_queue
            where status = 'pending'
            group by source_table, mapping_type, ambiguous_value
            order by pending_count desc, source_table, ambiguous_value
            """
        ).fetchall()
        pending_export = []
        for row in pending_groups:
            candidate_skus, candidate_names = find_candidate_texts(conn, row['ambiguous_value'])
            pending_export.append(
                [
                    row['source_table'],
                    row['mapping_type'],
                    row['ambiguous_value'],
                    row['pending_count'],
                    row['sample_note'],
                    candidate_skus,
                    candidate_names,
                    '',
                ]
            )
        pending_path = manual_dir / 'worklist_pending_aliases.csv'
        write_csv(
            pending_path,
            ['source_table', 'mapping_type', 'ambiguous_value', 'pending_count', 'sample_note', 'candidate_skus', 'candidate_product_names', 'approved_sku'],
            pending_export,
        )

        print_banner(f'Exported {len(close_export)} close issues to {close_path.name}')
        print_banner(f'Exported {len(pending_export)} pending alias groups to {pending_path.name}')
        return 0
    finally:
        conn.close()


if __name__ == '__main__':
    raise SystemExit(main())
