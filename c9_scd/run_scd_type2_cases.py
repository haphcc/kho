#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Mô phỏng SCD Type 2 cho ST-0001 (store_id = 1) và customer_id = 101.

Script chỉ ghi vào PostgreSQL Data Warehouse (schema dw); không thay đổi
hai hệ thống nguồn. Các trạng thái sau baseline là snapshot mô phỏng để
kiểm chứng việc đóng/mở phiên bản Dimension.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import sys
import urllib.parse
from datetime import datetime
from typing import Any, Mapping

import sqlalchemy as sa
from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine


def configure_unicode_output() -> None:
    """Giữ log tiếng Việt đọc được trên terminal Windows không dùng UTF-8."""
    for stream_name in ("stdout", "stderr"):
        stream = getattr(sys, stream_name)
        if getattr(stream, "encoding", None) and stream.encoding.lower() != "utf-8":
            setattr(sys, stream_name, io.TextIOWrapper(stream.buffer, encoding="utf-8", errors="replace"))


configure_unicode_output()


STORE_TABLE = "dw.dim_store"
CUSTOMER_TABLE = "dw.dim_customer"
FAR_FUTURE = datetime(2099, 12, 31, 23, 59, 59)

STORE_KEY = "1"  # stores.store_id = 1, tương ứng stores.store_code = ST-0001
CUSTOMER_KEY = "101"

STORE_SCD_COLUMNS = ("store_type", "region_id", "region_name", "status")
CUSTOMER_SCD_COLUMNS = ("member_level", "status")
SCD_METADATA_COLUMNS = {
    "effective_from",
    "effective_to",
    "is_current",
    "version_num",
    "row_hash",
}


def build_engine() -> Engine:
    """Tạo kết nối PostgreSQL đến Data Warehouse từ biến môi trường."""
    host = os.getenv("PG_HOST", "127.0.0.1")
    port = os.getenv("PG_PORT", "5432")
    user = os.getenv("PG_USER", "postgres")
    password = urllib.parse.quote_plus(os.getenv("PG_PASSWORD", "1"))
    database = os.getenv("DW_DATABASE", "wincommerce_dw")
    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
    return sa.create_engine(url, future=True)


def make_hash(record: Mapping[str, Any], columns: tuple[str, ...]) -> str:
    """Tạo MD5 ổn định chỉ từ các thuộc tính SCD Type 2."""
    values = {column: record[column] for column in columns}
    payload = json.dumps(
        values, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str
    )
    return hashlib.md5(payload.encode("utf-8")).hexdigest()


def required_columns(business_key: str, attributes: set[str]) -> set[str]:
    return {business_key, *attributes, *SCD_METADATA_COLUMNS}


def validate_schema(conn: Connection) -> None:
    """Dừng sớm nếu DIM chưa được bổ sung metadata SCD Type 2."""
    specs = (
        (STORE_TABLE, "store_id", {"store_name", "province", "district", *STORE_SCD_COLUMNS}),
        (
            CUSTOMER_TABLE,
            "customer_id",
            {
                "customer_name",
                "gender",
                "date_of_birth",
                "registration_channel",
                "registration_date",
                *CUSTOMER_SCD_COLUMNS,
            },
        ),
    )

    for table, business_key, attributes in specs:
        schema, table_name = table.split(".", maxsplit=1)
        existing = set(
            conn.execute(
                text(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = :schema AND table_name = :table_name
                    """
                ),
                {"schema": schema, "table_name": table_name},
            ).scalars()
        )
        missing = required_columns(business_key, attributes) - existing
        if missing:
            joined = ", ".join(sorted(missing))
            raise RuntimeError(
                f"{table} thiếu cột: {joined}. Hãy chạy ALTER TABLE bổ sung 5 cột "
                "SCD (effective_from, effective_to, is_current, version_num, row_hash) "
                "và bỏ UNIQUE đơn trên business key trước khi chạy script."
            )


def baseline_store() -> dict[str, Any]:
    """Snapshot nguồn của stores.store_id = 1 / stores.store_code = ST-0001."""
    return {
        "store_id": STORE_KEY,
        "store_name": "WinMart+ Hải Dương 1",
        "store_type": "WINMART_PLUS",
        "region_id": "91",
        "region_name": "Khu vực Hải Dương 2",
        "province": "Hải Dương",
        "district": "Thị xã Sơn Tây",
        "status": "ACTIVE",
    }


def baseline_customer() -> dict[str, Any]:
    """Snapshot nguồn của customers.customer_id = 101."""
    return {
        "customer_id": CUSTOMER_KEY,
        "customer_name": "Võ Thanh Anh",
        "gender": "FEMALE",
        "date_of_birth": datetime(2001, 12, 24).date(),
        "registration_channel": "STORE",
        "registration_date": datetime(2023, 5, 27).date(),
        "member_level": "STANDARD",
        "status": "ACTIVE",
    }


def get_current_row(conn: Connection, table: str, business_key: str, key_value: str) -> Mapping[str, Any] | None:
    return conn.execute(
        text(
            f"SELECT * FROM {table} "
            f"WHERE {business_key} = :key_value AND is_current = TRUE FOR UPDATE"
        ),
        {"key_value": key_value},
    ).mappings().first()


def get_row_at_start(
    conn: Connection, table: str, business_key: str, key_value: str, effective_from: datetime
) -> Mapping[str, Any] | None:
    return conn.execute(
        text(
            f"SELECT * FROM {table} "
            f"WHERE {business_key} = :key_value AND effective_from = :effective_from"
        ),
        {"key_value": key_value, "effective_from": effective_from},
    ).mappings().first()


def insert_version(conn: Connection, table: str, record: Mapping[str, Any]) -> None:
    columns = list(record)
    sql_columns = ", ".join(columns)
    sql_values = ", ".join(f":{column}" for column in columns)
    conn.execute(text(f"INSERT INTO {table} ({sql_columns}) VALUES ({sql_values})"), dict(record))


def ensure_baseline(
    conn: Connection,
    table: str,
    business_key: str,
    baseline: Mapping[str, Any],
    scd_columns: tuple[str, ...],
    start_at: datetime,
) -> None:
    """Chèn version 1 nếu business key chưa có lịch sử trong DIM."""
    current = get_current_row(conn, table, business_key, str(baseline[business_key]))
    if current is not None:
        print(f"  - {table}: đã có bản ghi hiện hành, dùng làm baseline.")
        return

    record = dict(baseline)
    record.update(
        {
            "effective_from": start_at,
            "effective_to": FAR_FUTURE,
            "is_current": True,
            "version_num": 1,
            "row_hash": make_hash(record, scd_columns),
        }
    )
    insert_version(conn, table, record)
    print(f"  - {table}: tạo baseline version 1.")


def apply_scd2_change(
    conn: Connection,
    table: str,
    business_key: str,
    key_value: str,
    scd_columns: tuple[str, ...],
    effective_from: datetime,
    changes: Mapping[str, Any],
) -> None:
    """Đóng dòng hiện hành và tạo phiên bản mới khi snapshot SCD2 thay đổi."""
    existing_version = get_row_at_start(conn, table, business_key, key_value, effective_from)

    if existing_version is not None:
        expected = dict(existing_version)
        expected.update(changes)
        expected_hash = make_hash(expected, scd_columns)
        if existing_version["row_hash"] != expected_hash:
            raise RuntimeError(
                f"{table} đã có phiên bản tại {effective_from:%Y-%m-%d} nhưng dữ liệu không khớp kịch bản."
            )
        print(f"  - {table}: phiên bản {effective_from:%Y-%m-%d} đã tồn tại, bỏ qua.")
        return

    current = get_current_row(conn, table, business_key, key_value)
    if current is None:
        raise RuntimeError(f"Không tìm thấy phiên bản hiện hành của {table}, key = {key_value}.")
    if current["effective_from"] >= effective_from:
        raise RuntimeError(
            f"{table} có effective_from = {current['effective_from']}, không thể đóng tại "
            f"{effective_from}. Dùng --reset-demo để tạo lại riêng lịch sử minh họa."
        )

    next_record = dict(current)
    next_record.update(changes)
    next_hash = make_hash(next_record, scd_columns)
    current_hash = current["row_hash"] or make_hash(current, scd_columns)
    if current_hash == next_hash:
        print(f"  - {table}: không có thay đổi SCD2 tại {effective_from:%Y-%m-%d}.")
        return

    conn.execute(
        text(
            f"UPDATE {table} "
            f"SET effective_to = :effective_to, is_current = FALSE "
            f"WHERE {business_key} = :key_value AND is_current = TRUE"
        ),
        {"effective_to": effective_from, "key_value": key_value},
    )

    next_record.pop("store_dim_id", None)
    next_record.pop("customer_dim_id", None)
    next_record.update(
        {
            "effective_from": effective_from,
            "effective_to": FAR_FUTURE,
            "is_current": True,
            "version_num": int(current["version_num"]) + 1,
            "row_hash": next_hash,
        }
    )
    insert_version(conn, table, next_record)
    print(f"  - {table}: tạo version {next_record['version_num']} tại {effective_from:%Y-%m-%d}.")


def reset_demo_history(conn: Connection) -> None:
    """Xóa duy nhất lịch sử hai business key của kịch bản, dùng khi chạy demo lại từ đầu."""
    conn.execute(text(f"DELETE FROM {STORE_TABLE} WHERE store_id = :key"), {"key": STORE_KEY})
    conn.execute(text(f"DELETE FROM {CUSTOMER_TABLE} WHERE customer_id = :key"), {"key": CUSTOMER_KEY})
    print("Đã xóa lịch sử demo cũ của store_id = 1 và customer_id = 101.")


def print_history(conn: Connection) -> None:
    print("\n=== LỊCH SỬ DIM_STORE (ST-0001 / store_id = 1) ===")
    rows = conn.execute(
        text(
            f"SELECT store_id, store_type, region_id, region_name, version_num, "
            f"effective_from, effective_to, is_current FROM {STORE_TABLE} "
            "WHERE store_id = :key ORDER BY version_num"
        ),
        {"key": STORE_KEY},
    ).mappings().all()
    for row in rows:
        print(dict(row))

    print("\n=== LỊCH SỬ DIM_CUSTOMER (customer_id = 101) ===")
    rows = conn.execute(
        text(
            f"SELECT customer_id, customer_name, member_level, version_num, "
            f"effective_from, effective_to, is_current FROM {CUSTOMER_TABLE} "
            "WHERE customer_id = :key ORDER BY version_num"
        ),
        {"key": CUSTOMER_KEY},
    ).mappings().all()
    for row in rows:
        print(dict(row))

    print("\n=== KIỂM TRA TIME-TRAVEL ===")
    checkpoints = (datetime(2026, 2, 15), datetime(2026, 7, 15), datetime(2026, 12, 15))
    for point in checkpoints:
        store = conn.execute(
            text(
                f"SELECT store_type, region_name, version_num FROM {STORE_TABLE} "
                "WHERE store_id = :key AND effective_from <= :point AND :point < effective_to"
            ),
            {"key": STORE_KEY, "point": point},
        ).mappings().first()
        customer = conn.execute(
            text(
                f"SELECT member_level, version_num FROM {CUSTOMER_TABLE} "
                "WHERE customer_id = :key AND effective_from <= :point AND :point < effective_to"
            ),
            {"key": CUSTOMER_KEY, "point": point},
        ).mappings().first()
        print(f"{point:%d/%m/%Y}: store={dict(store) if store else None}; customer={dict(customer) if customer else None}")


def run(reset_demo: bool) -> None:
    baseline_at = datetime(2026, 1, 1)
    first_change_at = datetime(2026, 5, 1)
    second_change_at = datetime(2026, 11, 1)

    engine = build_engine()
    try:
        with engine.begin() as conn:
            validate_schema(conn)
            if reset_demo:
                reset_demo_history(conn)

            print("Khởi tạo baseline:")
            ensure_baseline(conn, STORE_TABLE, "store_id", baseline_store(), STORE_SCD_COLUMNS, baseline_at)
            ensure_baseline(
                conn,
                CUSTOMER_TABLE,
                "customer_id",
                baseline_customer(),
                CUSTOMER_SCD_COLUMNS,
                baseline_at,
            )

            print("\nÁp dụng thay đổi ngày 01/05/2026:")
            apply_scd2_change(
                conn,
                STORE_TABLE,
                "store_id",
                STORE_KEY,
                STORE_SCD_COLUMNS,
                first_change_at,
                {"store_type": "WIN"},
            )
            apply_scd2_change(
                conn,
                CUSTOMER_TABLE,
                "customer_id",
                CUSTOMER_KEY,
                CUSTOMER_SCD_COLUMNS,
                first_change_at,
                {"member_level": "PLATINUM"},
            )

            print("\nÁp dụng thay đổi ngày 01/11/2026:")
            apply_scd2_change(
                conn,
                STORE_TABLE,
                "store_id",
                STORE_KEY,
                STORE_SCD_COLUMNS,
                second_change_at,
                {"region_id": "28", "region_name": "Khu vực Hải Dương"},
            )
            apply_scd2_change(
                conn,
                CUSTOMER_TABLE,
                "customer_id",
                CUSTOMER_KEY,
                CUSTOMER_SCD_COLUMNS,
                second_change_at,
                {"member_level": "GOLD"},
            )

            print_history(conn)
    finally:
        engine.dispose()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Chạy hai tình huống SCD Type 2 cho WinCommerce.")
    parser.add_argument(
        "--reset-demo",
        action="store_true",
        help="Xóa lịch sử hiện có chỉ của store_id=1 và customer_id=101 trước khi tạo lại demo.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    try:
        run(parse_args().reset_demo)
    except Exception as exc:
        print(f"\n[THẤT BẠI] Transaction đã rollback: {exc}", file=sys.stderr)
        sys.exit(1)
