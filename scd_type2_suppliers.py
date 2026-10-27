# -*- coding: utf-8 -*-
"""
=============================================================================
KHO DỮ LIỆU: SCD TYPE 2 CHO PHÂN HỆ LOGISTICS & CHUỖI CUNG ỨNG
KẾT NỐI TRỰC TIẾP TỪ POSTGRESQL [logistics_purchasing] (bảng suppliers)
=============================================================================
Mục đích:
- Theo dõi lịch sử biến động nhà cung cấp (Địa chỉ, tỉnh thành, trạng thái hợp tác ACTIVE/INACTIVE).
- Đảm bảo khi kiểm toán chi phí mua hàng và nhập kho quá khứ (Purchase Orders),
  thông tin nhà cung cấp tại thời điểm ký PO luôn chính xác.
"""

import sys
import io
import time
from datetime import datetime
import pandas as pd
import duckdb
import sqlalchemy as sa
from sqlalchemy import text

from db_connect import load_suppliers_from_db, get_postgres_engine, get_duckdb_connection

if sys.stdout.encoding != 'utf-8':
    try:
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    except Exception:
        pass


def run_scd2_suppliers_pipeline():
    print("\n" + "=" * 80)
    print("      SCD TYPE 2: THEO DÕI LỊCH SỬ NHÀ CUNG CẤP TỪ POSTGRESQL [logistics_purchasing]")
    print("=" * 80)

    # 1. Nạp 150 nhà cung cấp thực tế từ PostgreSQL
    print("[BƯỚC 1] Đọc dữ liệu từ PostgreSQL database [logistics_purchasing]...")
    df_suppliers_base = load_suppliers_from_db()
    print(f"-> Đã tải thành công: {len(df_suppliers_base)} nhà cung cấp từ PostgreSQL.")
    supp1_init = df_suppliers_base.iloc[0]
    print(f"-> Nhà cung cấp ID=1 ban đầu: [{supp1_init['supplier_code']}] {supp1_init['supplier_name']} | Tỉnh: {supp1_init['province']} | Status: {supp1_init['status']}")

    # 2. Tạo kịch bản biến động thực tế trong chuỗi cung ứng
    # - NCC 1: Chuyển trụ sở/kho từ Quảng Trị về TP. Hồ Chí Minh, chuyển status ACTIVE -> INACTIVE (đang đánh giá lại)
    # - NCC 2: Giữ nguyên (Kiểm tra tránh sinh phiên bản rác)
    # - NCC 9999: Nhà cung cấp mới tại Lâm Đồng
    df_t1 = pd.DataFrame([
        {
            "supplier_id": 1,
            "supplier_code": "SUP-0001",
            "supplier_name": supp1_init["supplier_name"],
            "phone": "02839998888",
            "email": "contact@supplier1.vn",
            "address": "Khu Công Nghiệp Tân Bình",
            "province": "TP. Hồ Chí Minh",  # Đổi tỉnh thành
            "status": "INACTIVE",           # Tạm ngưng
        },
        {
            "supplier_id": 2,
            "supplier_code": df_suppliers_base.iloc[1]["supplier_code"],
            "supplier_name": df_suppliers_base.iloc[1]["supplier_name"],
            "phone": df_suppliers_base.iloc[1]["phone"],
            "email": df_suppliers_base.iloc[1]["email"],
            "address": df_suppliers_base.iloc[1]["address"],
            "province": df_suppliers_base.iloc[1]["province"],  # Giữ nguyên
            "status": df_suppliers_base.iloc[1]["status"],      # Giữ nguyên
        },
        {
            "supplier_id": 9999,
            "supplier_code": "SUP-9999",
            "supplier_name": "Công ty TNHH Rau Sạch Đà Lạt",
            "phone": "02633888999",
            "email": "dalat.farm@gmail.com",
            "address": "Phường 7, TP. Đà Lạt",
            "province": "Lâm Đồng",
            "status": "ACTIVE",
        }
    ])

    print("\n" + "-" * 80)
    print(" [KỊCH BẢN BIẾN ĐỘNG CHUỖI CUNG ỨNG - Ngày 2026-06-01]")
    print(f" - NCC 1: Chuyển địa chỉ sang TP. Hồ Chí Minh, chuyển trạng thái INACTIVE")
    print(f" - NCC 2: Không thay đổi thuộc tính (Đảm bảo không sinh dòng rác)")
    print(f" - NCC 9999: Ký hợp đồng nhà cung cấp rau sạch mới tại Lâm Đồng")
    print("-" * 80)

    # 3. Triển khai bằng DUCKDB (Tập trung dữ liệu từ PostgreSQL vào DWH)
    print("\n[BƯỚC 2] Xử lý SCD Type 2 bằng DUCKDB OLAP Engine...")
    t0_duck = time.perf_counter()
    con = get_duckdb_connection("dwh_analytics.duckdb")

    # Tạo sequence và bảng dimension trong DuckDB
    con.execute("CREATE SEQUENCE IF NOT EXISTS seq_scd2_supp START 1;")
    con.execute("""
        CREATE OR REPLACE TABLE dim_suppliers_scd2 (
            supplier_sk BIGINT PRIMARY KEY DEFAULT nextval('seq_scd2_supp'),
            supplier_id BIGINT,
            supplier_code VARCHAR,
            supplier_name VARCHAR,
            phone VARCHAR,
            email VARCHAR,
            address VARCHAR,
            province VARCHAR,
            status VARCHAR,
            start_date TIMESTAMP,
            end_date TIMESTAMP,
            is_current BOOLEAN,
            version INT
        );
    """)

    # Nạp dữ liệu ban đầu T0 (2026-01-01) từ PostgreSQL DataFrame
    con.execute("""
        INSERT INTO dim_suppliers_scd2 (
            supplier_id, supplier_code, supplier_name, phone, email, address, province, status,
            start_date, end_date, is_current, version
        )
        SELECT 
            supplier_id, supplier_code, supplier_name, phone, email, address, province, status,
            TIMESTAMP '2026-01-01 00:00:00' AS start_date,
            NULL AS end_date,
            TRUE AS is_current,
            1 AS version
        FROM df_suppliers_base;
    """)

    # Xử lý Batch T1 (2026-06-01)
    con.register("stg_supp", df_t1)

    # Đóng bản ghi cũ nếu province hoặc status thay đổi
    con.execute("""
        UPDATE dim_suppliers_scd2
        SET end_date = TIMESTAMP '2026-06-01 00:00:00',
            is_current = FALSE
        FROM stg_supp s
        WHERE dim_suppliers_scd2.supplier_id = s.supplier_id
          AND dim_suppliers_scd2.is_current = TRUE
          AND (dim_suppliers_scd2.province <> s.province OR dim_suppliers_scd2.status <> s.status);
    """)

    # Thêm bản ghi mới
    con.execute("""
        INSERT INTO dim_suppliers_scd2 (
            supplier_id, supplier_code, supplier_name, phone, email, address, province, status,
            start_date, end_date, is_current, version
        )
        SELECT 
            s.supplier_id, s.supplier_code, s.supplier_name, s.phone, s.email, s.address, s.province, s.status,
            TIMESTAMP '2026-06-01 00:00:00' AS start_date,
            NULL AS end_date,
            TRUE AS is_current,
            COALESCE((SELECT MAX(version) FROM dim_suppliers_scd2 v WHERE v.supplier_id = s.supplier_id), 0) + 1 AS version
        FROM stg_supp s
        LEFT JOIN dim_suppliers_scd2 curr 
          ON s.supplier_id = curr.supplier_id AND curr.is_current = TRUE
        WHERE curr.supplier_id IS NULL;
    """)
    con.unregister("stg_supp")
    time_duck = (time.perf_counter() - t0_duck) * 1000
    print(f"-> Xử lý SCD Type 2 trên DuckDB hoàn tất trong: {time_duck:.2f} ms")

    # 4. Triển khai bằng SQLALCHEMY trên POSTGRESQL
    print("\n[BƯỚC 3] Xử lý SCD Type 2 trực tiếp trên POSTGRESQL qua SQLAlchemy Transaction...")
    t0_pg = time.perf_counter()
    engine_pg = get_postgres_engine("logistics_purchasing")
    
    with engine_pg.begin() as conn:
        conn.execute(text("""
            DROP TABLE IF EXISTS dwh_dim_suppliers_scd2 CASCADE;
            CREATE TABLE dwh_dim_suppliers_scd2 (
                supplier_sk BIGSERIAL PRIMARY KEY,
                supplier_id BIGINT NOT NULL,
                supplier_code VARCHAR(50),
                supplier_name VARCHAR(200),
                phone VARCHAR(20),
                email VARCHAR(150),
                address VARCHAR(255),
                province VARCHAR(100),
                status VARCHAR(10),
                start_date TIMESTAMP NOT NULL,
                end_date TIMESTAMP,
                is_current BOOLEAN NOT NULL,
                version INT NOT NULL
            );
        """))
    
    # Nạp T0
    init_pg = df_suppliers_base.copy()
    init_pg["start_date"] = pd.to_datetime("2026-01-01 00:00:00")
    init_pg["end_date"] = None
    init_pg["is_current"] = True
    init_pg["version"] = 1
    init_pg.to_sql("dwh_dim_suppliers_scd2", engine_pg, if_exists="append", index=False)

    # Đóng phiên bản cũ & thêm phiên bản mới trong Transaction PostgreSQL
    with engine_pg.begin() as conn:
        # Cập nhật đóng bản ghi cũ
        conn.execute(text("""
            UPDATE dwh_dim_suppliers_scd2
            SET end_date = '2026-06-01 00:00:00',
                is_current = FALSE
            WHERE supplier_id = 1 AND is_current = TRUE;
        """))
        # Thêm bản ghi mới
        conn.execute(text(f"""
            INSERT INTO dwh_dim_suppliers_scd2 (
                supplier_id, supplier_code, supplier_name, phone, email, address, province, status,
                start_date, end_date, is_current, version
            ) VALUES 
            (1, 'SUP-0001', '{supp1_init["supplier_name"]}', '02839998888', 'contact@supplier1.vn', 
             'Khu Công Nghiệp Tân Bình', 'TP. Hồ Chí Minh', 'INACTIVE', '2026-06-01 00:00:00', NULL, TRUE, 2),
            (9999, 'SUP-9999', 'Công ty TNHH Rau Sạch Đà Lạt', '02633888999', 'dalat.farm@gmail.com', 
             'Phường 7, TP. Đà Lạt', 'Lâm Đồng', 'ACTIVE', '2026-06-01 00:00:00', NULL, TRUE, 1);
        """))

    time_pg = (time.perf_counter() - t0_pg) * 1000
    print(f"-> Xử lý SCD Type 2 trực tiếp trên PostgreSQL hoàn tất trong: {time_pg:.2f} ms")

    # 5. Kiểm tra kết quả lịch sử
    print("\n" + "=" * 80)
    print("KẾT QUẢ KIỂM CHỨNG LỊCH SỬ NHÀ CUNG CẤP ID=1 (Từ DuckDB DWH):")
    print("=" * 80)
    res_hist = con.execute("""
        SELECT supplier_sk, supplier_id, supplier_code, province, status, start_date, end_date, is_current, version
        FROM dim_suppliers_scd2
        WHERE supplier_id IN (1, 2, 9999)
        ORDER BY supplier_id, version;
    """).df()
    print(res_hist.to_string(index=False))

    # Xuất ra Parquet
    con.execute("COPY dim_suppliers_scd2 TO 'dim_suppliers_scd2.parquet' (FORMAT PARQUET);")
    print(f"\n-> Đã xuất bảng Dimension Nhà cung cấp SCD2 ra: [dim_suppliers_scd2.parquet]")
    print("=" * 80)


if __name__ == "__main__":
    run_scd2_suppliers_pipeline()
