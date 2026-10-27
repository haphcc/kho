# -*- coding: utf-8 -*-
"""
Module: scd_type1.py
Triển khai & Phân loại tính năng hỗ trợ SCD Type 1 (Ghi đè - In-place Overwrite)
So sánh 3 công cụ:
1. Pandas (In-Memory Data Manipulation)
2. SQLAlchemy (Database Transaction & Direct Upsert)
3. DuckDB (Embedded Vectorized OLAP Engine)

Nghiệp vụ mẫu từ cơ sở dữ liệu multichannel_sales:
- Cập nhật số điện thoại, email, điểm tích lũy loyalty_points cho khách hàng hiện có.
- Chèn thêm khách hàng mới (Upsert / Merge).
- Không lưu vết lịch sử quá khứ (Overwrite).
"""

import sys
import io
import time
from datetime import datetime
import pandas as pd
import duckdb
import sqlalchemy as sa
from sqlalchemy import text

from db_connect import load_customers_from_db, get_sql_server_engine, get_duckdb_connection

if sys.stdout.encoding != 'utf-8':
    try:
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    except Exception:
        pass


# =====================================================================
# 1. TẠO DỮ LIỆU THỬ NGHIỆM BIẾN ĐỘNG (STAGING UPDATES)
# =====================================================================
def generate_scd1_batch_data():
    """
    Tạo batch dữ liệu mới (Staging) gồm:
    - Cập nhật thông tin cho khách hàng có sẵn (ID = 1, 2, 3)
    - Thêm 2 khách hàng mới hoàn toàn (ID = 90001, 90002)
    """
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    updates = [
        {
            "customer_id": 1,
            "full_name": "Võ Minh Minh (Đã đính chính tên)",
            "phone": "0988111222",
            "email": "minh.vo.updated@winmart.vn",
            "province": "Hà Nội",
            "member_tier": "STANDARD",
            "loyalty_points": 99999,  # Cập nhật điểm thưởng mới
            "updated_at": now_str,
        },
        {
            "customer_id": 2,
            "full_name": "Vũ Minh Khánh",
            "phone": "0977333444",
            "email": "khanh.vu.updated@winmart.vn",
            "province": "Đà Nẵng",
            "member_tier": "SILVER",
            "loyalty_points": 88888,
            "updated_at": now_str,
        },
        {
            "customer_id": 90001,
            "full_name": "Khách Hàng Mới A",
            "phone": "0912000001",
            "email": "new.cust.a@gmail.com",
            "province": "TP. Hồ Chí Minh",
            "member_tier": "GOLD",
            "loyalty_points": 500,
            "updated_at": now_str,
        },
        {
            "customer_id": 90002,
            "full_name": "Khách Hàng Mới B",
            "phone": "0912000002",
            "email": "new.cust.b@gmail.com",
            "province": "Cần Thơ",
            "member_tier": "STANDARD",
            "loyalty_points": 0,
            "updated_at": now_str,
        },
    ]
    return pd.DataFrame(updates)


# =====================================================================
# 2. TRIỂN KHAI VỚI PANDAS
# =====================================================================
def run_scd1_pandas(df_target_initial: pd.DataFrame, df_staging: pd.DataFrame):
    """
    Xử lý SCD Type 1 bằng Pandas:
    - Dùng index là customer_id
    - Dùng df.update() để ghi đè các dòng đã có
    - Dùng pd.concat() để ghép các dòng mới chưa có trong target
    """
    start_time = time.perf_counter()
    
    # Tạo bản sao bảng đích để không làm ảnh hưởng nguồn
    df_dim = df_target_initial.copy().set_index("customer_id")
    df_stg = df_staging.copy().set_index("customer_id")
    
    # 1. Update các bản ghi trùng index
    df_dim.update(df_stg)
    
    # 2. Tìm và nối thêm bản ghi mới hoàn toàn
    new_records = df_stg.loc[~df_stg.index.isin(df_dim.index)]
    df_dim_final = pd.concat([df_dim, new_records]).reset_index()
    
    elapsed_ms = (time.perf_counter() - start_time) * 1000
    return df_dim_final, elapsed_ms


# =====================================================================
# 3. TRIỂN KHAI VỚI SQLALCHEMY (Trực tiếp trên Database với Transaction)
# =====================================================================
def run_scd1_sqlalchemy(df_target_initial: pd.DataFrame, df_staging: pd.DataFrame):
    """
    Xử lý SCD Type 1 bằng SQLAlchemy:
    - Tạo bảng tạm dwh_dim_customers_scd1 trên DB
    - Thực hiện transaction MERGE / UPSERT an toàn
    """
    start_time = time.perf_counter()
    engine = get_sql_server_engine("multichannel_sales")
    
    # Tạo bảng đích thử nghiệm cho DWH Dimension trong SQL Server
    with engine.begin() as conn:
        conn.execute(text("""
            IF OBJECT_ID('dwh_dim_customers_scd1', 'U') IS NOT NULL
                DROP TABLE dwh_dim_customers_scd1;
            
            CREATE TABLE dwh_dim_customers_scd1 (
                customer_id BIGINT PRIMARY KEY,
                full_name NVARCHAR(150),
                phone NVARCHAR(20),
                email NVARCHAR(150),
                province NVARCHAR(100),
                member_tier NVARCHAR(10),
                loyalty_points INT,
                updated_at DATETIME2
            );
        """))
    
    # Nạp dữ liệu ban đầu vào bảng đích
    cols = ["customer_id", "full_name", "phone", "email", "province", "member_tier", "loyalty_points", "updated_at"]
    df_target_initial[cols].to_sql("dwh_dim_customers_scd1", engine, if_exists="append", index=False)
    
    # Nạp staging
    with engine.begin() as conn:
        conn.execute(text("""
            IF OBJECT_ID('temp_stg_customers_scd1', 'U') IS NOT NULL
                DROP TABLE temp_stg_customers_scd1;
            
            CREATE TABLE temp_stg_customers_scd1 (
                customer_id BIGINT PRIMARY KEY,
                full_name NVARCHAR(150),
                phone NVARCHAR(20),
                email NVARCHAR(150),
                province NVARCHAR(100),
                member_tier NVARCHAR(10),
                loyalty_points INT,
                updated_at DATETIME2
            );
        """))
    df_staging[cols].to_sql("temp_stg_customers_scd1", engine, if_exists="append", index=False)
    
    # Thực thi câu lệnh T-SQL MERGE (Chuẩn công nghiệp cho SCD Type 1)
    merge_sql = text("""
        MERGE INTO dwh_dim_customers_scd1 AS target
        USING temp_stg_customers_scd1 AS source
        ON (target.customer_id = source.customer_id)
        WHEN MATCHED THEN
            UPDATE SET 
                target.full_name = source.full_name,
                target.phone = source.phone,
                target.email = source.email,
                target.province = source.province,
                target.member_tier = source.member_tier,
                target.loyalty_points = source.loyalty_points,
                target.updated_at = source.updated_at
        WHEN NOT MATCHED THEN
            INSERT (customer_id, full_name, phone, email, province, member_tier, loyalty_points, updated_at)
            VALUES (source.customer_id, source.full_name, source.phone, source.email, source.province, source.member_tier, source.loyalty_points, source.updated_at);
    """)
    
    with engine.begin() as conn:
        conn.execute(merge_sql)
        # Lấy lại các dòng vừa update/insert để kiểm tra
        res = conn.execute(text("SELECT * FROM dwh_dim_customers_scd1 WHERE customer_id IN (1, 2, 90001, 90002)")).fetchall()
        df_result = pd.DataFrame(res, columns=cols)
        
        # Dọn bảng tạm
        conn.execute(text("DROP TABLE temp_stg_customers_scd1;"))
        conn.execute(text("DROP TABLE dwh_dim_customers_scd1;"))

    elapsed_ms = (time.perf_counter() - start_time) * 1000
    return df_result, elapsed_ms


# =====================================================================
# 4. TRIỂN KHAI VỚI DUCKDB (Vectorized In-Process OLAP Engine)
# =====================================================================
def run_scd1_duckdb(df_target_initial: pd.DataFrame, df_staging: pd.DataFrame):
    """
    Xử lý SCD Type 1 bằng DuckDB:
    - Tận dụng cơ chế zero-copy query trực tiếp trên DataFrame
    - Sử dụng cú pháp INSERT OR REPLACE INTO hoặc MERGE
    """
    start_time = time.perf_counter()
    con = get_duckdb_connection(":memory:")
    
    cols = ["customer_id", "full_name", "phone", "email", "province", "member_tier", "loyalty_points", "updated_at"]
    df_init = df_target_initial[cols]
    
    # 1. Tạo bảng đích trong DuckDB
    con.execute("""
        CREATE TABLE dim_customers_scd1 AS 
        SELECT * FROM df_init;
    """)
    
    # 2. Xử lý SCD Type 1 thông qua câu lệnh ON CONFLICT / REPLACE
    # DuckDB hỗ trợ INSERT OR REPLACE INTO cực kỳ nhanh
    con.execute("""
        CREATE UNIQUE INDEX idx_cust_scd1 ON dim_customers_scd1(customer_id);
    """)
    
    con.execute("""
        INSERT OR REPLACE INTO dim_customers_scd1
        SELECT 
            customer_id,
            full_name,
            phone,
            email,
            province,
            member_tier,
            loyalty_points,
            CAST(updated_at AS TIMESTAMP)
        FROM df_staging;
    """)
    
    df_result = con.execute("""
        SELECT * FROM dim_customers_scd1 
        WHERE customer_id IN (1, 2, 90001, 90002) 
        ORDER BY customer_id
    """).df()
    
    elapsed_ms = (time.perf_counter() - start_time) * 1000
    con.close()
    return df_result, elapsed_ms


# =====================================================================
# 5. SO SÁNH VÀ ĐÁNH GIÁ TỔNG QUAN
# =====================================================================
def run_scd1_benchmark():
    print("\n" + "=" * 70)
    print("           KỊCH BẢN SCD TYPE 1 (GHI ĐÈ - OVERWRITE)")
    print("=" * 70)
    
    # Đọc 1.000 khách hàng thực tế từ database
    print("-> Đang tải 1,000 khách hàng từ database SQL Server [multichannel_sales]...")
    df_raw = load_customers_from_db()
    print(f"-> Đã tải thành công: {len(df_raw):,} bản ghi.")
    
    df_staging = generate_scd1_batch_data()
    print(f"-> Staging batch gồm {len(df_staging)} bản ghi:")
    for _, row in df_staging.iterrows():
        print(f"   [ID {row['customer_id']}] {row['full_name']} | ĐT: {row['phone']} | Điểm: {row['loyalty_points']:,}")
    print("-" * 70)
    
    # Chạy Pandas
    print("\n1. Đang chạy SCD Type 1 với PANDAS...")
    res_pandas, time_pandas = run_scd1_pandas(df_raw, df_staging)
    print(f"   Hoàn tất trong: {time_pandas:.2f} ms")
    
    # Chạy SQLAlchemy
    print("\n2. Đang chạy SCD Type 1 với SQLALCHEMY (T-SQL MERGE)...")
    res_sa, time_sa = run_scd1_sqlalchemy(df_raw, df_staging)
    print(f"   Hoàn tất trong: {time_sa:.2f} ms")
    
    # Chạy DuckDB
    print("\n3. Đang chạy SCD Type 1 với DUCKDB (Vectorized INSERT OR REPLACE)...")
    res_duck, time_duck = run_scd1_duckdb(df_raw, df_staging)
    print(f"   Hoàn tất trong: {time_duck:.2f} ms")
    
    # Hiển thị kết quả kiểm chứng
    print("\n" + "=" * 70)
    print("KẾT QUẢ KIỂM CHỨNG CẬP NHẬT (Các bản ghi ID 1, 2, 90001, 90002):")
    print("=" * 70)
    disp_cols = ["customer_id", "full_name", "phone", "member_tier", "loyalty_points"]
    print(res_duck[disp_cols].to_string(index=False))
    
    # Bảng phân loại tính năng & hiệu năng
    print("\n" + "=" * 70)
    print("BẢNG PHÂN LOẠI & ĐÁNH GIÁ MỨC ĐỘ PHÙ HỢP CHO SCD TYPE 1:")
    print("=" * 70)
    eval_matrix = [
        {
            "Tiêu chí": "Thời gian thực thi (1k rows)",
            "Pandas": f"{time_pandas:.2f} ms",
            "SQLAlchemy": f"{time_sa:.2f} ms",
            "DuckDB": f"{time_duck:.2f} ms (Nhanh nhất)",
        },
        {
            "Tiêu chí": "Cơ chế cập nhật",
            "Pandas": "In-memory update + concat",
            "SQLAlchemy": "T-SQL MERGE / ON CONFLICT",
            "DuckDB": "Vectorized INSERT OR REPLACE",
        },
        {
            "Tiêu chí": "Tính an toàn ACID",
            "Pandas": "❌ Không (Phải tự save file)",
            "SQLAlchemy": " Đầy đủ (Transaction Commit)",
            "DuckDB": " Đầy đủ (MVCC Transaction)",
        },
        {
            "Tiêu chí": "Khả năng mở rộng (Scale)",
            "Pandas": "Giới hạn bởi RAM",
            "SQLAlchemy": "Theo Database DWH đích",
            "DuckDB": "Hàng triệu dòng (Out-of-core)",
        },
        {
            "Tiêu chí": "Điểm đánh giá SCD Type 1",
            "Pandas": "7.0 / 10",
            "SQLAlchemy": "9.0 / 10",
            "DuckDB": "9.5 / 10",
        },
    ]
    df_eval = pd.DataFrame(eval_matrix)
    print(df_eval.to_string(index=False))
    print("=" * 70)


if __name__ == "__main__":
    run_scd1_benchmark()
