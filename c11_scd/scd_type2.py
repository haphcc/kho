# -*- coding: utf-8 -*-
"""
=============================================================================
HỆ THỐNG KHO DỮ LIỆU WINCOMMERCE: SLOWLY CHANGING DIMENSION TYPE 2 (SCD TYPE 2)
HIỆN THỰC HÓA ĐẦY ĐỦ 4 KỊCH BẢN NGHIỆP VỤ THEO BÁO CÁO THIẾT KẾ
=============================================================================
4 Kịch bản nghiệp vụ theo thiết kế:
  1. DIM_STORE   : Chuyển đổi mô hình WinMart+ -> mini-mall WiN & Tái cơ cấu vùng
  2. DIM_CUSTOMER: Chu kỳ Thăng hạng - Hạ hạng Hội viên WIN (Silver -> VIP -> Gold)
  3. DIM_PRODUCT : Tái phân loại Ngành hàng & Nhãn hàng riêng Masan (Thực phẩm tươi sống -> Chilled Food)
  4. DIM_SUPPLIER: Điều chỉnh Cam kết Giao hàng (Lead Time: 3 ngày -> 1 ngày) & Đổi kho/tỉnh thành

Cấu trúc chuẩn 5 cột Metadata SCD Type 2:
  - effective_from : TIMESTAMP (Thời điểm phiên bản bắt đầu có hiệu lực)
  - effective_to   : TIMESTAMP (Thời điểm hết hiệu lực; mặc định 2099-12-31 23:59:59 cho dòng hiện hành)
  - is_current     : BOOLEAN   (TRUE = Đang hiệu lực, FALSE = Lịch sử cũ)
  - version_num    : INTEGER   (Số thứ tự phiên bản: 1, 2, 3...)
  - row_hash       : VARCHAR   (Mã băm MD5 tổ hợp các cột SCD Type 2 giúp phát hiện thay đổi tức thì)

Tích hợp bảng Sự kiện (Fact) & Báo cáo Đa chiều:
  - FACT_SALES_TRANSACTION: Phép toán Roll-up (Câu 4.2) & Drill-down theo thời gian
  - Time-Travel Query (Point-in-time)
  - So sánh đối chứng: Tác hại sai lệch số liệu doanh thu nếu dùng sai SCD Type 1
=============================================================================
"""

import os
import sys
import io
import time
import hashlib
from datetime import datetime
import pandas as pd
import duckdb
import sqlalchemy as sa
from sqlalchemy import text

from db_connect import (
    load_customers_from_db,
    load_stores_from_db,
    load_products_from_db,
    load_suppliers_from_db,
    get_sql_server_engine,
    get_duckdb_connection
)

# Thiết lập UTF-8 tránh lỗi font chữ tiếng Việt trên Windows Console
if sys.stdout.encoding != 'utf-8':
    try:
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    except Exception:
        pass


# =============================================================================
# HÀM BĂM VÀ THUẬT TOÁN ETL SCD TYPE 2 TỔNG QUÁT (GENERIC SCD2 ETL ENGINE)
# =============================================================================
def make_hash(row, cols):
    """Tạo mã băm MD5 từ các thuộc tính SCD Type 2 để phát hiện biến động dữ liệu."""
    raw = "|".join([str(row[c]) if pd.notna(row[c]) else "" for c in cols])
    return hashlib.md5(raw.encode('utf-8')).hexdigest()


def etl_scd2_generic(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    sk_col: str,
    sk_prefix: str,
    nk_col: str,
    scd2_cols: list,
    scd1_cols: list,
    stg_df: pd.DataFrame,
    batch_date: str
):
    """
    Hàm ETL tổng quát xử lý đồng thời Insert (Mới) + SCD Type 2 (Lịch sử) + SCD Type 1 (Ghi đè)
    cho bất kỳ bảng Dimension nào trong hệ thống Kho dữ liệu WinCommerce.
    """
    batch_ts = pd.to_datetime(batch_date)
    end_of_time = pd.to_datetime('2099-12-31 23:59:59')
    
    # 1. Tính hash cho các cột SCD Type 2 trên tập dữ liệu Staging
    stg_df = stg_df.copy()
    stg_df['row_hash'] = stg_df.apply(lambda r: make_hash(r, scd2_cols), axis=1)
    
    dim_df = con.execute(f"SELECT * FROM {table_name}").df()
    
    # Trường hợp bảng Dimension đang rỗng (Lần nạp Baseline đầu tiên)
    if dim_df.empty:
        stg_df[sk_col] = [f"{sk_prefix}_{i:03d}" for i in range(1, len(stg_df) + 1)]
        stg_df['version_num'] = 1
        stg_df['effective_from'] = batch_ts
        stg_df['effective_to'] = end_of_time
        stg_df['is_current'] = True
        
        # Sắp xếp đúng thứ tự cột của bảng đích
        all_cols = [sk_col, nk_col] + [c for c in dim_df.columns if c not in [sk_col, nk_col, 'row_hash', 'version_num', 'effective_from', 'effective_to', 'is_current']] + ['row_hash', 'version_num', 'effective_from', 'effective_to', 'is_current']
        # Đảm bảo đủ các cột
        for col in dim_df.columns:
            if col not in stg_df.columns:
                stg_df[col] = None
        init_df = stg_df[list(dim_df.columns)]
        con.execute(f"INSERT INTO {table_name} SELECT * FROM init_df")
        return len(stg_df), 0, 0
    
    # Tách bản ghi active và history
    active_dim = dim_df[dim_df['is_current'] == True].copy()
    history_dim = dim_df[dim_df['is_current'] == False].copy()
    
    # Ép kiểu natural key thành chuỗi để join an toàn
    stg_df[nk_col] = stg_df[nk_col].astype(str)
    active_dim[nk_col] = active_dim[nk_col].astype(str)
    
    merged = pd.merge(stg_df, active_dim, on=nk_col, how='left', suffixes=('_stg', '_dim'))
    
    # Đếm số surrogate key hiện hữu để tạo key tăng dần
    current_max_idx = len(dim_df)
    new_records = []
    scd2_changed_cnt = 0
    scd1_updated_cnt = 0
    inserted_cnt = 0
    
    for _, row in merged.iterrows():
        # TH1: Bản ghi mới hoàn toàn (Insert mới)
        if pd.isna(row[sk_col]):
            current_max_idx += 1
            inserted_cnt += 1
            rec = {nk_col: row[nk_col], sk_col: f"{sk_prefix}_{current_max_idx:03d}"}
            for c in scd2_cols + scd1_cols:
                rec[c] = row[f"{c}_stg"] if f"{c}_stg" in row else row.get(c, None)
            rec['row_hash'] = row['row_hash_stg']
            rec['version_num'] = 1
            rec['effective_from'] = batch_ts
            rec['effective_to'] = end_of_time
            rec['is_current'] = True
            new_records.append(rec)
            
        else:
            # TH2: Thay đổi thuộc tính SCD Type 2 (Khác row_hash) -> Đóng dòng cũ, sinh dòng mới
            if row['row_hash_stg'] != row['row_hash_dim']:
                scd2_changed_cnt += 1
                # 1. Đóng dòng cũ
                mask = active_dim[nk_col] == row[nk_col]
                active_dim.loc[mask, 'effective_to'] = batch_ts
                active_dim.loc[mask, 'is_current'] = False
                
                # 2. Mở dòng phiên bản mới
                current_max_idx += 1
                rec = {nk_col: row[nk_col], sk_col: f"{sk_prefix}_{current_max_idx:03d}"}
                for c in scd2_cols + scd1_cols:
                    rec[c] = row[f"{c}_stg"] if f"{c}_stg" in row else row.get(c, None)
                rec['row_hash'] = row['row_hash_stg']
                rec['version_num'] = int(row['version_num']) + 1
                rec['effective_from'] = batch_ts
                rec['effective_to'] = end_of_time
                rec['is_current'] = True
                new_records.append(rec)
                
            # TH3: Không đổi SCD Type 2 nhưng có đổi SCD Type 1 -> Ghi đè in-place (không sinh dòng mới)
            else:
                scd1_changed = any(row[f"{c}_stg"] != row[f"{c}_dim"] for c in scd1_cols if f"{c}_stg" in row and f"{c}_dim" in row)
                if scd1_changed:
                    scd1_updated_cnt += 1
                    mask = active_dim[nk_col] == row[nk_col]
                    for c in scd1_cols:
                        if f"{c}_stg" in row:
                            active_dim.loc[mask, c] = row[f"{c}_stg"]
    
    new_df = pd.DataFrame(new_records)
    dfs_to_concat = [d for d in [history_dim, active_dim, new_df] if not d.empty]
    final_dim = pd.concat(dfs_to_concat, ignore_index=True)
    
    # Đảm bảo đủ các cột như thiết kế ban đầu
    for col in dim_df.columns:
        if col not in final_dim.columns:
            final_dim[col] = None
    final_dim = final_dim[list(dim_df.columns)].sort_values(by=[nk_col, 'version_num'])
    
    con.execute(f"DELETE FROM {table_name}")
    con.execute(f"INSERT INTO {table_name} SELECT * FROM final_dim")
    return inserted_cnt, scd2_changed_cnt, scd1_updated_cnt


# =============================================================================
# KHỞI TẠO CẤU TRÚC BẢNG KHO DỮ LIỆU DWH TRÊN DUCKDB
# =============================================================================
def init_wincommerce_dwh_schema(con: duckdb.DuckDBPyConnection):
    """Khởi tạo cấu trúc các bảng Chiều (Dimensions) và Bảng Sự kiện (Fact) chuẩn Kimball."""
    con.execute("""
    -- 1. BẢNG CHIỀU CỬA HÀNG (DIM_STORE)
    CREATE OR REPLACE TABLE DIM_STORE (
        store_dim_id VARCHAR PRIMARY KEY,
        store_id VARCHAR,
        store_name VARCHAR,
        store_type VARCHAR,
        region_id VARCHAR,
        region_name VARCHAR,
        province VARCHAR,
        district VARCHAR,
        status VARCHAR,
        row_hash VARCHAR,
        version_num INTEGER,
        effective_from TIMESTAMP,
        effective_to TIMESTAMP,
        is_current BOOLEAN
    );

    -- 2. BẢNG CHIỀU KHÁCH HÀNG (DIM_CUSTOMER)
    CREATE OR REPLACE TABLE DIM_CUSTOMER (
        customer_dim_id VARCHAR PRIMARY KEY,
        customer_id VARCHAR,
        customer_name VARCHAR,
        gender VARCHAR,
        member_level VARCHAR,
        registration_channel VARCHAR,
        status VARCHAR,
        row_hash VARCHAR,
        version_num INTEGER,
        effective_from TIMESTAMP,
        effective_to TIMESTAMP,
        is_current BOOLEAN
    );

    -- 3. BẢNG CHIỀU SẢN PHẨM (DIM_PRODUCT)
    CREATE OR REPLACE TABLE DIM_PRODUCT (
        product_dim_id VARCHAR PRIMARY KEY,
        product_id VARCHAR,
        product_name VARCHAR,
        category_id VARCHAR,
        category_name VARCHAR,
        brand VARCHAR,
        unit VARCHAR,
        status VARCHAR,
        row_hash VARCHAR,
        version_num INTEGER,
        effective_from TIMESTAMP,
        effective_to TIMESTAMP,
        is_current BOOLEAN
    );

    -- 4. BẢNG CHIỀU NHÀ CUNG CẤP (DIM_SUPPLIER)
    CREATE OR REPLACE TABLE DIM_SUPPLIER (
        supplier_dim_id VARCHAR PRIMARY KEY,
        supplier_id VARCHAR,
        supplier_name VARCHAR,
        phone VARCHAR,
        email VARCHAR,
        address VARCHAR,
        province VARCHAR,
        lead_time_days INTEGER,
        status VARCHAR,
        row_hash VARCHAR,
        version_num INTEGER,
        effective_from TIMESTAMP,
        effective_to TIMESTAMP,
        is_current BOOLEAN
    );

    -- 5. BẢNG SỰ KIỆN GIAO DỊCH BÁN HÀNG (FACT_SALES_TRANSACTION)
    CREATE OR REPLACE TABLE FACT_SALES_TRANSACTION (
        date_dim_id INTEGER,
        transaction_ts TIMESTAMP,
        store_dim_id VARCHAR,
        customer_dim_id VARCHAR,
        product_dim_id VARCHAR,
        sales_order_id VARCHAR,
        channel VARCHAR,
        line_total_amount DECIMAL(15,2),
        line_gross_profit DECIMAL(15,2),
        is_win_member INTEGER
    );

    -- 6. BẢNG SỰ KIỆN ĐẶT MUA HÀNG VỚI NHÀ CUNG CẤP (FACT_PURCHASE_ORDER)
    CREATE OR REPLACE TABLE FACT_PURCHASE_ORDER (
        po_id VARCHAR,
        po_date TIMESTAMP,
        supplier_dim_id VARCHAR,
        delivery_date TIMESTAMP,
        actual_lead_time_days INTEGER,
        total_po_amount DECIMAL(15,2),
        is_on_time BOOLEAN
    );
    """)


# =============================================================================
# DỮ LIỆU MẪU CỦA 4 KỊCH BẢN QUA 4 MỐC THỜI GIAN (QUÝ 1 -> QUÝ 4 NĂM 2026)
# =============================================================================
def get_scenario_data(use_real_db: bool = True):
    """
    Chuẩn bị dữ liệu cho 4 đợt biến đổi trong năm 2026:
    - Nếu use_real_db=True: Trích xuất trực tiếp toàn bộ 300 cửa hàng, 1.000 khách hàng,
      800 sản phẩm từ SQL Server [multichannel_sales] và 136 nhà cung cấp làm Baseline,
      sau đó tích hợp 4 kịch bản biến động (ST01, CUS_101, PRD_01, SUP_01) theo đúng file Word.
    - Nếu use_real_db=False: Chạy ở chế độ dữ liệu mẫu thu gọn.
    """
    # Các bản ghi trọng tâm của kịch bản WinCommerce
    stg_store_scenario_d1 = pd.DataFrame([
        {'store_id': 'ST01', 'store_name': 'WinMart+ Cầu Giấy', 'store_type': 'WinMart+', 'region_id': 'R_HN1', 'region_name': 'HN - Nội thành', 'province': 'Hà Nội', 'district': 'Cầu Giấy', 'status': 'Active'},
        {'store_id': 'ST02', 'store_name': 'WinMart Royal City', 'store_type': 'WinMart',  'region_id': 'R_HN1', 'region_name': 'HN - Nội thành', 'province': 'Hà Nội', 'district': 'Thanh Xuân', 'status': 'Active'}
    ])
    stg_cust_scenario_d1 = pd.DataFrame([
        {'customer_id': 'CUS_101', 'customer_name': 'Nguyễn Minh Tuấn', 'gender': 'Nam', 'member_level': 'WIN Silver', 'registration_channel': 'App', 'status': 'Active'},
        {'customer_id': 'CUS_102', 'customer_name': 'Trần Thị Mai',     'gender': 'Nữ',  'member_level': 'WIN Standard', 'registration_channel': 'Store', 'status': 'Active'}
    ])
    stg_prod_scenario_d1 = pd.DataFrame([
        {'product_id': 'PRD_01', 'product_name': 'Thịt heo mát MEATLife 500g', 'category_id': 'CAT_FRESH', 'category_name': 'Thực phẩm tươi sống', 'brand': 'MEATLife', 'unit': 'Khay', 'status': 'Active'},
        {'product_id': 'PRD_02', 'product_name': 'Rau xà lách thủy canh WinEco', 'category_id': 'CAT_VEG', 'category_name': 'Rau củ WinEco', 'brand': 'WinEco', 'unit': 'Túi', 'status': 'Active'}
    ])
    stg_supp_scenario_d1 = pd.DataFrame([
        {'supplier_id': 'SUP_01', 'supplier_name': 'Công ty Masan MEATLife', 'phone': '0243888999', 'email': 'meatlife@masangroup.com', 'address': 'KCN Đồng Văn', 'province': 'Hà Nam', 'lead_time_days': 3, 'status': 'Active'},
        {'supplier_id': 'SUP_02', 'supplier_name': 'Nông trường WinEco Tam Đảo', 'phone': '02113777888', 'email': 'wineco.td@wineco.vn', 'address': 'Huyện Tam Đảo', 'province': 'Vĩnh Phúc', 'lead_time_days': 2, 'status': 'Active'}
    ])

    if use_real_db:
        print("[KẾT NỐI DATABASE THỰC TẾ]:")
        try:
            db_stores = load_stores_from_db()
            print(f" -> SQL Server [multichannel_sales]: Đã trích xuất {len(db_stores):,} cửa hàng.")
            # Loại trừ trùng lặp mã nếu có
            stg_store_d1 = pd.concat([stg_store_scenario_d1, db_stores[~db_stores['store_id'].isin(['ST01', 'ST02'])]], ignore_index=True)
        except Exception as e:
            print(f" -> Fallback stores ({e})")
            stg_store_d1 = stg_store_scenario_d1

        try:
            db_cust = load_customers_from_db()
            print(f" -> SQL Server [multichannel_sales]: Đã trích xuất {len(db_cust):,} khách hàng.")
            stg_cust_d1 = pd.concat([stg_cust_scenario_d1, db_cust[~db_cust['customer_id'].isin(['CUS_101', 'CUS_102'])]], ignore_index=True)
        except Exception as e:
            print(f" -> Fallback customers ({e})")
            stg_cust_d1 = stg_cust_scenario_d1

        try:
            db_prod = load_products_from_db()
            print(f" -> SQL Server [multichannel_sales]: Đã trích xuất {len(db_prod):,} sản phẩm.")
            stg_prod_d1 = pd.concat([stg_prod_scenario_d1, db_prod[~db_prod['product_id'].isin(['PRD_01', 'PRD_02'])]], ignore_index=True)
        except Exception as e:
            print(f" -> Fallback products ({e})")
            stg_prod_d1 = stg_prod_scenario_d1

        try:
            db_supp = load_suppliers_from_db()
            print(f" -> Database Chuỗi cung ứng: Đã trích xuất {len(db_supp):,} nhà cung cấp.")
            stg_supp_d1 = pd.concat([stg_supp_scenario_d1, db_supp[~db_supp['supplier_id'].isin(['SUP_01', 'SUP_02'])]], ignore_index=True)
        except Exception as e:
            print(f" -> Fallback suppliers ({e})")
            stg_supp_d1 = stg_supp_scenario_d1
    else:
        stg_store_d1 = stg_store_scenario_d1
        stg_cust_d1 = stg_cust_scenario_d1
        stg_prod_d1 = stg_prod_scenario_d1
        stg_supp_d1 = stg_supp_scenario_d1

    # -------------------------------------------------------------------------
    # ĐỢT 2 (2026-05-01): BIẾN ĐỘNG SCD TYPE 2 ĐỢT 1
    # - ST01: Nâng cấp thành mini-mall 'WiN' (tích hợp Phúc Long & Dr.Win) -> SCD2 (version 2)
    # - CUS_101: Chi tiêu tích cực thăng hạng lên 'WIN VIP' -> SCD2 (version 2)
    # - PRD_01: Tách riêng sang danh mục chiến lược 'Thực phẩm Công nghệ Mát' -> SCD2 (version 2)
    # - SUP_01: Ký phụ lục hợp đồng rút ngắn lead_time từ 3 ngày xuống 1 ngày -> SCD2 (version 2)
    # -------------------------------------------------------------------------
    stg_store_d2 = pd.DataFrame([
        {'store_id': 'ST01', 'store_name': 'WiN Cầu Giấy', 'store_type': 'WiN', 'region_id': 'R_HN1', 'region_name': 'HN - Nội thành', 'province': 'Hà Nội', 'district': 'Cầu Giấy', 'status': 'Active'},
        {'store_id': 'ST02', 'store_name': 'WinMart Royal City', 'store_type': 'WinMart',  'region_id': 'R_HN1', 'region_name': 'HN - Nội thành', 'province': 'Hà Nội', 'district': 'Thanh Xuân', 'status': 'Active'}
    ])
    stg_cust_d2 = pd.DataFrame([
        {'customer_id': 'CUS_101', 'customer_name': 'Nguyễn Minh Tuấn', 'gender': 'Nam', 'member_level': 'WIN VIP', 'registration_channel': 'App', 'status': 'Active'},
        {'customer_id': 'CUS_102', 'customer_name': 'Trần Thị Mai',     'gender': 'Nữ',  'member_level': 'WIN Standard', 'registration_channel': 'Store', 'status': 'Active'}
    ])
    stg_prod_d2 = pd.DataFrame([
        {'product_id': 'PRD_01', 'product_name': 'Thịt heo mát MEATLife 500g', 'category_id': 'CAT_CHILLED', 'category_name': 'Thực phẩm Công nghệ Mát (Chilled Food)', 'brand': 'MEATLife', 'unit': 'Khay', 'status': 'Active'},
        {'product_id': 'PRD_02', 'product_name': 'Rau xà lách thủy canh WinEco', 'category_id': 'CAT_VEG', 'category_name': 'Rau củ WinEco', 'brand': 'WinEco', 'unit': 'Túi', 'status': 'Active'}
    ])
    stg_supp_d2 = pd.DataFrame([
        {'supplier_id': 'SUP_01', 'supplier_name': 'Công ty Masan MEATLife', 'phone': '0243888999', 'email': 'meatlife@masangroup.com', 'address': 'KCN Đồng Văn', 'province': 'Hà Nam', 'lead_time_days': 1, 'status': 'Active'},
        {'supplier_id': 'SUP_02', 'supplier_name': 'Nông trường WinEco Tam Đảo', 'phone': '02113777888', 'email': 'wineco.td@wineco.vn', 'address': 'Huyện Tam Đảo', 'province': 'Vĩnh Phúc', 'lead_time_days': 2, 'status': 'Active'}
    ])

    # -------------------------------------------------------------------------
    # ĐỢT 3 (2026-08-01): CẬP NHẬT SCD TYPE 1 (GHI ĐÈ - KHÔNG SINH DÒNG MỚI)
    # - ST02: Đính chính tên thành "Siêu thị WinMart Royal City" -> SCD1 (Giữ nguyên version 1)
    # - CUS_101: Đính chính tên "Nguyễn Minh Tuấn (VIP App)" -> SCD1 (Giữ nguyên version 2)
    # - PRD_02: Sửa đơn vị quy cách thành "Gói 300g" và tên -> SCD1 (Giữ nguyên version 1)
    # - SUP_02: Cập nhật SĐT người đại diện "02113999111" -> SCD1 (Giữ nguyên version 1)
    # -------------------------------------------------------------------------
    stg_store_d3 = pd.DataFrame([
        {'store_id': 'ST01', 'store_name': 'WiN Cầu Giấy', 'store_type': 'WiN', 'region_id': 'R_HN1', 'region_name': 'HN - Nội thành', 'province': 'Hà Nội', 'district': 'Cầu Giấy', 'status': 'Active'},
        {'store_id': 'ST02', 'store_name': 'Siêu thị WinMart Royal City', 'store_type': 'WinMart',  'region_id': 'R_HN1', 'region_name': 'HN - Nội thành', 'province': 'Hà Nội', 'district': 'Thanh Xuân', 'status': 'Active'}
    ])
    stg_cust_d3 = pd.DataFrame([
        {'customer_id': 'CUS_101', 'customer_name': 'Nguyễn Minh Tuấn (VIP App)', 'gender': 'Nam', 'member_level': 'WIN VIP', 'registration_channel': 'App', 'status': 'Active'},
        {'customer_id': 'CUS_102', 'customer_name': 'Trần Thị Mai',     'gender': 'Nữ',  'member_level': 'WIN Standard', 'registration_channel': 'Store', 'status': 'Active'}
    ])
    stg_prod_d3 = pd.DataFrame([
        {'product_id': 'PRD_01', 'product_name': 'Thịt heo mát MEATLife 500g', 'category_id': 'CAT_CHILLED', 'category_name': 'Thực phẩm Công nghệ Mát (Chilled Food)', 'brand': 'MEATLife', 'unit': 'Khay', 'status': 'Active'},
        {'product_id': 'PRD_02', 'product_name': 'Rau xà lách thủy canh WinEco 300g', 'category_id': 'CAT_VEG', 'category_name': 'Rau củ WinEco', 'brand': 'WinEco', 'unit': 'Gói 300g', 'status': 'Active'}
    ])
    stg_supp_d3 = pd.DataFrame([
        {'supplier_id': 'SUP_01', 'supplier_name': 'Công ty Masan MEATLife', 'phone': '0243888999', 'email': 'meatlife@masangroup.com', 'address': 'KCN Đồng Văn', 'province': 'Hà Nam', 'lead_time_days': 1, 'status': 'Active'},
        {'supplier_id': 'SUP_02', 'supplier_name': 'Nông trường WinEco Tam Đảo', 'phone': '02113999111', 'email': 'wineco.td@wineco.vn', 'address': 'Huyện Tam Đảo', 'province': 'Vĩnh Phúc', 'lead_time_days': 2, 'status': 'Active'}
    ])

    # -------------------------------------------------------------------------
    # ĐỢT 4 (2026-11-01): BIẾN ĐỘNG SCD TYPE 2 ĐỢT 2
    # - ST01: WinCommerce tái cấu trúc vùng quản lý sang 'HN - Khu vực Tây' -> SCD2 (version 3)
    # - CUS_101: 6 tháng ít phát sinh đơn, bị hạ hạng xuống 'WIN Gold' -> SCD2 (version 3, Downgrade state)
    # - PRD_01: Tạm ngưng kinh doanh phiên bản cũ (status = 'Discontinued') -> SCD2 (version 3)
    # - SUP_01: Di dời tổng kho sang Hưng Yên (KCN Phố Nối A) -> SCD2 (version 3)
    # -------------------------------------------------------------------------
    stg_store_d4 = pd.DataFrame([
        {'store_id': 'ST01', 'store_name': 'WiN Cầu Giấy', 'store_type': 'WiN', 'region_id': 'R_HNW', 'region_name': 'HN - Khu vực Tây', 'province': 'Hà Nội', 'district': 'Cầu Giấy', 'status': 'Active'},
        {'store_id': 'ST02', 'store_name': 'Siêu thị WinMart Royal City', 'store_type': 'WinMart',  'region_id': 'R_HN1', 'region_name': 'HN - Nội thành', 'province': 'Hà Nội', 'district': 'Thanh Xuân', 'status': 'Active'}
    ])
    stg_cust_d4 = pd.DataFrame([
        {'customer_id': 'CUS_101', 'customer_name': 'Nguyễn Minh Tuấn (VIP App)', 'gender': 'Nam', 'member_level': 'WIN Gold', 'registration_channel': 'App', 'status': 'Active'},
        {'customer_id': 'CUS_102', 'customer_name': 'Trần Thị Mai',     'gender': 'Nữ',  'member_level': 'WIN Standard', 'registration_channel': 'Store', 'status': 'Active'}
    ])
    stg_prod_d4 = pd.DataFrame([
        {'product_id': 'PRD_01', 'product_name': 'Thịt heo mát MEATLife 500g', 'category_id': 'CAT_CHILLED', 'category_name': 'Thực phẩm Công nghệ Mát (Chilled Food)', 'brand': 'MEATLife', 'unit': 'Khay', 'status': 'Discontinued'},
        {'product_id': 'PRD_02', 'product_name': 'Rau xà lách thủy canh WinEco 300g', 'category_id': 'CAT_VEG', 'category_name': 'Rau củ WinEco', 'brand': 'WinEco', 'unit': 'Gói 300g', 'status': 'Active'}
    ])
    stg_supp_d4 = pd.DataFrame([
        {'supplier_id': 'SUP_01', 'supplier_name': 'Công ty Masan MEATLife', 'phone': '0243888999', 'email': 'meatlife@masangroup.com', 'address': 'KCN Phố Nối A', 'province': 'Hưng Yên', 'lead_time_days': 1, 'status': 'Active'},
        {'supplier_id': 'SUP_02', 'supplier_name': 'Nông trường WinEco Tam Đảo', 'phone': '02113999111', 'email': 'wineco.td@wineco.vn', 'address': 'Huyện Tam Đảo', 'province': 'Vĩnh Phúc', 'lead_time_days': 2, 'status': 'Active'}
    ])

    return {
        'd1': (stg_store_d1, stg_cust_d1, stg_prod_d1, stg_supp_d1),
        'd2': (stg_store_d2, stg_cust_d2, stg_prod_d2, stg_supp_d2),
        'd3': (stg_store_d3, stg_cust_d3, stg_prod_d3, stg_supp_d3),
        'd4': (stg_store_d4, stg_cust_d4, stg_prod_d4, stg_supp_d4),
    }


# =============================================================================
# CHÈN DỮ LIỆU FACT TƯƠNG ỨNG VÀO CÁC MỐC THỜI GIAN
# =============================================================================
def populate_fact_sales(con: duckdb.DuckDBPyConnection):
    """
    Nạp dữ liệu vào FACT_SALES_TRANSACTION qua 3 giai đoạn của năm 2026:
      - Tháng 02/2026: ST01 (WinMart+ @ HN - Nội thành) bán cho CUS_101 (Silver) -> 50.000.000 VNĐ
      - Tháng 07/2026: ST01 (WiN @ HN - Nội thành) bán cho CUS_101 (VIP) -> 120.000.000 VNĐ
      - Tháng 12/2026: ST01 (WiN @ HN - Khu vực Tây) bán cho CUS_101 (Gold) -> 150.000.000 VNĐ
    """
    st1_sk1 = con.execute("SELECT store_dim_id FROM DIM_STORE WHERE store_id = 'ST01' AND version_num = 1").fetchone()[0]
    st1_sk2 = con.execute("SELECT store_dim_id FROM DIM_STORE WHERE store_id = 'ST01' AND version_num = 2").fetchone()[0]
    st1_sk3 = con.execute("SELECT store_dim_id FROM DIM_STORE WHERE store_id = 'ST01' AND version_num = 3").fetchone()[0]

    c1_sk1 = con.execute("SELECT customer_dim_id FROM DIM_CUSTOMER WHERE customer_id = 'CUS_101' AND version_num = 1").fetchone()[0]
    c1_sk2 = con.execute("SELECT customer_dim_id FROM DIM_CUSTOMER WHERE customer_id = 'CUS_101' AND version_num = 2").fetchone()[0]
    c1_sk3 = con.execute("SELECT customer_dim_id FROM DIM_CUSTOMER WHERE customer_id = 'CUS_101' AND version_num = 3").fetchone()[0]

    p1_sk1 = con.execute("SELECT product_dim_id FROM DIM_PRODUCT WHERE product_id = 'PRD_01' AND version_num = 1").fetchone()[0]
    p1_sk2 = con.execute("SELECT product_dim_id FROM DIM_PRODUCT WHERE product_id = 'PRD_01' AND version_num = 2").fetchone()[0]

    con.execute("DELETE FROM FACT_SALES_TRANSACTION;")
    con.execute(f"""
    INSERT INTO FACT_SALES_TRANSACTION VALUES
    -- Giai đoạn 1: Tháng 02/2026 (ST01 là WinMart+, CUS_101 là WIN Silver)
    (20260215, TIMESTAMP '2026-02-15 10:30:00', '{st1_sk1}', '{c1_sk1}', '{p1_sk1}', 'ORD_202602_001', 'Store', 50000000.00, 15000000.00, 1),
    
    -- Giai đoạn 2: Tháng 07/2026 (ST01 đã lên WiN @ HN - Nội thành, CUS_101 là WIN VIP)
    (20260720, TIMESTAMP '2026-07-20 18:45:00', '{st1_sk2}', '{c1_sk2}', '{p1_sk2}', 'ORD_202607_002', 'App', 120000000.00, 36000000.00, 1),
    
    -- Giai đoạn 3: Tháng 12/2026 (ST01 là WiN @ HN - Khu vực Tây, CUS_101 hạ hạng xuống WIN Gold)
    (20261210, TIMESTAMP '2026-12-10 14:15:00', '{st1_sk3}', '{c1_sk3}', '{p1_sk2}', 'ORD_202612_003', 'App', 150000000.00, 45000000.00, 1);
    """)


# =============================================================================
# CHƯƠNG TRÌNH CHÍNH: CHẠY TOÀN DIỆN 4 KỊCH BẢN SCD TYPE 2
# =============================================================================
def run_scd2_full_pipeline():
    print("\n" + "=" * 90)
    print("      HỆ THỐNG KHO DỮ LIỆU WINCOMMERCE: SCD TYPE 2 TOÀN DIỆN 4 KỊCH BẢN NGHIỆP VỤ")
    print("=" * 90)

    # 1. Khởi tạo kho dữ liệu DuckDB
    con = get_duckdb_connection("dwh_analytics.duckdb")
    init_wincommerce_dwh_schema(con)
    data = get_scenario_data()

    # Cấu hình danh sách cột phân loại SCD1 và SCD2 cho 4 bảng Chiều theo tài liệu
    dim_configs = {
        'DIM_STORE': {
            'sk_col': 'store_dim_id', 'sk_prefix': 'ST_SK', 'nk_col': 'store_id',
            'scd2_cols': ['store_type', 'region_id', 'region_name', 'status'],
            'scd1_cols': ['store_name', 'province', 'district'],
        },
        'DIM_CUSTOMER': {
            'sk_col': 'customer_dim_id', 'sk_prefix': 'CUS_SK', 'nk_col': 'customer_id',
            'scd2_cols': ['member_level', 'status'],
            'scd1_cols': ['customer_name', 'gender', 'registration_channel'],
        },
        'DIM_PRODUCT': {
            'sk_col': 'product_dim_id', 'sk_prefix': 'PRD_SK', 'nk_col': 'product_id',
            'scd2_cols': ['category_id', 'category_name', 'brand', 'status'],
            'scd1_cols': ['product_name', 'unit'],
        },
        'DIM_SUPPLIER': {
            'sk_col': 'supplier_dim_id', 'sk_prefix': 'SUP_SK', 'nk_col': 'supplier_id',
            'scd2_cols': ['lead_time_days', 'status', 'province', 'address'],
            'scd1_cols': ['supplier_name', 'phone', 'email'],
        }
    }

    print("\n[BƯỚC 1] ĐỢT 1 (01/01/2026): NẠP DỮ LIỆU KHỞI TẠO (BASELINE V1) CHO 4 BẢNG DIMENSION")
    t0 = time.perf_counter()
    stg_store_d1, stg_cust_d1, stg_prod_d1, stg_supp_d1 = data['d1']
    etl_scd2_generic(con, 'DIM_STORE', **dim_configs['DIM_STORE'], stg_df=stg_store_d1, batch_date='2026-01-01 00:00:00')
    etl_scd2_generic(con, 'DIM_CUSTOMER', **dim_configs['DIM_CUSTOMER'], stg_df=stg_cust_d1, batch_date='2026-01-01 00:00:00')
    etl_scd2_generic(con, 'DIM_PRODUCT', **dim_configs['DIM_PRODUCT'], stg_df=stg_prod_d1, batch_date='2026-01-01 00:00:00')
    etl_scd2_generic(con, 'DIM_SUPPLIER', **dim_configs['DIM_SUPPLIER'], stg_df=stg_supp_d1, batch_date='2026-01-01 00:00:00')
    t_d1 = (time.perf_counter() - t0) * 1000
    print(f"-> Hoàn tất Đợt 1 trong {t_d1:.2f} ms. Toàn bộ bản ghi ban đầu có version_num = 1, is_current = True.")

    print("\n[BƯỚC 2] ĐỢT 2 (01/05/2026): BIẾN ĐỘNG SCD TYPE 2 ĐỢT 1")
    print(" - DIM_STORE   : ST01 nâng cấp thành 'WiN' mini-mall (Phúc Long, Dr.Win) -> Đóng ST_SK_001, sinh version 2")
    print(" - DIM_CUSTOMER: CUS_101 mua sắm chuỗi WiN thăng hạng lên 'WIN VIP' -> Đóng CUS_SK_001, sinh version 2")
    print(" - DIM_PRODUCT : PRD_01 tách sang danh mục chiến lược 'Thực phẩm Công nghệ Mát' -> Đóng version 1, sinh version 2")
    print(" - DIM_SUPPLIER: SUP_01 ký phụ lục rút ngắn lead_time từ 3 ngày -> 1 ngày -> Đóng version 1, sinh version 2")
    t0 = time.perf_counter()
    stg_store_d2, stg_cust_d2, stg_prod_d2, stg_supp_d2 = data['d2']
    etl_scd2_generic(con, 'DIM_STORE', **dim_configs['DIM_STORE'], stg_df=stg_store_d2, batch_date='2026-05-01 00:00:00')
    etl_scd2_generic(con, 'DIM_CUSTOMER', **dim_configs['DIM_CUSTOMER'], stg_df=stg_cust_d2, batch_date='2026-05-01 00:00:00')
    etl_scd2_generic(con, 'DIM_PRODUCT', **dim_configs['DIM_PRODUCT'], stg_df=stg_prod_d2, batch_date='2026-05-01 00:00:00')
    etl_scd2_generic(con, 'DIM_SUPPLIER', **dim_configs['DIM_SUPPLIER'], stg_df=stg_supp_d2, batch_date='2026-05-01 00:00:00')
    t_d2 = (time.perf_counter() - t0) * 1000
    print(f"-> Hoàn tất Đợt 2 trong {t_d2:.2f} ms.")

    print("\n[BƯỚC 3] ĐỢT 3 (01/08/2026): CẬP NHẬT THUỘC TÍNH SCD TYPE 1 (GHI ĐÈ TRỰC TIẾP)")
    print(" - DIM_STORE   : ST02 sửa lỗi tên hiển thị thành 'Siêu thị WinMart Royal City' -> Ghi đè, KHÔNG sinh dòng mới!")
    print(" - DIM_CUSTOMER: CUS_101 đính chính tên -> Ghi đè trực tiếp")
    print(" - DIM_PRODUCT : PRD_02 sửa đơn vị quy cách sang 'Gói 300g' -> Ghi đè trực tiếp")
    print(" - DIM_SUPPLIER: SUP_02 cập nhật số điện thoại liên hệ người đại diện -> Ghi đè trực tiếp")
    t0 = time.perf_counter()
    stg_store_d3, stg_cust_d3, stg_prod_d3, stg_supp_d3 = data['d3']
    etl_scd2_generic(con, 'DIM_STORE', **dim_configs['DIM_STORE'], stg_df=stg_store_d3, batch_date='2026-08-01 00:00:00')
    etl_scd2_generic(con, 'DIM_CUSTOMER', **dim_configs['DIM_CUSTOMER'], stg_df=stg_cust_d3, batch_date='2026-08-01 00:00:00')
    etl_scd2_generic(con, 'DIM_PRODUCT', **dim_configs['DIM_PRODUCT'], stg_df=stg_prod_d3, batch_date='2026-08-01 00:00:00')
    etl_scd2_generic(con, 'DIM_SUPPLIER', **dim_configs['DIM_SUPPLIER'], stg_df=stg_supp_d3, batch_date='2026-08-01 00:00:00')
    t_d3 = (time.perf_counter() - t0) * 1000
    print(f"-> Hoàn tất Đợt 3 trong {t_d3:.2f} ms.")

    print("\n[BƯỚC 4] ĐỢT 4 (01/11/2026): BIẾN ĐỘNG SCD TYPE 2 ĐỢT 2")
    print(" - DIM_STORE   : ST01 tái cơ cấu sang 'HN - Khu vực Tây' -> Đóng version 2, sinh version 3")
    print(" - DIM_CUSTOMER: CUS_101 hạ hạng định kỳ từ VIP xuống 'WIN Gold' (Downgrade state) -> Đóng v2, sinh version 3")
    print(" - DIM_PRODUCT : PRD_01 tạm ngưng phân phối phiên bản cũ (status = Discontinued) -> Đóng v2, sinh version 3")
    print(" - DIM_SUPPLIER: SUP_01 di dời kho sang Hưng Yên -> Đóng v2, sinh version 3")
    t0 = time.perf_counter()
    stg_store_d4, stg_cust_d4, stg_prod_d4, stg_supp_d4 = data['d4']
    etl_scd2_generic(con, 'DIM_STORE', **dim_configs['DIM_STORE'], stg_df=stg_store_d4, batch_date='2026-11-01 00:00:00')
    etl_scd2_generic(con, 'DIM_CUSTOMER', **dim_configs['DIM_CUSTOMER'], stg_df=stg_cust_d4, batch_date='2026-11-01 00:00:00')
    etl_scd2_generic(con, 'DIM_PRODUCT', **dim_configs['DIM_PRODUCT'], stg_df=stg_prod_d4, batch_date='2026-11-01 00:00:00')
    etl_scd2_generic(con, 'DIM_SUPPLIER', **dim_configs['DIM_SUPPLIER'], stg_df=stg_supp_d4, batch_date='2026-11-01 00:00:00')
    t_d4 = (time.perf_counter() - t0) * 1000
    print(f"-> Hoàn tất Đợt 4 trong {t_d4:.2f} ms.")

    # =========================================================================
    # HIỂN THỊ KẾT QUẢ KIỂM CHỨNG TRỰC TIẾP CỦA 4 BẢNG DIMENSION
    # =========================================================================
    print("\n" + "=" * 90)
    print("1. KẾT QUẢ BẢNG CHIỀU CỬA HÀNG (DIM_STORE) SAU 4 ĐỢT ETL:")
    print("=" * 90)
    df_store_res = con.execute("""
        SELECT store_dim_id, store_id, store_name, store_type, region_name, 
               version_num, strftime(effective_from, '%Y-%m-%d') AS eff_from, 
               strftime(effective_to, '%Y-%m-%d') AS eff_to, is_current
        FROM DIM_STORE
        ORDER BY store_id, version_num
    """).df()
    print(df_store_res.to_string(index=False))

    print("\n" + "=" * 90)
    print("2. KẾT QUẢ BẢNG CHIỀU KHÁCH HÀNG (DIM_CUSTOMER) - CHU KỲ THĂNG & HẠ HẠNG:")
    print("=" * 90)
    df_cust_res = con.execute("""
        SELECT customer_dim_id, customer_id, customer_name, member_level, 
               version_num, strftime(effective_from, '%Y-%m-%d') AS eff_from, 
               strftime(effective_to, '%Y-%m-%d') AS eff_to, is_current
        FROM DIM_CUSTOMER
        ORDER BY customer_id, version_num
    """).df()
    print(df_cust_res.to_string(index=False))

    print("\n" + "=" * 90)
    print("3. KẾT QUẢ BẢNG CHIỀU SẢN PHẨM (DIM_PRODUCT) - TÁI PHÂN LOẠI NGÀNH HÀNG:")
    print("=" * 90)
    df_prod_res = con.execute("""
        SELECT product_dim_id, product_id, product_name, category_name, brand, unit,
               version_num, strftime(effective_from, '%Y-%m-%d') AS eff_from, 
               strftime(effective_to, '%Y-%m-%d') AS eff_to, is_current, status
        FROM DIM_PRODUCT
        ORDER BY product_id, version_num
    """).df()
    print(df_prod_res.to_string(index=False))

    print("\n" + "=" * 90)
    print("4. KẾT QUẢ BẢNG CHIỀU NHÀ CUNG CẤP (DIM_SUPPLIER) - ĐIỀU CHỈNH LEAD TIME & KHO:")
    print("=" * 90)
    df_supp_res = con.execute("""
        SELECT supplier_dim_id, supplier_id, supplier_name, province, lead_time_days, 
               phone, version_num, strftime(effective_from, '%Y-%m-%d') AS eff_from, 
               strftime(effective_to, '%Y-%m-%d') AS eff_to, is_current
        FROM DIM_SUPPLIER
        ORDER BY supplier_id, version_num
    """).df()
    print(df_supp_res.to_string(index=False))

    # =========================================================================
    # NẠP FACT VÀ TRÌNH DIỄN CÁC PHÉP TOÁN OLAP (ROLL-UP, DRILL-DOWN)
    # =========================================================================
    populate_fact_sales(con)
    print("\n" + "=" * 90)
    print("PHÉP TOÁN ROLL-UP: TỔNG HỢP DOANH THU THEO MÔ HÌNH VÀ KHU VỰC QUẢN LÝ (CÂU 4.2)")
    print("=" * 90)
    df_rollup = con.execute("""
        SELECT 
            s.store_type AS "Mô hình cửa hàng",
            s.region_name AS "Khu vực quản lý",
            printf('%,d VNĐ', CAST(SUM(f.line_total_amount) AS BIGINT)) AS "Doanh thu"
        FROM FACT_SALES_TRANSACTION f
        JOIN DIM_STORE s ON f.store_dim_id = s.store_dim_id
        GROUP BY s.store_type, s.region_name
        ORDER BY "Doanh thu" ASC;
    """).df()
    print(df_rollup.to_string(index=False))

    print("\n" + "=" * 90)
    print("PHÉP TOÁN DRILL-DOWN: PHÂN RÃ DOANH THU THEO MỐC THỜI GIAN & TỪNG GIAI ĐOẠN")
    print("=" * 90)
    df_drilldown = con.execute("""
        SELECT 
            strftime(f.transaction_ts, 'Tháng %m/%Y') AS "Thời gian",
            s.store_name AS "Tên điểm bán",
            s.store_type AS "Mô hình",
            s.region_name AS "Khu vực quản lý",
            c.member_level AS "Hạng KH lúc mua",
            p.category_name AS "Ngành hàng lúc bán",
            printf('%,d VNĐ', CAST(f.line_total_amount AS BIGINT)) AS "Doanh thu"
        FROM FACT_SALES_TRANSACTION f
        JOIN DIM_STORE s ON f.store_dim_id = s.store_dim_id
        JOIN DIM_CUSTOMER c ON f.customer_dim_id = c.customer_dim_id
        JOIN DIM_PRODUCT p ON f.product_dim_id = p.product_dim_id
        ORDER BY f.transaction_ts ASC;
    """).df()
    print(df_drilldown.to_string(index=False))

    # =========================================================================
    # PHÂN TÍCH POINT-IN-TIME (TIME-TRAVEL) & MINH CHỨNG TÁC HẠI NẾU DÙNG SCD 1
    # =========================================================================
    print("\n" + "=" * 90)
    print("TRUY VẤN POINT-IN-TIME (TIME TRAVEL) - LỊCH SỬ CỬA HÀNG ST01 QUA TỪNG THỜI ĐIỂM")
    print("=" * 90)
    mils = ["2026-03-01", "2026-07-01", "2026-11-15"]
    for m in mils:
        row = con.execute(f"""
            SELECT store_id, store_name, store_type, region_name, version_num
            FROM DIM_STORE
            WHERE store_id = 'ST01'
              AND effective_from <= TIMESTAMP '{m}'
              AND effective_to > TIMESTAMP '{m}'
        """).df().iloc[0]
        print(f"-> Tại ngày {m}: Cửa hàng [{row['store_name']}] mô hình [{row['store_type']}] thuộc khu vực [{row['region_name']}] (Version {row['version_num']})")

    print("\n" + "-" * 90)
    print("ĐỐI CHỨNG: TẠI SAO BẮT BUỘC DÙNG SCD TYPE 2? (NẾU DÙNG SAI SCD TYPE 1 SẼ RA SAO?)")
    print("-" * 90)
    print("Nếu hệ thống dùng SCD Type 1 (Ghi đè mô hình hiện hành là 'WiN' và khu vực 'HN - Khu vực Tây'):")
    print("-> Toàn bộ 50.000.000 VNĐ doanh thu lúc còn là WinMart+ (6 tháng đầu năm) sẽ bị cộng gộp nhầm cho WiN!")
    print("-> Báo cáo Doanh thu sai lệch hoàn toàn: WinMart+ = 0 VNĐ | WiN = 320.000.000 VNĐ!")
    print("-> Khi dùng chuẩn SCD Type 2: Phản ánh trung thực hiệu quả từng mô hình theo đúng thời gian thực tế!")

    # =========================================================================
    # XUẤT CÁC BẢNG DIMENSION RA PARQUET CHO DATA LAKE / BI TOOLS
    # =========================================================================
    for tbl, f_out in [
        ('DIM_STORE', 'dim_stores_scd2.parquet'),
        ('DIM_CUSTOMER', 'dim_customers_scd2.parquet'),
        ('DIM_PRODUCT', 'dim_products_scd2.parquet'),
        ('DIM_SUPPLIER', 'dim_suppliers_scd2.parquet')
    ]:
        con.execute(f"COPY {tbl} TO '{f_out}' (FORMAT PARQUET);")
        sz = os.path.getsize(f_out)
        print(f"-> Đã xuất bảng [{tbl}] sang Parquet: {f_out} ({sz:,} bytes)")

    con.close()
    print("\n" + "=" * 90)
    print("   HOÀN THÀNH TOÀN DIỆN PIPELINE SCD TYPE 2 CHO 4 KỊCH BẢN THEO ĐÚNG THIẾT KẾ!")
    print("=" * 90)


if __name__ == "__main__":
    run_scd2_full_pipeline()