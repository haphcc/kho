# -*- coding: utf-8 -*-
"""
=============================================================================
CÂU 9 - KHO DỮ LIỆU WINCOMMERCE: SCD TYPE 2 - KỊCH BẢN 4: DIM_SUPPLIER
ĐIỀU CHỈNH CAM KẾT GIAO HÀNG (LEAD TIME) & ĐÁNH GIÁ NHÀ CUNG CẤP CHUỖI CUNG ỨNG
=============================================================================
Bối cảnh thực tế tại WinCommerce (Phân hệ Chuỗi cung ứng - PostgreSQL / DuckDB):
- Nhà cung cấp ký lại phụ lục hợp đồng thay đổi thời gian cam kết giao hàng 
  (lead_time_days từ 3 ngày rút xuống 1 ngày) hoặc chuyển kho/địa chỉ nhà máy.
- Tại sao bắt buộc dùng SCD Type 2?
  + Phục vụ Câu hỏi 3.2: "Cảnh báo tồn kho dưới Reorder Point kèm gợi ý nhà cung cấp".
  + Phục vụ Câu hỏi 4.11: "Tỷ lệ đơn hàng giao đúng hạn của từng nhà cung cấp (On-time Delivery Rate)".
  + Việc đánh giá nhà cung cấp giao trễ hay đúng hạn ở quý trước phải đối chiếu với
    lead_time_days có hiệu lực tại đúng thời điểm đặt đơn purchase_orders!

Phân loại thuộc tính DIM_SUPPLIER:
  - Natural Key : supplier_id (hoặc supplier_code)
  - Surrogate Key: supplier_dim_id (SUP_SK_001, SUP_SK_002, ...)
  - SCD Type 2  : lead_time_days, status, province, address (Sinh dòng mới)
  - SCD Type 1  : supplier_name, phone, email (Ghi đè in-place)

Chuẩn 5 cột Metadata:
  - effective_from, effective_to, is_current, version_num, row_hash (MD5)
=============================================================================
"""

import os
import sys
import io
import time
import hashlib
import pandas as pd
import duckdb

from db_connect import load_suppliers_from_db, get_duckdb_connection

if sys.stdout.encoding != 'utf-8':
    try:
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    except Exception:
        pass


def make_hash(row, cols):
    """Mã băm MD5 phát hiện thay đổi thuộc tính SCD Type 2."""
    raw = "|".join([str(row[c]) if pd.notna(row[c]) else "" for c in cols])
    return hashlib.md5(raw.encode('utf-8')).hexdigest()


def etl_scd2_suppliers(con: duckdb.DuckDBPyConnection, stg_df: pd.DataFrame, batch_date: str):
    """Hàm xử lý ETL chuyên sâu SCD Type 2 cho DIM_SUPPLIER."""
    table_name = "DIM_SUPPLIER"
    sk_col = "supplier_dim_id"
    sk_prefix = "SUP_SK"
    nk_col = "supplier_id"
    scd2_cols = ["lead_time_days", "status", "province", "address"]
    scd1_cols = ["supplier_name", "phone", "email"]

    batch_ts = pd.to_datetime(batch_date)
    end_of_time = pd.to_datetime('2099-12-31 23:59:59')

    stg_df = stg_df.copy()
    stg_df['row_hash'] = stg_df.apply(lambda r: make_hash(r, scd2_cols), axis=1)
    stg_df[nk_col] = stg_df[nk_col].astype(str)

    dim_df = con.execute(f"SELECT * FROM {table_name}").df()

    if dim_df.empty:
        stg_df[sk_col] = [f"{sk_prefix}_{i:03d}" for i in range(1, len(stg_df) + 1)]
        stg_df['version_num'] = 1
        stg_df['effective_from'] = batch_ts
        stg_df['effective_to'] = end_of_time
        stg_df['is_current'] = True
        init_df = stg_df[list(dim_df.columns)]
        con.execute(f"INSERT INTO {table_name} SELECT * FROM init_df")
        return len(stg_df), 0, 0

    active_dim = dim_df[dim_df['is_current'] == True].copy()
    history_dim = dim_df[dim_df['is_current'] == False].copy()
    active_dim[nk_col] = active_dim[nk_col].astype(str)

    merged = pd.merge(stg_df, active_dim, on=nk_col, how='left', suffixes=('_stg', '_dim'))
    current_max_idx = len(dim_df)
    new_records = []
    scd2_cnt, scd1_cnt, ins_cnt = 0, 0, 0

    for _, row in merged.iterrows():
        if pd.isna(row[sk_col]):
            current_max_idx += 1
            ins_cnt += 1
            rec = {nk_col: row[nk_col], sk_col: f"{sk_prefix}_{current_max_idx:03d}"}
            for c in scd2_cols + scd1_cols:
                rec[c] = row[f"{c}_stg"]
            rec['row_hash'] = row['row_hash_stg']
            rec['version_num'] = 1
            rec['effective_from'] = batch_ts
            rec['effective_to'] = end_of_time
            rec['is_current'] = True
            new_records.append(rec)
        else:
            if row['row_hash_stg'] != row['row_hash_dim']:
                scd2_cnt += 1
                # 1. Đóng dòng cũ
                mask = active_dim[nk_col] == row[nk_col]
                active_dim.loc[mask, 'effective_to'] = batch_ts
                active_dim.loc[mask, 'is_current'] = False

                # 2. Sinh dòng mới
                current_max_idx += 1
                rec = {nk_col: row[nk_col], sk_col: f"{sk_prefix}_{current_max_idx:03d}"}
                for c in scd2_cols + scd1_cols:
                    rec[c] = row[f"{c}_stg"]
                rec['row_hash'] = row['row_hash_stg']
                rec['version_num'] = int(row['version_num']) + 1
                rec['effective_from'] = batch_ts
                rec['effective_to'] = end_of_time
                rec['is_current'] = True
                new_records.append(rec)
            else:
                scd1_changed = any(row[f"{c}_stg"] != row[f"{c}_dim"] for c in scd1_cols if f"{c}_stg" in row and f"{c}_dim" in row)
                if scd1_changed:
                    scd1_cnt += 1
                    mask = active_dim[nk_col] == row[nk_col]
                    for c in scd1_cols:
                        active_dim.loc[mask, c] = row[f"{c}_stg"]

    new_df = pd.DataFrame(new_records)
    dfs = [d for d in [history_dim, active_dim, new_df] if not d.empty]
    final_dim = pd.concat(dfs, ignore_index=True)
    final_dim = final_dim[list(dim_df.columns)].sort_values(by=[nk_col, 'version_num'])

    con.execute(f"DELETE FROM {table_name}")
    con.execute(f"INSERT INTO {table_name} SELECT * FROM final_dim")
    return ins_cnt, scd2_cnt, scd1_cnt


def run_scd2_suppliers_pipeline():
    print("\n" + "=" * 90)
    print("      KHO DỮ LIỆU WINCOMMERCE: SCD TYPE 2 - KỊCH BẢN 4 (DIM_SUPPLIER)")
    print("      ĐIỀU CHỈNH CAM KẾT LEAD TIME & KIỂM TOÁN TỶ LỆ GIAO ĐÚNG HẠN (CÂU 4.11)")
    print("=" * 90)

    con = get_duckdb_connection("dwh_analytics.duckdb")

    # Tạo bảng DIM_SUPPLIER chuẩn 5 cột metadata
    con.execute("""
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

    CREATE OR REPLACE TABLE FACT_PURCHASE_ORDER (
        po_id VARCHAR PRIMARY KEY,
        order_date TIMESTAMP,
        supplier_dim_id VARCHAR,
        supplier_id VARCHAR,
        actual_receipt_date TIMESTAMP,
        actual_lead_time_days INTEGER,
        total_amount DECIMAL(15,2)
    );
    """)

    # -------------------------------------------------------------------------
    # 4 MỐC BIẾN ĐỔI THEO KỊCH BẢN WINCOMMERCE 2026
    # -------------------------------------------------------------------------
    # Đợt 1 (01/01/2026): Khởi tạo hợp đồng
    stg_d1 = pd.DataFrame([
        {'supplier_id': 'SUP_01', 'supplier_name': 'Công ty Masan MEATLife', 'phone': '0243888999', 'email': 'meatlife@masangroup.com', 'address': 'KCN Đồng Văn', 'province': 'Hà Nam', 'lead_time_days': 3, 'status': 'Active'},
        {'supplier_id': 'SUP_02', 'supplier_name': 'Nông trường WinEco Tam Đảo', 'phone': '02113777888', 'email': 'wineco.td@wineco.vn', 'address': 'Huyện Tam Đảo', 'province': 'Vĩnh Phúc', 'lead_time_days': 2, 'status': 'Active'},
        {'supplier_id': 'SUP_03', 'supplier_name': 'Công ty CP Sữa Quốc Tế (LOF)', 'phone': '02838221122', 'email': 'contact@lof.vn', 'address': 'KCN Mỹ Phước', 'province': 'Bình Dương', 'lead_time_days': 4, 'status': 'Active'}
    ])

    # Đợt 2 (01/05/2026): Ký phụ lục rút ngắn lead time giao hàng tươi sống từ 3 ngày xuống 1 ngày
    stg_d2 = pd.DataFrame([
        {'supplier_id': 'SUP_01', 'supplier_name': 'Công ty Masan MEATLife', 'phone': '0243888999', 'email': 'meatlife@masangroup.com', 'address': 'KCN Đồng Văn', 'province': 'Hà Nam', 'lead_time_days': 1, 'status': 'Active'},
        {'supplier_id': 'SUP_02', 'supplier_name': 'Nông trường WinEco Tam Đảo', 'phone': '02113777888', 'email': 'wineco.td@wineco.vn', 'address': 'Huyện Tam Đảo', 'province': 'Vĩnh Phúc', 'lead_time_days': 2, 'status': 'Active'},
        {'supplier_id': 'SUP_03', 'supplier_name': 'Công ty CP Sữa Quốc Tế (LOF)', 'phone': '02838221122', 'email': 'contact@lof.vn', 'address': 'KCN Mỹ Phước', 'province': 'Bình Dương', 'lead_time_days': 4, 'status': 'Active'}
    ])

    # Đợt 3 (01/08/2026): SUP_02 cập nhật thông tin liên hệ mới (SĐT + Email) -> SCD Type 1
    stg_d3 = pd.DataFrame([
        {'supplier_id': 'SUP_01', 'supplier_name': 'Công ty Masan MEATLife', 'phone': '0243888999', 'email': 'meatlife@masangroup.com', 'address': 'KCN Đồng Văn', 'province': 'Hà Nam', 'lead_time_days': 1, 'status': 'Active'},
        {'supplier_id': 'SUP_02', 'supplier_name': 'Nông trường WinEco Tam Đảo', 'phone': '02113999111', 'email': 'contact@tamdao.wineco.vn', 'address': 'Huyện Tam Đảo', 'province': 'Vĩnh Phúc', 'lead_time_days': 2, 'status': 'Active'},
        {'supplier_id': 'SUP_03', 'supplier_name': 'Công ty CP Sữa Quốc Tế (LOF)', 'phone': '02838221122', 'email': 'contact@lof.vn', 'address': 'KCN Mỹ Phước', 'province': 'Bình Dương', 'lead_time_days': 4, 'status': 'Active'}
    ])

    # Đợt 4 (01/11/2026): SUP_01 mở rộng quy mô, chuyển tổng kho trung tâm sang Hưng Yên -> SCD Type 2
    stg_d4 = pd.DataFrame([
        {'supplier_id': 'SUP_01', 'supplier_name': 'Công ty Masan MEATLife', 'phone': '0243888999', 'email': 'meatlife@masangroup.com', 'address': 'KCN Phố Nối A', 'province': 'Hưng Yên', 'lead_time_days': 1, 'status': 'Active'},
        {'supplier_id': 'SUP_02', 'supplier_name': 'Nông trường WinEco Tam Đảo', 'phone': '02113999111', 'email': 'contact@tamdao.wineco.vn', 'address': 'Huyện Tam Đảo', 'province': 'Vĩnh Phúc', 'lead_time_days': 2, 'status': 'Active'},
        {'supplier_id': 'SUP_03', 'supplier_name': 'Công ty CP Sữa Quốc Tế (LOF)', 'phone': '02838221122', 'email': 'contact@lof.vn', 'address': 'KCN Mỹ Phước', 'province': 'Bình Dương', 'lead_time_days': 4, 'status': 'Active'}
    ])

    # Chạy 4 đợt ETL
    print("[1] Đợt 1 (01/01/2026): Khởi tạo ban đầu...")
    try:
        db_supps = load_suppliers_from_db()
        print(f" -> Đã trích xuất {len(db_supps):,} nhà cung cấp từ hệ thống Database.")
        stg_d1_all = pd.concat([stg_d1, db_supps[~db_supps['supplier_id'].isin(['SUP_01', 'SUP_02', 'SUP_03'])]], ignore_index=True)
    except Exception:
        stg_d1_all = stg_d1

    etl_scd2_suppliers(con, stg_d1_all, '2026-01-01 00:00:00')
    print("[2] Đợt 2 (01/05/2026): Ký phụ lục hợp đồng giảm lead time SUP_01 (3 -> 1 ngày) [SCD Type 2]...")
    etl_scd2_suppliers(con, stg_d2, '2026-05-01 00:00:00')
    print("[3] Đợt 3 (01/08/2026): SUP_02 cập nhật SĐT/Email [SCD Type 1 - Ghi đè in-place]...")
    etl_scd2_suppliers(con, stg_d3, '2026-08-01 00:00:00')
    print("[4] Đợt 4 (01/11/2026): SUP_01 di dời kho sang Hưng Yên [SCD Type 2]...")
    etl_scd2_suppliers(con, stg_d4, '2026-11-01 00:00:00')

    print("\n" + "=" * 90)
    print("BẢNG DIM_SUPPLIER SAU 4 ĐỢT ETL:")
    print("=" * 90)
    df_supp = con.execute("""
        SELECT supplier_dim_id, supplier_id, supplier_name, province, lead_time_days, phone,
               version_num, strftime(effective_from, '%Y-%m-%d') AS eff_from, 
               strftime(effective_to, '%Y-%m-%d') AS eff_to, is_current
        FROM DIM_SUPPLIER
        WHERE supplier_id IN ('SUP_01', 'SUP_02', 'SUP_03')
        ORDER BY supplier_id, version_num
    """).df()
    print(df_supp.to_string(index=False))

    # -------------------------------------------------------------------------
    # NẠP DỮ LIỆU ĐƠN MUA HÀNG (FACT_PURCHASE_ORDER) & ĐÁNH GIÁ ON-TIME DELIVERY
    # -------------------------------------------------------------------------
    sup1_sk1 = con.execute("SELECT supplier_dim_id FROM DIM_SUPPLIER WHERE supplier_id = 'SUP_01' AND version_num = 1").fetchone()[0]
    sup1_sk2 = con.execute("SELECT supplier_dim_id FROM DIM_SUPPLIER WHERE supplier_id = 'SUP_01' AND version_num = 2").fetchone()[0]
    sup2_sk1 = con.execute("SELECT supplier_dim_id FROM DIM_SUPPLIER WHERE supplier_id = 'SUP_02' AND version_num = 1").fetchone()[0]

    con.execute("DELETE FROM FACT_PURCHASE_ORDER;")
    con.execute(f"""
    INSERT INTO FACT_PURCHASE_ORDER VALUES
    -- Đơn 1: Đặt ngày 15/02/2026 (Lúc SUP_01 cam kết 3 ngày). Giao sau 2 ngày (17/02) -> ĐÚNG HẠN!
    ('PO_202602_01', TIMESTAMP '2026-02-15 08:00:00', '{sup1_sk1}', 'SUP_01', TIMESTAMP '2026-02-17 14:00:00', 2, 85000000.00),
    
    -- Đơn 2: Đặt ngày 15/06/2026 (Lúc SUP_01 cam kết 1 ngày). Giao sau 2 ngày (17/06) -> TRỄ HẠN!
    ('PO_202606_02', TIMESTAMP '2026-06-15 08:00:00', '{sup1_sk2}', 'SUP_01', TIMESTAMP '2026-06-17 10:00:00', 2, 120000000.00),
    
    -- Đơn 3: Đặt ngày 20/09/2026 cho SUP_02 (Cam kết 2 ngày). Giao sau 2 ngày -> ĐÚNG HẠN!
    ('PO_202609_03', TIMESTAMP '2026-09-20 07:30:00', '{sup2_sk1}', 'SUP_02', TIMESTAMP '2026-09-22 11:00:00', 2, 45000000.00);
    """)

    print("\n" + "=" * 90)
    print("KIỂM TOÁN TỶ LỆ GIAO HÀNG ĐÚNG HẠN (CÂU 4.11 & CÂU 3.2):")
    print("=" * 90)
    df_po_eval = con.execute("""
        SELECT 
            po.po_id AS "Mã PO",
            strftime(po.order_date, '%d/%m/%Y') AS "Ngày đặt PO",
            s.supplier_name AS "Nhà cung cấp",
            s.lead_time_days AS "Cam kết Lead Time (ngày)",
            po.actual_lead_time_days AS "Thực tế giao (ngày)",
            CASE 
                WHEN po.actual_lead_time_days <= s.lead_time_days THEN ' ĐÚNG HẠN (ON-TIME)'
                ELSE '❌ TRỄ HẠN (DELAYED)'
            END AS "Đánh giá KPI giao hàng",
            printf('%,d VNĐ', CAST(po.total_amount AS BIGINT)) AS "Giá trị PO"
        FROM FACT_PURCHASE_ORDER po
        JOIN DIM_SUPPLIER s ON po.supplier_dim_id = s.supplier_dim_id
        ORDER BY po.order_date ASC;
    """).df()
    print(df_po_eval.to_string(index=False))

    print("\n" + "-" * 90)
    print("TẠI SAO BẮT BUỘC PHẢI DÙNG SCD TYPE 2 CHO DIM_SUPPLIER?")
    print("-" * 90)
    print("1. Tại Đơn PO_202602_01 (Tháng 2/2026): Masan MEATLife giao trong 2 ngày.")
    print("   Vì tại thời điểm đó hợp đồng cam kết 3 ngày, nên nhà cung cấp ĐẠT KPI đúng hạn.")
    print("2. NẾU DÙNG SCD TYPE 1 (Ghi đè lead_time_days = 1 ngày cho mọi đơn trong quá khứ):")
    print("   Hệ thống sẽ đối chiếu 2 ngày thực tế với cam kết 1 ngày -> KẾT LUẬN SAI: BỊ PHẠT TRỄ HẠN!")
    print("   Điều này gây tranh chấp hợp đồng và phạt oan đối tác chiến lược của WinCommerce!")
    print("3. SCD Type 2 đảm bảo tính toàn vẹn kiểm toán: Đánh giá theo đúng phụ lục có hiệu lực tại thời điểm phát sinh giao dịch.")

    # Xuất file Parquet
    con.execute("COPY DIM_SUPPLIER TO 'dim_suppliers_scd2.parquet' (FORMAT PARQUET);")
    print(f"\n-> Đã cập nhật file chuẩn: dim_suppliers_scd2.parquet ({os.path.getsize('dim_suppliers_scd2.parquet'):,} bytes)")
    con.close()


if __name__ == "__main__":
    run_scd2_suppliers_pipeline()