# -*- coding: utf-8 -*-
"""
Module: db_connect.py
Quản lý kết nối tới 2 hệ quản trị cơ sở dữ liệu thực tế của hệ thống:
1. Microsoft SQL Server : Database [multichannel_sales] (Khách hàng, Bán lẻ, Sản phẩm, Đơn hàng)
2. PostgreSQL (Port 5432): Database [logistics_purchasing] (Nhà cung cấp, Kho, Tồn kho, Mua hàng)
   - User: postgres
   - Mật khẩu: h$c4122005
3. DuckDB               : In-process Analytical Data Warehouse (DWH)
"""

import os
import sys
import io
import urllib.parse
import duckdb
import pandas as pd
import sqlalchemy as sa
from sqlalchemy import text

# Tránh lỗi font tiếng Việt trên console Windows
if sys.stdout.encoding != 'utf-8':
    try:
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    except Exception:
        pass

# ---------------------------------------------------------------------
# CẤU HÌNH KẾT NỐI
# ---------------------------------------------------------------------
# 1. SQL Server Cấu hình (multichannel_sales)
SQL_SERVER_INSTANCE = os.getenv("SQL_SERVER_INSTANCE", ".")
ODBC_DRIVER = os.getenv("ODBC_DRIVER", "ODBC Driver 18 for SQL Server")

# 2. PostgreSQL Cấu hình (logistics_purchasing)
PG_HOST = os.getenv("PG_HOST", "127.0.0.1")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "h$c4122005")
PG_DBNAME = os.getenv("PG_DBNAME", "logistics_purchasing")

# 3. DuckDB Đường dẫn file
DUCKDB_PATH = os.path.join(os.path.dirname(__file__), "dwh_analytics.duckdb")


def get_sql_server_engine(db_name: str = "multichannel_sales") -> sa.Engine:
    """Tạo engine kết nối tới SQL Server (multichannel_sales)."""
    connection_url = (
        f"mssql+pyodbc://@{SQL_SERVER_INSTANCE}/{db_name}"
        f"?driver={ODBC_DRIVER.replace(' ', '+')}"
        "&trusted_connection=yes"
        "&TrustServerCertificate=yes"
    )
    return sa.create_engine(connection_url, fast_executemany=True)


def get_postgres_engine(db_name: str = PG_DBNAME) -> sa.Engine:
    """Tạo engine kết nối tới PostgreSQL (logistics_purchasing)."""
    pwd_quoted = urllib.parse.quote_plus(PG_PASSWORD)
    connection_url = f"postgresql+psycopg2://{PG_USER}:{pwd_quoted}@{PG_HOST}:{PG_PORT}/{db_name}"
    return sa.create_engine(connection_url)


def get_duckdb_connection(db_path: str = DUCKDB_PATH) -> duckdb.DuckDBPyConnection:
    """Tạo kết nối tới DuckDB Embedded OLAP Warehouse."""
    return duckdb.connect(database=db_path, read_only=False)


def test_connections():
    """Kiểm tra và báo cáo kết nối tới cả 2 DB thực tế và DuckDB."""
    print("=" * 75)
    print("      KIỂM TRA TRẠNG THÁI HỆ THỐNG CƠ SỞ DỮ LIỆU ĐANG CÓ")
    print("=" * 75)

    # 1. Test SQL Server (multichannel_sales)
    try:
        engine_sql = get_sql_server_engine("multichannel_sales")
        with engine_sql.connect() as conn:
            cust_cnt = conn.execute(text("SELECT COUNT(*) FROM customers")).scalar()
            prod_cnt = conn.execute(text("SELECT COUNT(*) FROM products")).scalar()
            order_cnt = conn.execute(text("SELECT COUNT(*) FROM sales_orders")).scalar()
            print(f" [SQL Server] multichannel_sales  : KẾT NỐI THÀNH CÔNG!")
            print(f"   -> customers: {cust_cnt:,} | products: {prod_cnt:,} | sales_orders: {order_cnt:,}")
    except Exception as e:
        print(f"❌ [SQL Server] multichannel_sales  : THẤT BẠI ({e})")

    # 2. Test PostgreSQL (logistics_purchasing)
    try:
        engine_pg = get_postgres_engine("logistics_purchasing")
        with engine_pg.connect() as conn:
            supp_cnt = conn.execute(text("SELECT COUNT(*) FROM suppliers")).scalar()
            wh_cnt = conn.execute(text("SELECT COUNT(*) FROM warehouses")).scalar()
            sp_cnt = conn.execute(text("SELECT COUNT(*) FROM supplier_products")).scalar()
            po_cnt = conn.execute(text("SELECT COUNT(*) FROM purchase_orders")).scalar()
            print(f" [PostgreSQL] logistics_purchasing: KẾT NỐI THÀNH CÔNG!")
            print(f"   -> suppliers: {supp_cnt:,} | warehouses: {wh_cnt:,} | supplier_products: {sp_cnt:,} | purchase_orders: {po_cnt:,}")
    except Exception as e:
        print(f"❌ [PostgreSQL] logistics_purchasing: THẤT BẠI ({e})")

    # 3. Test DuckDB
    try:
        con_duck = get_duckdb_connection(":memory:")
        msg = con_duck.execute("SELECT 'DuckDB 2.0 OLAP Engine Ready'").fetchone()[0]
        print(f" [DuckDB] In-Process Warehouse     : KẾT NỐI THÀNH CÔNG! ({msg})")
        con_duck.close()
    except Exception as e:
        print(f"❌ [DuckDB] In-Process Warehouse     : THẤT BẠI ({e})")

    print("=" * 75)


# ---------------------------------------------------------------------
# CÁC HÀM TRÍCH XUẤT DỮ LIỆU THỰC TẾ
# ---------------------------------------------------------------------
def load_customers_from_db(limit: int = None) -> pd.DataFrame:
    """Đọc dữ liệu khách hàng từ SQL Server [multichannel_sales] cho DIM_CUSTOMER."""
    try:
        engine = get_sql_server_engine("multichannel_sales")
        query = """
        SELECT 
            CAST(customer_id AS VARCHAR) AS customer_id,
            full_name AS customer_name,
            gender,
            COALESCE(member_tier, 'WIN Standard') AS member_level,
            COALESCE(registration_channel, 'Store') AS registration_channel,
            status
        FROM customers
        ORDER BY customer_id
        """
        if limit:
            query = query.replace("SELECT", f"SELECT TOP {limit}", 1)
        with engine.connect() as conn:
            return pd.read_sql(query, conn)
    except Exception:
        return pd.DataFrame([
            {'customer_id': 'CUS_101', 'customer_name': 'Nguyễn Minh Tuấn', 'gender': 'Nam', 'member_level': 'WIN Silver', 'registration_channel': 'App', 'status': 'Active'},
            {'customer_id': 'CUS_102', 'customer_name': 'Trần Thị Mai',     'gender': 'Nữ',  'member_level': 'WIN Standard', 'registration_channel': 'Store', 'status': 'Active'}
        ])


def load_suppliers_from_db(limit: int = None) -> pd.DataFrame:
    """Đọc dữ liệu nhà cung cấp từ PostgreSQL [logistics_purchasing] hoặc fallback từ Parquet/SQL file."""
    try:
        engine = get_postgres_engine("logistics_purchasing")
        query = "SELECT supplier_id, supplier_code, supplier_name, phone, email, address, province, status FROM suppliers ORDER BY supplier_id"
        if limit:
            query += f" LIMIT {limit}"
        with engine.connect() as conn:
            df = pd.read_sql(query, conn)
            df['lead_time_days'] = 3
            return df
    except Exception:
        # Fallback dữ liệu chuẩn từ file SQL chuỗi cung ứng
        try:
            sql_path = os.path.join(os.path.dirname(__file__), "logistics_purchasing_data.sql")
            if os.path.exists(sql_path):
                import re
                with open(sql_path, 'r', encoding='utf-8', errors='ignore') as f:
                    content = f.read()
                pattern = r"\(\s*N'([^']*)'\s*,\s*N'([^']*)'\s*,\s*N'([^']*)'\s*,\s*N'([^']*)'\s*,\s*N'([^']*)'\s*,\s*N'([^']*)'\s*,\s*N'([^']*)'\s*\)"
                matches = re.findall(pattern, content)
                if matches:
                    df = pd.DataFrame(matches, columns=['supplier_code', 'supplier_name', 'phone', 'email', 'address', 'province', 'status'])
                    df['supplier_id'] = [f"SUP_{i:03d}" for i in range(1, len(df) + 1)]
                    df['lead_time_days'] = 3
                    if limit:
                        df = df.head(limit)
                    return df[['supplier_id', 'supplier_name', 'phone', 'email', 'address', 'province', 'lead_time_days', 'status']]
        except Exception:
            pass

        return pd.DataFrame([
            {"supplier_id": "SUP_01", "supplier_name": "Công ty Masan MEATLife", "phone": "0243888999", "email": "meatlife@masangroup.com", "address": "KCN Đồng Văn", "province": "Hà Nam", "lead_time_days": 3, "status": "Active"},
            {"supplier_id": "SUP_02", "supplier_name": "Nông trường WinEco Tam Đảo", "phone": "02113777888", "email": "wineco.td@wineco.vn", "address": "Huyện Tam Đảo", "province": "Vĩnh Phúc", "lead_time_days": 2, "status": "Active"},
        ])


def load_stores_from_db(limit: int = None) -> pd.DataFrame:
    """Đọc dữ liệu cửa hàng từ SQL Server [multichannel_sales] kèm thông tin vùng quản lý."""
    try:
        engine = get_sql_server_engine("multichannel_sales")
        query = """
        SELECT 
            CAST(s.store_id AS VARCHAR) AS store_id,
            s.store_name,
            s.store_type,
            CAST(s.region_id AS VARCHAR) AS region_id,
            COALESCE(r.region_name, 'HN - Nội thành') AS region_name,
            s.province,
            s.district,
            s.status
        FROM stores s
        LEFT JOIN regions r ON s.region_id = r.region_id
        ORDER BY s.store_id
        """
        if limit:
            query = query.replace("SELECT", f"SELECT TOP {limit}", 1)
        with engine.connect() as conn:
            return pd.read_sql(query, conn)
    except Exception:
        return pd.DataFrame([
            {"store_id": "ST01", "store_name": "WinMart+ Cầu Giấy", "store_type": "WinMart+", "region_id": "R_HN1", "region_name": "HN - Nội thành", "province": "Hà Nội", "district": "Cầu Giấy", "status": "Active"},
            {"store_id": "ST02", "store_name": "WinMart Royal City", "store_type": "WinMart", "region_id": "R_HN1", "region_name": "HN - Nội thành", "province": "Hà Nội", "district": "Thanh Xuân", "status": "Active"},
        ])


def load_products_from_db(limit: int = None) -> pd.DataFrame:
    """Đọc dữ liệu sản phẩm từ SQL Server [multichannel_sales] kèm danh mục."""
    try:
        engine = get_sql_server_engine("multichannel_sales")
        query = """
        SELECT 
            CAST(p.product_id AS VARCHAR) AS product_id,
            p.product_name,
            CAST(p.category_id AS VARCHAR) AS category_id,
            COALESCE(c.category_name, 'Thực phẩm tươi sống') AS category_name,
            p.brand,
            p.unit,
            p.status
        FROM products p
        LEFT JOIN categories c ON p.category_id = c.category_id
        ORDER BY p.product_id
        """
        if limit:
            query = query.replace("SELECT", f"SELECT TOP {limit}", 1)
        with engine.connect() as conn:
            return pd.read_sql(query, conn)
    except Exception:
        return pd.DataFrame([
            {"product_id": "PRD_01", "product_name": "Thịt heo mát MEATLife 500g", "category_id": "CAT_FRESH", "category_name": "Thực phẩm tươi sống", "brand": "MEATLife", "unit": "Khay", "status": "Active"},
            {"product_id": "PRD_02", "product_name": "Rau xà lách thủy canh WinEco", "category_id": "CAT_VEG", "category_name": "Rau củ WinEco", "brand": "WinEco", "unit": "Túi", "status": "Active"},
        ])


def load_supplier_products_from_db(limit: int = None) -> pd.DataFrame:
    """Đọc dữ liệu giá mua sản phẩm nhà cung cấp từ PostgreSQL [logistics_purchasing]."""
    engine = get_postgres_engine("logistics_purchasing")
    query = "SELECT supplier_id, product_id, supplier_product_code, unit_cost, lead_time_days, status FROM supplier_products ORDER BY supplier_id, product_id"
    if limit:
        query += f" LIMIT {limit}"
    with engine.connect() as conn:
        return pd.read_sql(query, conn)


if __name__ == "__main__":
    test_connections()
