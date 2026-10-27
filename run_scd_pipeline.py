# -*- coding: utf-8 -*-
"""
=============================================================================
CHƯƠNG TRÌNH ĐIỀU PHỐI CHÍNH: DATA WAREHOUSE SCD TYPE 1 & TYPE 2 PIPELINE
HỖ TRỢ ĐA DATABASE: SQL SERVER + POSTGRESQL + DUCKDB
=============================================================================
"""

import sys
import io

if sys.stdout.encoding != 'utf-8':
    try:
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    except Exception:
        pass

from db_connect import test_connections
from scd_type2 import run_scd2_full_pipeline
from scd_type2_suppliers import run_scd2_suppliers_pipeline
from scd_type1 import run_scd1_benchmark


def main():
    print("=" * 85)
    print("      HỆ THỐNG THỬ NGHIỆM KHO DỮ LIỆU: SCD TYPE 1 & SCD TYPE 2")
    print("      KẾT NỐI ĐA DATABASE THỰC TẾ: SQL SERVER + POSTGRESQL + DUCKDB")
    print("=" * 85)
    
    # 1. Kiểm tra kết nối trước
    test_connections()
    
    if len(sys.argv) > 1:
        choice = sys.argv[1].strip()
    else:
        print("\nCHỌN KỊCH BẢN THỰC THI:")
        print("  [1] SCD Type 2: Khách hàng bán lẻ từ SQL SERVER [multichannel_sales] (ƯU TIÊN)")
        print("  [2] SCD Type 2: Nhà cung cấp từ POSTGRESQL [logistics_purchasing] (MẬT KHẨU: h$c4122005)")
        print("  [3] SCD Type 1: Ghi đè thông tin khách hàng từ SQL SERVER")
        print("  [4] Chạy TOÀN BỘ tất cả kịch bản trên cả 2 Database")
        print("  [0] Thoát")
        try:
            choice = input("\nNhập lựa chọn của bạn (1/2/3/4/0) [Mặc định: 1]: ").strip()
            if not choice:
                choice = "1"
        except (EOFError, KeyboardInterrupt):
            choice = "1"

    if choice == "1":
        print("\n>>> ĐANG CHẠY KỊCH BẢN SCD TYPE 2 TRÊN SQL SERVER (BẢNG CUSTOMERS)...")
        run_scd2_full_pipeline()
    elif choice == "2":
        print("\n>>> ĐANG CHẠY KỊCH BẢN SCD TYPE 2 TRÊN POSTGRESQL (BẢNG SUPPLIERS)...")
        run_scd2_suppliers_pipeline()
    elif choice == "3":
        print("\n>>> ĐANG CHẠY KỊCH BẢN SCD TYPE 1 TRÊN SQL SERVER...")
        run_scd1_benchmark()
    elif choice == "4":
        print("\n>>> 1/3: CHẠY SCD TYPE 2 TRÊN SQL SERVER [multichannel_sales]...")
        run_scd2_full_pipeline()
        print("\n>>> 2/3: CHẠY SCD TYPE 2 TRÊN POSTGRESQL [logistics_purchasing]...")
        run_scd2_suppliers_pipeline()
        print("\n>>> 3/3: CHẠY SCD TYPE 1 TRÊN SQL SERVER...")
        run_scd1_benchmark()
    else:
        print("Đã thoát chương trình.")


if __name__ == "__main__":
    main()
