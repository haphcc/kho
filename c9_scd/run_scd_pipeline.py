# -*- coding: utf-8 -*-
"""
=============================================================================
CÂU 9: CHƯƠNG TRÌNH ĐIỀU PHỐI CHÍNH: WINCOMMERCE DWH SCD TYPE 1 & TYPE 2 PIPELINE
HIỆN THỰC HÓA ĐẦY ĐỦ 4 KỊCH BẢN THEO TÀI LIỆU THIẾT KẾ KHO DỮ LIỆU
=============================================================================
4 Kịch bản nghiệp vụ:
  1. DIM_STORE   : Chuyển đổi WinMart+ -> mini-mall WiN & Tái cơ cấu vùng quản lý
  2. DIM_CUSTOMER: Chu kỳ Thăng hạng - Hạ hạng Hội viên WIN (Silver -> VIP -> Gold)
  3. DIM_PRODUCT : Tái phân loại Ngành hàng Masan (Thực phẩm tươi sống -> Chilled Food)
  4. DIM_SUPPLIER: Điều chỉnh Cam kết Lead Time (3 ngày -> 1 ngày) & Đánh giá PO
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
    print("=" * 88)
    print("      [CÂU 9] HỆ THỐNG ĐIỀU PHỐI KHO DỮ LIỆU WINCOMMERCE: SCD TYPE 1 & SCD TYPE 2")
    print("      ĐÁP ỨNG ĐẦY ĐỦ 4 KỊCH BẢN NGHIỆP VỤ THEO BÁO CÁO THIẾT KẾ")
    print("=" * 88)
    
    # 1. Kiểm tra trạng thái kết nối các nguồn dữ liệu
    test_connections()
    
    if len(sys.argv) > 1:
        choice = sys.argv[1].strip()
    else:
        print("\nCHỌN KỊCH BẢN THỰC THI:")
        print("  [1] SCD Type 2: Chạy TOÀN BỘ 4 Kịch bản (Store, Customer, Product, Supplier + Fact Roll-up/Drill-down) (ƯU TIÊN)")
        print("  [2] SCD Type 2: Chuyên sâu Kịch bản 4 (DIM_SUPPLIER - Đánh giá Lead Time & Kiểm toán KPI giao hàng PO)")
        print("  [3] SCD Type 1: Ghi đè thuộc tính SCD1 trên 4 thực thể & So sánh 3 Engine (Pandas / SQLAlchemy / DuckDB)")
        print("  [4] Chạy TOÀN BỘ tất cả các pipeline và xuất các file Parquet cho Data Lake / BI")
        print("  [0] Thoát")
        try:
            choice = input("\nNhập lựa chọn của bạn (1/2/3/4/0) [Mặc định: 1]: ").strip()
            if not choice:
                choice = "1"
        except (EOFError, KeyboardInterrupt):
            choice = "1"

    if choice == "1":
        print("\n>>> ĐANG CHẠY KỊCH BẢN SCD TYPE 2 TOÀN DIỆN (4 BẢNG DIMENSION & FACT SALES)...")
        run_scd2_full_pipeline()
    elif choice == "2":
        print("\n>>> ĐANG CHẠY CHUYÊN SÂU KỊCH BẢN 4: DIM_SUPPLIER & FACT PURCHASE ORDER...")
        run_scd2_suppliers_pipeline()
    elif choice == "3":
        print("\n>>> ĐANG CHẠY THỬ NGHIỆM SCD TYPE 1 (IN-PLACE OVERWRITE) & BENCHMARK 3 ENGINE...")
        run_scd1_benchmark()
    elif choice == "4":
        print("\n>>> [1/3] CHẠY SCD TYPE 2 TOÀN DIỆN 4 KỊCH BẢN...")
        run_scd2_full_pipeline()
        print("\n>>> [2/3] CHẠY CHUYÊN SÂU KỊCH BẢN 4 (DIM_SUPPLIER & FACT_PURCHASE_ORDER)...")
        run_scd2_suppliers_pipeline()
        print("\n>>> [3/3] CHẠY THỬ NGHIỆM & BENCHMARK SCD TYPE 1...")
        run_scd1_benchmark()
    else:
        print("Đã thoát chương trình.")


if __name__ == "__main__":
    main()