# Hướng Dẫn & Báo Cáo Triển Khai SCD Type 2 & Type 1 Cho WinCommerce
## Dựa trên Báo cáo Thiết kế Kho Dữ Liệu WinCommerce

Hệ thống kho dữ liệu triển khai đầy đủ **4 Kịch bản nghiệp vụ SCD Type 2** theo đúng tài liệu thiết kế của WinCommerce:
1. **DIM_STORE**: Chuyển đổi mô hình điểm bán từ WinMart+ sang "Mini-mall" WiN & Tái cơ cấu vùng quản lý (Hà Nội - Nội thành sang Hà Nội - Khu vực Tây).
2. **DIM_CUSTOMER**: Chu kỳ Thăng hạng – Hạ hạng Hội viên WIN (`WIN Silver` -> `WIN VIP` -> `WIN Gold`).
3. **DIM_PRODUCT**: Tái phân loại Ngành hàng & Nhãn hàng riêng Masan (Thịt heo mát MEATLife từ `Thực phẩm tươi sống` sang `Thực phẩm Công nghệ Mát (Chilled Food)`).
4. **DIM_SUPPLIER**: Điều chỉnh Cam kết Giao hàng (`lead_time_days` từ 3 ngày rút xuống 1 ngày) & Di dời kho trung tâm.

---

## 1. Thiết Kế Chuẩn 5 Cột Metadata Quản Lý Lịch Sử

Theo đúng chuẩn kiến trúc Kimball cho Google BigQuery / DuckDB:

| Tên Cột | Kiểu Dữ Liệu | Ý Nghĩa Trong Kho Dữ Liệu WinCommerce |
| :--- | :--- | :--- |
| **`effective_from`** | TIMESTAMP | Thời điểm phiên bản bắt đầu có hiệu lực. |
| **`effective_to`** | TIMESTAMP | Thời điểm phiên bản hết hiệu lực (`2099-12-31 23:59:59` cho dòng active). |
| **`is_current`** | BOOLEAN | Cờ trạng thái hiện hành (`TRUE` = Đang hiệu lực, `FALSE` = Lịch sử cũ). |
| **`version_num`** | INTEGER | Số thứ tự phiên bản biến đổi (`1, 2, 3...`). |
| **`row_hash`** | VARCHAR | Mã băm MD5 tổ hợp các cột SCD Type 2 giúp phát hiện biến động tức thì. |

---

## 2. Cấu Trúc Mã Nguồn

| Tập tin | Vai trò & Kịch bản thực thi |
| :--- | :--- |
| **`scd_type2.py`** | **[CHÍNH]** Triển khai toàn diện **4 Kịch bản SCD Type 2** qua 4 mốc thời gian trong năm 2026, hàm tổng quát `etl_scd2_generic()`, tích hợp `FACT_SALES_TRANSACTION`, phép toán Roll-up (Câu 4.2), Drill-down theo thời gian, Time-travel Query và xuất Parquet. |
| **`scd_type2_suppliers.py`** | **[CHUYÊN SÂU]** Kịch bản 4: `DIM_SUPPLIER` & `FACT_PURCHASE_ORDER`. Kiểm toán tỷ lệ giao hàng đúng hạn (Câu 4.11 & Câu 3.2), chứng minh tại sao SCD 1 làm sai lệch KPI hợp đồng. |
| **`scd_type1.py`** | Triển khai **SCD Type 1 (Ghi đè in-place)** cho các thuộc tính không lưu lịch sử (đính chính tên cửa hàng, quy cách bao bì `unit`, SĐT/email nhà cung cấp) và Benchmark 3 engine (Pandas vs SQLAlchemy vs DuckDB). |
| **`run_scd_pipeline.py`** | Menu dòng lệnh điều phối và chạy thử nghiệm tương tác toàn bộ hệ thống. |
| **`db_connect.py`** | Kết nối đa CSDL: SQL Server (`multichannel_sales`), DuckDB (`dwh_analytics.duckdb`) và cơ chế fallback an toàn. |

---

## 3. Cách Chạy Thử Nghiệm

Mở Terminal / PowerShell tại thư mục dự án và chạy:

### Chạy trực tiếp SCD Type 2 (4 kịch bản + Roll-up/Drill-down):
```bash
python scd_type2.py
```

### Chạy kiểm toán Nhà cung cấp & Chuỗi cung ứng (Kịch bản 4):
```bash
python scd_type2_suppliers.py
```

### Chạy thử nghiệm & Benchmark SCD Type 1:
```bash
python scd_type1.py
```

### Chạy điều phối tổng hợp qua Menu dòng lệnh:
```bash
python run_scd_pipeline.py
```
*(Truyền trực tiếp tham số: `python run_scd_pipeline.py 4` để chạy toàn bộ từ A đến Z).*

---

## 4. Bảng Kết Quả Thực Thi Điển Hình

### Phép toán Roll-up (Câu 4.2): Doanh thu theo mô hình & khu vực
```text
Mô hình cửa hàng  Khu vực quản lý       Doanh thu
             WiN   HN - Nội thành 120,000,000 VNĐ
             WiN HN - Khu vực Tây 150,000,000 VNĐ
        WinMart+   HN - Nội thành  50,000,000 VNĐ
```

### Phép toán Drill-down: Chi tiết qua các giai đoạn
```text
    Thời gian      Tên điểm bán  Mô hình  Khu vực quản lý Hạng KH lúc mua                     Ngành hàng lúc bán       Doanh thu
Tháng 02/2026 WinMart+ Cầu Giấy WinMart+   HN - Nội thành      WIN Silver                    Thực phẩm tươi sống  50,000,000 VNĐ
Tháng 07/2026      WiN Cầu Giấy      WiN   HN - Nội thành         WIN VIP Thực phẩm Công nghệ Mát (Chilled Food) 120,000,000 VNĐ
Tháng 12/2026      WiN Cầu Giấy      WiN HN - Khu vực Tây        WIN Gold Thực phẩm Công nghệ Mát (Chilled Food) 150,000,000 VNĐ
```

### Minh chứng đối chứng SCD Type 2 vs SCD Type 1:
- **Nếu dùng sai SCD Type 1** (ghi đè `store_type = 'WiN'`): Toàn bộ 50 triệu doanh thu lúc còn là WinMart+ sẽ bị cộng nhầm sang WiN (`WinMart+ = 0 VNĐ`, `WiN = 320,000,000 VNĐ`), làm sai lệch hoàn toàn báo cáo đánh giá tái cơ cấu của Ban Lãnh đạo!
- **Chuẩn SCD Type 2** bảo toàn chính xác lịch sử hoạt động và gắn doanh thu đúng với phiên bản điểm bán tại thời điểm giao dịch.
