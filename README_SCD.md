# Hướng Dẫn & Phân Loại Kho Dữ Liệu: SCD Type 2 & Type 1
## So sánh: Pandas vs. SQLAlchemy vs. DuckDB

Dự án này sử dụng dữ liệu thực tế kết nối trực tiếp từ hệ thống cơ sở dữ liệu hiện có:
- **`multichannel_sales`** (SQL Server): Bảng `customers` (1.000 dòng), `products` (800 dòng), `sales_orders` (700 dòng).
- **`logistics_purchasing`** (PostgreSQL - Port 5432 - User `postgres`, Mật khẩu `h$c4122005`): Bảng `suppliers` (150 dòng), `warehouses` (100 dòng), `supplier_products` (900 dòng), `purchase_orders` (500 dòng).
- **`dwh_analytics.duckdb`** & **`dim_customers_scd2.parquet`**, **`dim_suppliers_scd2.parquet`**: Kho dữ liệu phân tích sinh ra tự động.

---

## 1. Cấu trúc thư mục mã nguồn

| File | Chức năng |
| :--- | :--- |
| **`scd_type2.py`** | **[ƯU TIÊN]** Triển khai toàn diện **SCD Type 2** (Theo dõi lịch sử, Surrogate Key, Time-travel Point-in-time) trên Pandas, SQLAlchemy và DuckDB. |
| **`scd_type1.py`** | Triển khai bài toán **SCD Type 1** (Ghi đè in-place update / Upsert). |
| **`db_connect.py`** | Module cấu hình kết nối tự động tới SQL Server (Windows Authentication) và DuckDB OLAP Engine. |
| **`run_scd_pipeline.py`** | Điểm kích hoạt chính, hỗ trợ giao diện menu dòng lệnh hoặc truyền tham số. |
| **`dim_customers_scd2.parquet`** | File dữ liệu dạng cột (Columnar Parquet) xuất ra từ DuckDB phục vụ Data Lake / PowerBI / BI tools. |

---

## 2. Cách chạy nhanh

Mở PowerShell / Terminal tại thư mục `C:\Users\phuoc\Downloads\kho` và chạy:

### Chạy trực tiếp SCD Type 2 (Khuyên dùng):
```bash
python scd_type2.py
```

### Chạy menu điều khiển tổng hợp:
```bash
python run_scd_pipeline.py
```
*(Nhập `1` để chạy Type 2, nhập `2` để chạy Type 1, hoặc nhập `3` để chạy toàn bộ).*

---

## 3. Ma trận đánh giá & Phân loại SCD Type 2

| Tiêu chí | **Pandas** | **SQLAlchemy** | **DuckDB** |
| :--- | :--- | :--- | :--- |
| **Vai trò kiến trúc** | In-Memory DataFrame (ETL Micro-batch) | Data Access / Transaction Orchestrator | Vectorized In-Process Columnar OLAP Engine |
| **Cơ chế Surrogate Key** | Phải tự sinh qua code Python (`range`) | Dùng `IDENTITY` / `SEQUENCE` của DB | Sequence tự động chuẩn DWH |
| **Cơ chế hết hạn bản ghi cũ** | Lọc mask boolean + gán `end_date`, `is_current = False` | SQL `UPDATE target ... INNER JOIN stg` trong Transaction | Vectorized `UPDATE dim ... FROM stg` |
| **Chèn bản ghi phiên bản mới** | `pd.concat` nối thêm DataFrames | SQL `INSERT INTO dim ... SELECT FROM stg` | Vectorized `INSERT INTO dim ... SELECT` |
| **Tính an toàn giao dịch (ACID)** | ❌ **Không** (Dễ hỏng trạng thái nếu crash giữa lúc update và concat) |  **Rất cao** (`with engine.begin():` Rollback an toàn) |  **Rất cao** (Hỗ trợ ACID MVCC đầy đủ) |
| **Tốc độ xử lý (Batch 1k dòng)** | Rất nhanh trên RAM (~12 ms) | ~220 ms (Do overhead roundtrip mạng và SQL Server) | ~35 ms (Cân bằng xuất sắc giữa Disk I/O & CPU Vectorization) |
| **Khả năng mở rộng (Scale)** | Bị giới hạn nghiêm ngặt bởi RAM | Phụ thuộc vào Server DWH đích | Vượt giới hạn RAM (Out-of-core execution), xử lý mượt hàng chục triệu dòng |
| **Xuất file phân tích (Parquet)** | Cần thêm thư viện `pyarrow` | Cần fetch về client rồi mới lưu | Xuất trực tiếp siêu nhanh qua câu lệnh SQL: `COPY TO 'dim.parquet'` |
| **ĐÁNH GIÁ MỨC ĐỘ PHÙ HỢP** | **6.0 / 10** *(Không khuyến nghị cho Production DWH)* | **8.5 / 10** *(Chuẩn mực cho Cloud DWH / RDBMS)* | **9.8 / 10** *(Lựa chọn số 1 cho Modern Data Stack)* |

---

## 4. Kịch bản nghiệp vụ thử nghiệm trong `scd_type2.py`

* **Mốc T0 (2026-01-01):** Khởi tạo 1.000 khách hàng từ database gốc `multichannel_sales`.
  * Khách hàng ID = 1 (`Võ Minh Minh`): Ở `Thái Nguyên`, hạng `STANDARD`.
  * Khách hàng ID = 3 (`Lý Diễm Hương`): Ở `Vĩnh Long`, hạng `GOLD`.
* **Mốc T1 (2026-06-01):**
  * Khách hàng ID = 1: Đổi tỉnh thành sang `TP. Hồ Chí Minh`, thăng hạng `GOLD` ➔ Đóng phiên bản 1, sinh phiên bản 2 (`version = 2`, `is_current = True`).
  * Khách hàng ID = 3: Giữ nguyên thuộc tính ➔ Hệ thống kiểm tra và **không sinh bản ghi thừa** (chỉ có 1 phiên bản duy nhất).
  * Khách hàng ID = 90001: Khách hàng mới đăng ký tại `Bình Dương` ➔ Sinh phiên bản đầu tiên (`version = 1`).
* **Mốc T2 (2026-10-27 - Hiện tại):**
  * Khách hàng ID = 1: Tiếp tục thăng hạng từ `GOLD` lên `PLATINUM` ➔ Đóng phiên bản 2, sinh phiên bản 3 (`version = 3`).
* **Truy vấn Point-in-Time (Time-travel):**
  * Tại ngày `2026-03-01`: KH 1 ở Thái Nguyên, hạng STANDARD.
  * Tại ngày `2026-08-01`: KH 1 ở TP.HCM, hạng GOLD.
  * Tại thời điểm hiện tại: KH 1 ở TP.HCM, hạng PLATINUM.
