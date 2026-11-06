# README – HƯỚNG DẪN TRIỂN KHAI ETL DATA WAREHOUSE WINCOMMERCE
**Phạm vi: Câu 10 – Nạp dữ liệu vào các bảng Dimension (DIM) và Câu 11 – Nạp dữ liệu vào các bảng Fact (FACT)**

---

## 1. Mục tiêu

Tài liệu này thống nhất cách triển khai quá trình ETL (Extract – Transform – Load) để đưa dữ liệu từ hai hệ thống cơ sở dữ liệu nghiệp vụ OLTP vào Data Warehouse của WinCommerce.

Các thành viên thực hiện Câu 10 và Câu 11 cần tuân thủ chung kiến trúc, cách tổ chức database, quy tắc đặt tên bảng và phương pháp kiểm tra dữ liệu nhằm đảm bảo các bảng DIM và FACT có thể liên kết với nhau chính xác.

**Nguyên tắc quan trọng:**
- Không thay đổi hoặc ghi đè dữ liệu trong hai database nguồn.
- Không đặt các bảng DIM và FACT trực tiếp trong database nghiệp vụ.
- Toàn bộ DIM và FACT được tập trung trong database `wincommerce_dw`.
- Sử dụng Python để thực hiện ETL.
- Dữ liệu FACT phải liên kết với các DIM thông qua Surrogate Key (SK), không sử dụng trực tiếp khóa tự nhiên của hệ thống nguồn làm khóa ngoại trong Data Warehouse.

## 2. Kiến trúc tổng thể

Hệ thống sử dụng hai database nguồn độc lập:

| Hệ thống | Database | Vai trò |
|---|---|---|
| Microsoft SQL Server | `multichannel_sales` | Nguồn dữ liệu nghiệp vụ bán lẻ đa kênh |
| PostgreSQL | `logistics_purchasing` | Nguồn dữ liệu nghiệp vụ chuỗi cung ứng và tồn kho |
| PostgreSQL | `wincommerce_dw` | Database đích – Data Warehouse |

Luồng xử lý dữ liệu:

```text
SQL Server
multichannel_sales
        |
        | Extract
        v
Python ETL <---------------- PostgreSQL
        |                    logistics_purchasing
        |                         |
        |                         | Extract
        v                         |
PostgreSQL: wincommerce_dw <------+
        |
        +---- Schema: staging
        |       Dữ liệu trung gian
        |
        +---- Schema: dw
        |       9 bảng DIM
        |       Các bảng FACT
        |
        +---- Schema: etl_log
                Nhật ký ETL
```

**Lưu ý:** `staging`, `dw` và `etl_log` là các schema nằm bên trong database `wincommerce_dw`, không phải ba database riêng biệt.

## 3. Cấu trúc database và schema

Sau khi tạo database `wincommerce_dw`, cần tạo ba schema:

```sql
CREATE SCHEMA staging;
CREATE SCHEMA dw;
CREATE SCHEMA etl_log;
```

### 3.1. Schema `staging`

Đây là khu vực trung gian lưu dữ liệu được trích xuất từ hai database nguồn.

Ví dụ các bảng có thể tạo:

| Bảng Staging | Nguồn dữ liệu |
|---|---|
| `staging.stg_stores` | SQL Server – `stores` |
| `staging.stg_regions` | SQL Server – `regions` |
| `staging.stg_products` | SQL Server – `products` |
| `staging.stg_categories` | SQL Server – `categories` |
| `staging.stg_customers` | SQL Server – `customers` |
| `staging.stg_promotions` | SQL Server – `promotions` |
| `staging.stg_warehouses` | PostgreSQL – `warehouses` |
| `staging.stg_suppliers` | PostgreSQL – `suppliers` |
| `staging.stg_supplier_products` | PostgreSQL – `supplier_products` |
| `staging.stg_goods_receipt_items` | PostgreSQL – `goods_receipt_items` |

Danh sách trên mang tính định hướng. Chỉ tạo các bảng thực sự cần thiết cho quá trình ETL.

Staging không nhất thiết phải chứa toàn bộ dữ liệu nguồn. Python chỉ trích xuất những trường cần thiết để xây dựng DIM và FACT.

### 3.2. Schema `dw`

Đây là khu vực lưu trữ chính thức của Data Warehouse, bao gồm 9 bảng Dimension và các bảng Fact theo thiết kế Star Schema.

**Chín bảng DIM:**

| STT | Tên bảng | Mục đích |
|---|---|---|
| 1 | `dw.dim_date` | Thông tin ngày, tuần, tháng, quý, năm |
| 2 | `dw.dim_time` | Thông tin giờ, phút và khung thời gian |
| 3 | `dw.dim_store` | Thông tin cửa hàng |
| 4 | `dw.dim_warehouse` | Thông tin kho hàng |
| 5 | `dw.dim_product` | Thông tin sản phẩm |
| 6 | `dw.dim_customer` | Thông tin khách hàng |
| 7 | `dw.dim_promotion` | Thông tin chương trình khuyến mãi |
| 8 | `dw.dim_supplier` | Thông tin nhà cung cấp |
| 9 | `dw.dim_batch` | Thông tin lô hàng |

Các bảng FACT được tạo theo mô hình dữ liệu đã thống nhất trong thiết kế Data Warehouse. Ví dụ: `fact_sales_transaction`, `fact_inventory`, `fact_purchase_order` nếu có trong Star Schema của nhóm.

### 3.3. Schema `etl_log`

Dùng để ghi nhận lịch sử chạy ETL, giúp kiểm tra quá trình nạp dữ liệu.

Có thể tạo các bảng:
- `etl_log.etl_run_log`: thời gian chạy, tên quy trình, trạng thái và số bản ghi xử lý.
- `etl_log.etl_error_log`: thông tin lỗi trong quá trình trích xuất, chuyển đổi hoặc nạp dữ liệu.

Schema này phục vụ quản lý và kiểm tra, không phải nơi lưu dữ liệu phân tích.

## 4. Câu 10 – ETL dữ liệu vào 9 bảng DIM

### 4.1. Nguồn dữ liệu của các bảng DIM

| Bảng DIM | Database nguồn | Bảng nguồn |
|---|---|---|
| DIM_DATE | Python tạo tự động | Không có bảng nguồn trực tiếp |
| DIM_TIME | SQL Server và PostgreSQL | Các bảng nghiệp vụ có trường thời gian |
| DIM_STORE | SQL Server | `stores`, `regions` |
| DIM_WAREHOUSE | PostgreSQL | `warehouses` |
| DIM_PRODUCT | SQL Server và PostgreSQL | `products`, `categories`, `supplier_products` |
| DIM_CUSTOMER | SQL Server | `customers` |
| DIM_PROMOTION | SQL Server | `promotions`, `promotion_products` |
| DIM_SUPPLIER | PostgreSQL | `suppliers`, `supplier_products` |
| DIM_BATCH | PostgreSQL | `goods_receipt_items`, `goods_receipts` |

### 4.2. Quy trình thực hiện

Mỗi bảng DIM cần thực hiện các bước:

**Bước 1 – Extract: Trích xuất dữ liệu**

Python kết nối tới database nguồn và lấy dữ liệu từ các bảng nghiệp vụ tương ứng.

Ví dụ:
- Lấy `stores` và `regions` từ SQL Server.
- Lấy `warehouses` từ PostgreSQL.
- Lấy `products` và `categories` từ SQL Server.

**Bước 2 – Staging: Lưu dữ liệu trung gian**

Đưa dữ liệu vừa trích xuất vào các bảng tương ứng trong schema `staging` của `wincommerce_dw`.

**Bước 3 – Transform: Xử lý dữ liệu**

Thực hiện:
- Kiểm tra dữ liệu NULL và dữ liệu không hợp lệ.
- Chuẩn hóa định dạng dữ liệu.
- Loại bỏ bản ghi trùng lặp.
- JOIN các bảng nguồn khi cần thiết.
- Đối chiếu mã định danh giữa hai hệ thống.
- Tính toán các thuộc tính bổ sung.
- Cấp phát Surrogate Key.
- Áp dụng SCD Type 1 hoặc Type 2 đối với những thuộc tính cần quản lý lịch sử.

**Bước 4 – Load: Nạp dữ liệu**

Nạp dữ liệu đã xử lý vào bảng DIM tương ứng thuộc schema `dw`.

Ví dụ:

```text
staging.stg_stores
        |
        | JOIN regions
        | Transform
        | Generate store_dim_id
        v
dw.dim_store
```

### 4.3. Quy tắc khóa của DIM

Mỗi bảng DIM cần có:
- **Surrogate Key (SK):** Khóa thay thế, dùng làm khóa chính trong Data Warehouse.
- **Natural Key (NK):** Mã định danh gốc từ hệ thống nghiệp vụ.
- **Các thuộc tính mô tả:** Tên, loại, trạng thái, địa chỉ và các thông tin liên quan.

Ví dụ:

```text
dw.dim_store

store_dim_id     -- Surrogate Key (Primary Key)
store_id         -- Natural Key
store_name
store_type
region_id
region_name
province
district
status
```

Với DIM sử dụng SCD Type 2, cần bổ sung các trường quản lý phiên bản lịch sử theo thiết kế, chẳng hạn:

```text
effective_from
effective_to
is_current
version_num
row_hash
```

Không mặc định tất cả 9 DIM đều phải áp dụng SCD Type 2. DIM_DATE và DIM_TIME thường không cần SCD; DIM_BATCH thường đại diện cho từng lô hàng riêng biệt thay vì tạo phiên bản lịch sử như DIM_STORE.

### 4.4. Lưu ý riêng đối với DIM_PRODUCT

DIM_PRODUCT lấy dữ liệu từ cả SQL Server và PostgreSQL.

Nhóm cần thống nhất quy tắc ánh xạ mã sản phẩm giữa:
- `multichannel_sales.products`
- `logistics_purchasing.supplier_products`

Mục tiêu là một sản phẩm chỉ có một định danh thống nhất trong Data Warehouse, tránh tạo hai bản ghi DIM_PRODUCT cho cùng một sản phẩm do khác mã hoặc cách biểu diễn giữa các nguồn.

## 5. Câu 11 – ETL dữ liệu vào các bảng FACT

Câu 11 tiếp tục sử dụng kiến trúc ETL đã triển khai ở Câu 10, nhưng đối tượng nạp dữ liệu là các bảng Fact.

### 5.1. Mục tiêu

Thực hiện ETL dữ liệu từ hai hệ thống cơ sở dữ liệu nguồn vào 4 bảng FACT trong Data Warehouse `wincommerce_dw`, theo đúng mô hình Star Schema đã thiết kế tại Câu 8.

Danh sách các bảng FACT cần triển khai:

| STT | Bảng FACT | Nguồn dữ liệu |
|---|---|---|
| 1 | `FACT_SALES_TRANSACTION` | SQL Server – `multichannel_sales` |
| 2 | `FACT_RETAIL_DAILY` | SQL Server – `multichannel_sales` |
| 3 | `FACT_GOODS_RECEIPT_LINE` | PostgreSQL – `logistics_purchasing` |
| 4 | `FACT_INVENTORY_DAILY_SNAPSHOT` | PostgreSQL – `logistics_purchasing` |

Các bảng FACT phải sử dụng khóa ngoại là Surrogate Key của các DIM tương ứng.

Ví dụ minh họa:

```text
dw.fact_sales_transaction

sales_fact_id
date_dim_id
time_dim_id
store_dim_id
product_dim_id
customer_dim_id
promotion_dim_id
quantity
unit_price
discount_amount
total_amount...
```

Các cột và DIM liên kết thực tế phải căn cứ vào Star Schema đã thống nhất của nhóm. Không tự ý thêm DIM hoặc FACT nếu không có trong thiết kế.

### 5.2. Quy trình nạp FACT

**Bước 1 – Extract**

Trích xuất dữ liệu giao dịch từ các bảng nguồn OLTP.

**Bước 2 – Staging**

Đưa dữ liệu giao dịch vào các bảng trung gian thuộc schema `staging`.

**Bước 3 – Transform**

- Làm sạch dữ liệu giao dịch.
- Kiểm tra dữ liệu thiếu hoặc trùng lặp.
- Chuẩn hóa các mã định danh.
- Tính toán các chỉ số đo lường nếu cần.
- Xác định các DIM tương ứng với từng giao dịch.

**Bước 4 – Lookup Surrogate Key**

Đây là bước rất quan trọng.

Dữ liệu nguồn thường sử dụng Natural Key, trong khi FACT cần lưu Surrogate Key.

Ví dụ:

```text
Nguồn:
store_id = 'ST01'
product_id = 'P001'

Tra cứu:
dw.dim_store
dw.dim_product

Kết quả:
store_dim_id = 1
product_dim_id = 5
```

Sau đó, các khóa thay thế được đưa vào FACT thay cho mã gốc.

Đối với DIM có SCD Type 2, cần xác định đúng phiên bản DIM tại thời điểm phát sinh giao dịch, dựa trên khoảng hiệu lực:

```sql
effective_from <= transaction_time
AND transaction_time < effective_to
```

**Bước 5 – Load**

Nạp bản ghi FACT vào schema `dw`, đảm bảo các khóa ngoại tham chiếu đúng đến DIM.

**Bước 6 – Validate**

Kiểm tra:
- Số lượng bản ghi nguồn và số lượng bản ghi đích.
- Không phát sinh khóa ngoại không tồn tại.
- Không nạp trùng giao dịch.
- Các chỉ số như doanh thu, số lượng và giá trị chiết khấu được tính đúng.
- Có xử lý rõ ràng các bản ghi không tìm thấy DIM tương ứng.

## 6. Cấu trúc thư mục dự án đề xuất

Các thành viên nên thống nhất cấu trúc mã nguồn như sau:

```text
WinCommerce-DWH/
│
├── README.md
│
├── config/
│   └── db_config.py
│
├── connections/
│   ├── sqlserver_connection.py
│   └── postgres_connection.py
│
├── scripts/
│   ├── 01_create_database_schema.sql
│   ├── 02_create_dim_tables.sql
│   └── 03_create_fact_tables.sql
│
├── etl/
│   ├── extract/
│   │   ├── extract_sqlserver.py
│   │   └── extract_postgres.py
│   │
│   ├── transform/
│   │   ├── transform_dim.py
│   │   └── transform_fact.py
│   │
│   ├── load/
│   │   ├── load_dim.py
│   │   └── load_fact.py
│   │
│   ├── dim/
│   │   ├── load_dim_date.py
│   │   ├── load_dim_time.py
│   │   ├── load_dim_store.py
│   │   ├── load_dim_warehouse.py
│   │   ├── load_dim_product.py
│   │   ├── load_dim_customer.py
│   │   ├── load_dim_promotion.py
│   │   ├── load_dim_supplier.py
│   │   └── load_dim_batch.py
│   │
│   ├── fact/
│   │   └── load_fact.py
│   │
│   └── main.py
│
├── screenshots/
│   ├── câu10/
│   └── câu11/
│
└── requirements.txt
```

Đây là cấu trúc tham khảo. Có thể điều chỉnh theo cách phân công của nhóm, nhưng cần giữ thống nhất tên database, schema và bảng.

**Lưu ý bảo mật:** Không commit mật khẩu database, tài khoản đăng nhập hoặc thông tin kết nối nhạy cảm lên GitHub. Nên sử dụng file `.env` và đưa file này vào `.gitignore`.

## 7. Kiểm tra kết quả sau khi ETL

Sau khi chạy ETL, cần kiểm tra số lượng bản ghi của 9 bảng DIM.

```sql
SELECT 'dim_date' AS table_name, COUNT(*) AS total_records
FROM dw.dim_date

UNION ALL
SELECT 'dim_time', COUNT(*) FROM dw.dim_time

UNION ALL
SELECT 'dim_store', COUNT(*) FROM dw.dim_store

UNION ALL
SELECT 'dim_warehouse', COUNT(*) FROM dw.dim_warehouse

UNION ALL
SELECT 'dim_product', COUNT(*) FROM dw.dim_product

UNION ALL
SELECT 'dim_customer', COUNT(*) FROM dw.dim_customer

UNION ALL
SELECT 'dim_promotion', COUNT(*) FROM dw.dim_promotion

UNION ALL
SELECT 'dim_supplier', COUNT(*) FROM dw.dim_supplier

UNION ALL
SELECT 'dim_batch', COUNT(*) FROM dw.dim_batch;
```

Kiểm tra dữ liệu thực tế:

```sql
SELECT *
FROM dw.dim_store
LIMIT 10;
```

Kiểm tra khóa chính bị trùng:

```sql
SELECT store_dim_id, COUNT(*)
FROM dw.dim_store
GROUP BY store_dim_id
HAVING COUNT(*) > 1;
```

Các câu lệnh tương tự cần được thực hiện đối với những bảng DIM và FACT còn lại.

## 8. Hình ảnh minh chứng cần chuẩn bị cho báo cáo

Yêu cầu của Câu 10 và Câu 11 không chỉ là viết mô tả ETL mà còn cần minh họa việc nạp dữ liệu thành công.

Mỗi thành viên phụ trách cần chụp ảnh kết quả thực tế, không sử dụng dữ liệu hoặc ảnh minh họa giả lập.

### Câu 10 – DIM

Các ảnh cần có:
1. Kết nối Python thành công đến SQL Server và PostgreSQL.
2. Kết quả trích xuất dữ liệu từ các bảng nguồn.
3. Kết quả tạo và nạp dữ liệu vào 9 bảng DIM.
4. Kết quả truy vấn `SELECT *` trên các bảng DIM.
5. Kết quả thống kê số lượng bản ghi của 9 DIM.

### Câu 11 – FACT

Các ảnh cần có:
1. Kết quả trích xuất dữ liệu giao dịch từ nguồn.
2. Kết quả tra cứu Surrogate Key từ các DIM.
3. Kết quả nạp dữ liệu vào các bảng FACT.
4. Kết quả truy vấn dữ liệu FACT sau khi nạp.
5. Kết quả kiểm tra khóa ngoại và đối chiếu số liệu.


## 9. Quy định phối hợp giữa các thành viên

Để tránh xung đột khi làm việc chung:

1. Mọi người sử dụng chung database đích `wincommerce_dw`.
2. Không tự ý thay đổi cấu trúc các bảng DIM/FACT đã được thống nhất theo nội dung câu 7 - câu 8.
3. Trước khi viết ETL cho FACT, cần đảm bảo các DIM liên quan đã được tạo và nạp dữ liệu.
4. Các thành viên cần thống nhất tên cột khóa chính, khóa tự nhiên và khóa ngoại theo nội dung câu 7 - câu 8.
5. Mỗi người nên phụ trách một phần độc lập, tránh nhiều người cùng sửa một file ETL hoặc chạy nạp dữ liệu đồng thời.
6. Nếu cần thay đổi thiết kế bảng, phải thông báo cho các thành viên phụ trách những bảng có liên quan.
7. Mỗi thành viên cần cung cấp mã nguồn, ảnh minh chứng và kết quả kiểm tra cho phần mình thực hiện.

## 10. Trình tự triển khai thống nhất

| Thứ tự | Công việc | Phụ trách |
|---:|---|---|
| 1 | Tạo database `wincommerce_dw` và ba schema | Thành viên phụ trách database |
| 2 | Kiểm tra kết nối SQL Server và PostgreSQL | Thành viên phụ trách kết nối |
| 3 | Tạo các bảng DIM và FACT theo thiết kế | Thành viên phụ trách cấu trúc DW |
| 4 | Viết ETL và nạp 9 bảng DIM | Nhóm Câu 10 |
| 5 | Kiểm tra DIM và Surrogate Key | Nhóm Câu 10 |
| 6 | Viết ETL và nạp các bảng FACT | Nhóm Câu 11 |
| 7 | Kiểm tra khóa ngoại và số liệu FACT | Nhóm Câu 11 |
| 8 | Chạy thử toàn bộ quy trình và chụp ảnh minh chứng | Cả nhóm |
| 9 | Hoàn thiện nội dung báo cáo Câu 10 và Câu 11 | Cả nhóm |

---
