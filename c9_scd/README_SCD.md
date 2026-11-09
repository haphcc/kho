# Hướng dẫn chạy SCD Type 2 cho DIM_STORE và DIM_CUSTOMER

`run_scd_type2_cases.py` minh họa SCD Type 2 trên PostgreSQL Data Warehouse `wincommerce_dw`, schema `dw`. Script chỉ tác động hai business key trong DW:

- `dw.dim_store.store_id = '1'`, tương ứng bản ghi nguồn `stores.store_id = 1`, `store_code = 'ST-0001'`.
- `dw.dim_customer.customer_id = '101'`, tương ứng khách hàng Võ Thanh Anh.

Script không cập nhật hai hệ thống nguồn. Các thay đổi sau bản ghi baseline là snapshot mô phỏng tại tầng ETL/DW để kiểm chứng việc lưu vết lịch sử.

## Điều kiện chuẩn bị

Hai DIM phải có năm metadata SCD: `effective_from`, `effective_to`, `is_current`, `version_num`, và `row_hash`. Đồng thời, không được giữ unique constraint đơn trên `store_id` hoặc `customer_id`, vì một business key có nhiều phiên bản.

```sql
ALTER TABLE dw.dim_store
    DROP CONSTRAINT IF EXISTS dim_store_store_id_key,
    ADD COLUMN IF NOT EXISTS effective_from TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ADD COLUMN IF NOT EXISTS effective_to TIMESTAMP NOT NULL DEFAULT TIMESTAMP '2099-12-31 23:59:59',
    ADD COLUMN IF NOT EXISTS is_current BOOLEAN NOT NULL DEFAULT TRUE,
    ADD COLUMN IF NOT EXISTS version_num INT NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS row_hash CHAR(32);

ALTER TABLE dw.dim_store
    ADD CONSTRAINT uq_dim_store_version UNIQUE (store_id, version_num);

CREATE UNIQUE INDEX IF NOT EXISTS uq_dim_store_current
    ON dw.dim_store (store_id) WHERE is_current = TRUE;

ALTER TABLE dw.dim_customer
    DROP CONSTRAINT IF EXISTS dim_customer_customer_id_key,
    ADD COLUMN IF NOT EXISTS effective_from TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ADD COLUMN IF NOT EXISTS effective_to TIMESTAMP NOT NULL DEFAULT TIMESTAMP '2099-12-31 23:59:59',
    ADD COLUMN IF NOT EXISTS is_current BOOLEAN NOT NULL DEFAULT TRUE,
    ADD COLUMN IF NOT EXISTS version_num INT NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS row_hash CHAR(32);

ALTER TABLE dw.dim_customer
    ADD CONSTRAINT uq_dim_customer_version UNIQUE (customer_id, version_num);

CREATE UNIQUE INDEX IF NOT EXISTS uq_dim_customer_current
    ON dw.dim_customer (customer_id) WHERE is_current = TRUE;
```

Chỉ chạy phần `ADD CONSTRAINT` một lần. Nếu constraint/index đã tồn tại, PostgreSQL sẽ báo lỗi; khi đó bỏ qua riêng hai lệnh đó.

## Cấu hình và chạy

Script dùng `SQLAlchemy` với driver PostgreSQL `psycopg2`. Thiết lập biến môi trường theo database đích của nhóm:

```powershell
$env:PG_HOST = "127.0.0.1"
$env:PG_PORT = "5432"
$env:PG_USER = "postgres"
$env:PG_PASSWORD = "1"
$env:DW_DATABASE = "wincommerce_dw"

python c9_scd/run_scd_type2_cases.py
```

Nếu hai DIM đã chứa lịch sử của chính hai business key này và cần dựng lại đúng ba phiên bản để chụp báo cáo, chạy:

```powershell
python c9_scd/run_scd_type2_cases.py --reset-demo
```

`--reset-demo` chỉ xóa các dòng có `store_id = '1'` trong `dim_store` và `customer_id = '101'` trong `dim_customer`; không tác động các DIM hay business key khác.

## Kịch bản mô phỏng

| Thời điểm | DIM_STORE - ST-0001 / store_id 1 | DIM_CUSTOMER - customer_id 101 |
|---|---|---|
| 01/01/2026 | `WINMART_PLUS`, vùng `91 - Khu vực Hải Dương 2` | `STANDARD` |
| 01/05/2026 | Đổi mô hình sang `WIN` | Nâng hạng lên `PLATINUM` |
| 01/11/2026 | Đổi vùng sang `28 - Khu vực Hải Dương` | Hạ hạng xuống `GOLD` |

Hash MD5 của cửa hàng dùng `store_type`, `region_id`, `region_name`, `status`; hash của khách hàng dùng `member_level`, `status`. Khi hash thay đổi, script đặt `effective_to` cho phiên bản hiện hành, gán `is_current = FALSE`, rồi chèn phiên bản mới.

## Kết quả mong đợi

Mỗi DIM có ba phiên bản, với khoảng hiệu lực liên tiếp:

| Version | effective_from | effective_to | is_current |
|---|---|---|---|
| 1 | 01/01/2026 | 01/05/2026 | `FALSE` |
| 2 | 01/05/2026 | 01/11/2026 | `FALSE` |
| 3 | 01/11/2026 | 31/12/2099 23:59:59 | `TRUE` |

Script cũng in kết quả time-travel tại 15/02, 15/07 và 15/12/2026. Chạy lại không sinh thêm phiên bản nếu lịch sử đúng với kịch bản. Mọi thao tác của một lần chạy nằm trong một transaction; khi bất kỳ bước nào lỗi, PostgreSQL rollback toàn bộ lần chạy.
