# Logic lõi SCD Type 2 cho DIM_STORE và DIM_CUSTOMER

Tài liệu này rút gọn logic chính từ `run_scd_type2_cases.py`. Mục tiêu là minh họa cách một hàm SCD Type 2 dùng chung xử lý hai bảng Dimension mà không ghi đè lịch sử.

## 1. Thuộc tính dùng để phát hiện thay đổi

```python
STORE_SCD_COLUMNS = ("store_type", "region_id", "region_name", "status")
CUSTOMER_SCD_COLUMNS = ("member_level", "status")
```

`store_id` và `customer_id` là business key. Các cột trên là thuộc tính SCD Type 2: nếu giá trị thay đổi thì ETL đóng bản ghi cũ và sinh phiên bản mới.

## 2. Tạo row hash

Hash chỉ tính từ thuộc tính SCD Type 2, vì vậy thay đổi ở các thuộc tính này sẽ tạo hash mới.

```python
import hashlib
import json


def make_hash(record: dict, columns: tuple[str, ...]) -> str:
    values = {column: record[column] for column in columns}
    payload = json.dumps(
        values,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return hashlib.md5(payload.encode("utf-8")).hexdigest()
```

Ví dụ, hash của cửa hàng sẽ thay đổi khi `store_type` đổi từ `WINMART_PLUS` sang `WIN`, hoặc khi `region_id` đổi từ `91` sang `28`.

## 3. Hàm xử lý SCD Type 2 dùng chung

```python
from datetime import datetime
from sqlalchemy import text
FAR_FUTURE = datetime(2099, 12, 31, 23, 59, 59)

def apply_scd2_change(
    conn,
    table: str,
    business_key: str,
    key_value: str,
    scd_columns: tuple[str, ...],
    effective_from: datetime,
    changes: dict,
) -> None:
    # Khóa dòng hiện hành để tránh hai ETL cùng tạo version mới.
    current = conn.execute(
        text(
            f"SELECT * FROM {table} "
            f"WHERE {business_key} = :key_value "
            "AND is_current = TRUE FOR UPDATE"
        ),
        {"key_value": key_value},
    ).mappings().first()
    if current is None:
        raise RuntimeError("Không có phiên bản hiện hành để đóng.")

    # Tạo snapshot mới bằng cách giữ thuộc tính cũ và áp dụng thay đổi.
    next_record = dict(current)
    next_record.update(changes)

    current_hash = current["row_hash"] or make_hash(current, scd_columns)
    next_hash = make_hash(next_record, scd_columns)
    # Không thay đổi thuộc tính SCD2 thì không tạo version mới.
    if current_hash == next_hash:
        return
    # Đóng phiên bản đang hiệu lực.
    conn.execute(
        text(
            f"UPDATE {table} "
            "SET effective_to = :effective_from, is_current = FALSE "
            f"WHERE {business_key} = :key_value AND is_current = TRUE"
        ),
        {"effective_from": effective_from, "key_value": key_value},
    )
    # Chuẩn bị và chèn phiên bản mới.
    next_record.pop("store_dim_id", None)
    next_record.pop("customer_dim_id", None)
    next_record.update(
        {
            "effective_from": effective_from,
            "effective_to": FAR_FUTURE,
            "is_current": True,
            "version_num": int(current["version_num"]) + 1,
            "row_hash": next_hash,
        }
    )
    columns = list(next_record)
    sql_columns = ", ".join(columns)
    sql_values = ", ".join(f":{column}" for column in columns)
    conn.execute(
        text(
            f"INSERT INTO {table} "
            f"({sql_columns}) VALUES ({sql_values})"
        ),
        next_record,
    )
```

## 4. Gọi hàm cho hai trường hợp mô phỏng

```python
# ST-0001 trong hệ thống nguồn tương ứng store_id = '1' trong DIM hiện tại.
apply_scd2_change(
    conn=conn,
    table="dw.dim_store",
    business_key="store_id",
    key_value="1",
    scd_columns=STORE_SCD_COLUMNS,
    effective_from=datetime(2026, 5, 1),
    changes={"store_type": "WIN"},
)

apply_scd2_change(
    conn=conn,
    table="dw.dim_store",
    business_key="store_id",
    key_value="1",
    scd_columns=STORE_SCD_COLUMNS,
    effective_from=datetime(2026, 11, 1),
    changes={
        "region_id": "28",
        "region_name": "Khu vực Hải Dương",
    },
)

# Khách hàng customer_id = '101'.
apply_scd2_change(
    conn=conn,
    table="dw.dim_customer",
    business_key="customer_id",
    key_value="101",
    scd_columns=CUSTOMER_SCD_COLUMNS,
    effective_from=datetime(2026, 5, 1),
    changes={"member_level": "PLATINUM"},
)

apply_scd2_change(
    conn=conn,
    table="dw.dim_customer",
    business_key="customer_id",
    key_value="101",
    scd_columns=CUSTOMER_SCD_COLUMNS,
    effective_from=datetime(2026, 11, 1),
    changes={"member_level": "GOLD"},
)
```

## 5. Kết quả sau mỗi lần thay đổi

| Bước | Version cũ | Version mới |
|---|---|---|
| Thay đổi lần 1 | `effective_to` bằng ngày thay đổi, `is_current = FALSE` | `version_num + 1`, `is_current = TRUE` |
| Thay đổi lần 2 | Tiếp tục đóng dòng hiện hành | Tạo phiên bản mới với hash mới |

Vì vậy, mỗi business key có thể có nhiều dòng lịch sử nhưng chỉ có một dòng `is_current = TRUE`. Giao dịch FACT được liên kết với dòng DIM thỏa điều kiện `effective_from <= transaction_time AND transaction_time < effective_to`.
