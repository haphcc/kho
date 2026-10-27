--1. DB multichannel_sales — Top sản phẩm bán chạy theo từng danh mục (phục vụ Khối Kinh doanh bán lẻ & Ban Lãnh đạo)
--Trả về top 5 sản phẩm doanh thu cao nhất trong mỗi danh mục (chỉ tính đơn COMPLETED), 
--dùng cho báo cáo hiệu quả bán hàng theo ngành hàng.

WITH product_sales AS (
    SELECT
        p.category_id,
        c.category_name,
        p.product_id,
        p.product_name,
        SUM(oi.quantity)     AS total_qty_sold,
        SUM(oi.line_amount)  AS total_net_revenue
    FROM sales_order_items oi
    INNER JOIN sales_orders o ON o.order_id = oi.order_id
    INNER JOIN products p     ON p.product_id = oi.product_id
    INNER JOIN categories c   ON c.category_id = p.category_id
    WHERE o.order_status = 'COMPLETED'
        AND o.order_datetime >= '2026-01-01'
        AND o.order_datetime <  '2026-09-01'
    GROUP BY p.category_id, c.category_name, p.product_id, p.product_name
),
ranked AS (
    SELECT
        *,
        RANK() OVER (PARTITION BY category_id ORDER BY total_net_revenue DESC) AS rank_in_category
    FROM product_sales
)
SELECT category_name, product_name, total_qty_sold, total_net_revenue, rank_in_category
FROM ranked
WHERE rank_in_category <= 5
ORDER BY category_name, rank_in_category;

--2. DB logistics_purchasing — Cảnh báo tồn kho dưới mức tái đặt hàng kèm gợi ý nhà cung cấp 
--(phục vụ Khối Mua hàng & Khối Chuỗi cung ứng)
--Liệt kê các lô hàng/kho đã chạm hoặc dưới reorder_point (theo available_qty sinh sẵn từ cột computed), 
--kèm nhà cung cấp có lead_time_days ngắn nhất để ưu tiên đặt mua bổ sung.
SELECT
    w.warehouse_name,
    ib.product_id,
    ib.batch_no,
    ib.on_hand_qty,
    ib.reserved_qty,
    ib.available_qty,
    ib.reorder_point,
    best_sp.supplier_id,
    s.supplier_name,
    best_sp.unit_cost,
    best_sp.lead_time_days
FROM inventory_balance ib
INNER JOIN warehouses w ON w.warehouse_id = ib.warehouse_id
OUTER APPLY (
    SELECT TOP 1 sp.supplier_id, sp.unit_cost, sp.lead_time_days
    FROM supplier_products sp
    WHERE sp.product_id = ib.product_id
        AND sp.status = 'ACTIVE'
    ORDER BY sp.lead_time_days ASC, sp.unit_cost ASC
) best_sp
LEFT JOIN suppliers s ON s.supplier_id = best_sp.supplier_id
WHERE ib.available_qty <= ib.reorder_point
    AND w.status = 'ACTIVE'
ORDER BY w.warehouse_name, ib.available_qty ASC;

--3. DB multichannel_sales — Báo cáo khách hàng mới và tần suất quay lại mua hàng theo tuần
--(phục vụ Khối Kinh doanh bán lẻ & Ban Lãnh đạo)
--Theo từng tuần đăng ký (join_date, tuần bắt đầu thứ Hai) và từng kênh đăng ký: số khách
--hàng mới, số khách đã phát sinh đơn COMPLETED đầu tiên, số khách quay lại mua đơn
--COMPLETED lần 2 trong vòng 30 ngày kể từ đơn đầu tiên, và tỷ lệ quay lại (%).

WITH weekly_new_customers AS (
    SELECT
        customer_id,
        registration_channel,
        -- Tuần bắt đầu thứ Hai, không phụ thuộc cấu hình DATEFIRST của server
        DATEADD(WEEK, DATEDIFF(WEEK, 0, join_date), 0) AS week_start
    FROM customers
    WHERE join_date >= '2026-01-01'
        AND join_date <  '2026-09-01'
),
customer_orders_ranked AS (
    SELECT
        o.customer_id,
        o.order_datetime,
        ROW_NUMBER() OVER (PARTITION BY o.customer_id ORDER BY o.order_datetime ASC) AS order_seq
    FROM sales_orders o
    WHERE o.order_status = 'COMPLETED'
        AND o.customer_id IS NOT NULL
),
first_second_orders AS (
    SELECT
        f.customer_id,
        f.order_datetime AS first_order_datetime,
        s.order_datetime AS second_order_datetime
    FROM customer_orders_ranked f
    LEFT JOIN customer_orders_ranked s
        ON s.customer_id = f.customer_id AND s.order_seq = 2
    WHERE f.order_seq = 1
)
SELECT
    wnc.week_start,
    wnc.registration_channel,
    COUNT(DISTINCT wnc.customer_id)                                        AS new_customers,
    COUNT(DISTINCT fso.customer_id)                                        AS new_customers_with_first_order,
    COUNT(DISTINCT CASE
        WHEN fso.second_order_datetime IS NOT NULL
            AND fso.second_order_datetime <= DATEADD(DAY, 30, fso.first_order_datetime)
        THEN fso.customer_id
    END)                                                                    AS repeat_customers_30d,
    CAST(
        CASE WHEN COUNT(DISTINCT fso.customer_id) = 0 THEN 0
             ELSE 100.0 * COUNT(DISTINCT CASE
                    WHEN fso.second_order_datetime IS NOT NULL
                        AND fso.second_order_datetime <= DATEADD(DAY, 30, fso.first_order_datetime)
                    THEN fso.customer_id
                  END) / COUNT(DISTINCT fso.customer_id)
        END AS DECIMAL(5,2)
    )                                                                       AS repeat_rate_pct_30d
FROM weekly_new_customers wnc
LEFT JOIN first_second_orders fso ON fso.customer_id = wnc.customer_id
GROUP BY wnc.week_start, wnc.registration_channel
ORDER BY wnc.week_start, wnc.registration_channel;
