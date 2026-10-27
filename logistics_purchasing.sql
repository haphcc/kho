-- =====================================================================
-- PHÂN HỆ NGHIỆP VỤ: QUẢN LÝ CHUỖI CUNG ỨNG VÀ TỒN KHO (WinCommerce)
-- Phục vụ: Khối Mua hàng, Khối Chuỗi cung ứng, Ban Lãnh đạo
-- Phiên bản SQL Server (T-SQL). Đây là database RIÊNG với
-- multichannel_sales.sql. Cột product_id chỉ lưu ID sản phẩm từ database
-- kia (không có FOREIGN KEY vật lý vì SQL Server không hỗ trợ ràng buộc
-- FK giữa 2 database khác nhau) - toàn vẹn dữ liệu do tầng ứng dụng/ETL
-- đảm bảo.
-- =====================================================================


-- 13. SUPPLIERS
CREATE TABLE suppliers (
    supplier_id BIGINT IDENTITY(1,1) NOT NULL,
    supplier_code NVARCHAR(50) NOT NULL UNIQUE,
    supplier_name NVARCHAR(200) NOT NULL,
    phone NVARCHAR(20) NULL,
    email NVARCHAR(150) NULL,
    address NVARCHAR(255) NULL,
    province NVARCHAR(100) NULL,
    status NVARCHAR(10) NOT NULL DEFAULT 'ACTIVE',

    CONSTRAINT pk_suppliers PRIMARY KEY (supplier_id),
    CONSTRAINT chk_supplier_status CHECK (status IN ('ACTIVE', 'INACTIVE'))
);
GO

CREATE INDEX idx_supplier_status ON suppliers (status);
CREATE INDEX idx_supplier_province ON suppliers (province);
GO


-- 14. WAREHOUSES
CREATE TABLE warehouses (
    warehouse_id BIGINT IDENTITY(1,1) NOT NULL,
    warehouse_code NVARCHAR(50) NOT NULL UNIQUE,
    warehouse_name NVARCHAR(200) NOT NULL,
    address NVARCHAR(255) NOT NULL,
    province NVARCHAR(100) NOT NULL,
    region NVARCHAR(10) NULL,
    status NVARCHAR(10) NOT NULL DEFAULT 'ACTIVE',

    CONSTRAINT pk_warehouses PRIMARY KEY (warehouse_id),
    CONSTRAINT chk_warehouse_region CHECK (region IN ('NORTH', 'CENTRAL', 'SOUTH')),
    CONSTRAINT chk_warehouse_status CHECK (status IN ('ACTIVE', 'INACTIVE'))
);
GO

CREATE INDEX idx_warehouse_province ON warehouses (province);
CREATE INDEX idx_warehouse_region ON warehouses (region);
CREATE INDEX idx_warehouse_status ON warehouses (status);
GO


-- 15. SUPPLIER_PRODUCTS
CREATE TABLE supplier_products (
    supplier_id BIGINT NOT NULL,
    product_id BIGINT NOT NULL,
    supplier_product_code NVARCHAR(100) NULL,
    unit_cost DECIMAL(15,2) NOT NULL DEFAULT 0,
    lead_time_days INT NOT NULL DEFAULT 0,
    min_order_qty DECIMAL(12,3) NOT NULL DEFAULT 1,
    status NVARCHAR(10) NOT NULL DEFAULT 'ACTIVE',

    CONSTRAINT pk_supplier_products PRIMARY KEY (supplier_id, product_id),
    CONSTRAINT chk_supplier_product_status CHECK (status IN ('ACTIVE', 'INACTIVE')),
    CONSTRAINT chk_supplier_product_lead_time CHECK (lead_time_days >= 0),

    CONSTRAINT fk_supplier_product_supplier
        FOREIGN KEY (supplier_id)
        REFERENCES suppliers(supplier_id)
        ON UPDATE CASCADE
        ON DELETE CASCADE
);
GO

CREATE INDEX idx_supplier_product_product ON supplier_products (product_id);
GO


-- 16. PURCHASE_ORDERS (trạng thái DRAFT/PENDING/APPROVED đóng luôn vai trò
--     quy trình duyệt mua, không tách riêng bước "đề nghị mua hàng")
CREATE TABLE purchase_orders (
    purchase_order_id BIGINT IDENTITY(1,1) NOT NULL,
    supplier_id BIGINT NOT NULL,
    warehouse_id BIGINT NOT NULL,
    po_number NVARCHAR(50) NOT NULL UNIQUE,
    order_date DATETIME2 NOT NULL DEFAULT SYSDATETIME(),
    expected_delivery_date DATE NULL,
    status NVARCHAR(15) NOT NULL DEFAULT 'DRAFT',
    total_amount DECIMAL(15,2) NOT NULL DEFAULT 0,

    CONSTRAINT pk_purchase_orders PRIMARY KEY (purchase_order_id),
    CONSTRAINT chk_po_status
        CHECK (status IN ('DRAFT', 'PENDING', 'APPROVED', 'PARTIAL', 'RECEIVED', 'CANCELLED')),

    CONSTRAINT fk_po_supplier
        FOREIGN KEY (supplier_id)
        REFERENCES suppliers(supplier_id)
        ON UPDATE CASCADE
        ON DELETE NO ACTION,

    CONSTRAINT fk_po_warehouse
        FOREIGN KEY (warehouse_id)
        REFERENCES warehouses(warehouse_id)
        ON UPDATE CASCADE
        ON DELETE NO ACTION
);
GO

CREATE INDEX idx_po_supplier ON purchase_orders (supplier_id);
CREATE INDEX idx_po_warehouse ON purchase_orders (warehouse_id);
CREATE INDEX idx_po_order_date ON purchase_orders (order_date);
CREATE INDEX idx_po_status ON purchase_orders (status);
GO


-- 17. PURCHASE_ORDER_ITEMS
CREATE TABLE purchase_order_items (
    po_item_id BIGINT IDENTITY(1,1) NOT NULL,
    purchase_order_id BIGINT NOT NULL,
    product_id BIGINT NOT NULL,
    ordered_qty DECIMAL(12,3) NOT NULL,
    unit_cost DECIMAL(15,2) NOT NULL,
    line_amount DECIMAL(15,2) NOT NULL DEFAULT 0,

    CONSTRAINT pk_purchase_order_items PRIMARY KEY (po_item_id),
    CONSTRAINT chk_po_item_quantity CHECK (ordered_qty > 0),

    CONSTRAINT fk_po_item_order
        FOREIGN KEY (purchase_order_id)
        REFERENCES purchase_orders(purchase_order_id)
        ON UPDATE CASCADE
        ON DELETE CASCADE
);
GO

CREATE INDEX idx_po_item_order ON purchase_order_items (purchase_order_id);
CREATE INDEX idx_po_item_product ON purchase_order_items (product_id);
GO


-- 18. GOODS_RECEIPTS
CREATE TABLE goods_receipts (
    receipt_id BIGINT IDENTITY(1,1) NOT NULL,
    purchase_order_id BIGINT NOT NULL,
    warehouse_id BIGINT NOT NULL,
    receipt_number NVARCHAR(50) NOT NULL UNIQUE,
    receipt_datetime DATETIME2 NOT NULL DEFAULT SYSDATETIME(),
    status NVARCHAR(15) NOT NULL DEFAULT 'PENDING',

    CONSTRAINT pk_goods_receipts PRIMARY KEY (receipt_id),
    CONSTRAINT chk_receipt_status CHECK (status IN ('PENDING', 'INSPECTING', 'COMPLETED', 'REJECTED')),

    CONSTRAINT fk_receipt_po
        FOREIGN KEY (purchase_order_id)
        REFERENCES purchase_orders(purchase_order_id)
        ON UPDATE CASCADE
        ON DELETE NO ACTION,

    CONSTRAINT fk_receipt_warehouse
        FOREIGN KEY (warehouse_id)
        REFERENCES warehouses(warehouse_id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);
GO

CREATE INDEX idx_receipt_po ON goods_receipts (purchase_order_id);
CREATE INDEX idx_receipt_warehouse ON goods_receipts (warehouse_id);
CREATE INDEX idx_receipt_datetime ON goods_receipts (receipt_datetime);
GO


-- 19. GOODS_RECEIPT_ITEMS
CREATE TABLE goods_receipt_items (
    receipt_item_id BIGINT IDENTITY(1,1) NOT NULL,
    receipt_id BIGINT NOT NULL,
    po_item_id BIGINT NOT NULL,
    product_id BIGINT NOT NULL,
    batch_no NVARCHAR(100) NOT NULL,
    expiry_date DATE NULL,
    received_qty DECIMAL(12,3) NOT NULL,
    accepted_qty DECIMAL(12,3) NOT NULL DEFAULT 0,
    rejected_qty DECIMAL(12,3) NOT NULL DEFAULT 0,
    unit_cost DECIMAL(15,2) NOT NULL,

    CONSTRAINT pk_goods_receipt_items PRIMARY KEY (receipt_item_id),
    CONSTRAINT chk_receipt_quantity
        CHECK (received_qty >= 0 AND accepted_qty >= 0 AND rejected_qty >= 0),

    CONSTRAINT fk_receipt_item_receipt
        FOREIGN KEY (receipt_id)
        REFERENCES goods_receipts(receipt_id)
        ON UPDATE CASCADE
        ON DELETE CASCADE,

    CONSTRAINT fk_receipt_item_po_item
        FOREIGN KEY (po_item_id)
        REFERENCES purchase_order_items(po_item_id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);
GO

CREATE INDEX idx_receipt_item_receipt ON goods_receipt_items (receipt_id);
CREATE INDEX idx_receipt_item_po_item ON goods_receipt_items (po_item_id);
CREATE INDEX idx_receipt_item_product ON goods_receipt_items (product_id);
CREATE INDEX idx_receipt_item_batch ON goods_receipt_items (batch_no);
CREATE INDEX idx_receipt_item_expiry ON goods_receipt_items (expiry_date);
GO


-- 20. INVENTORY_BALANCE (tồn kho theo từng KHO - phạm vi hiện tại chỉ theo
--     dõi tồn kho ở kho tổng, không theo dõi tồn tại cửa hàng)
CREATE TABLE inventory_balance (
    warehouse_id BIGINT NOT NULL,
    product_id BIGINT NOT NULL,
    batch_no NVARCHAR(100) NOT NULL,
    expiry_date DATE NULL,
    on_hand_qty DECIMAL(12,3) NOT NULL DEFAULT 0,
    reserved_qty DECIMAL(12,3) NOT NULL DEFAULT 0,
    available_qty AS (on_hand_qty - reserved_qty) PERSISTED,
    reorder_point DECIMAL(12,3) NOT NULL DEFAULT 0,
    unit_cost DECIMAL(15,2) NOT NULL DEFAULT 0,
    last_updated DATETIME2 NOT NULL DEFAULT SYSDATETIME(),

    CONSTRAINT pk_inventory_balance PRIMARY KEY (warehouse_id, product_id, batch_no),
    CONSTRAINT chk_inventory_qty CHECK (on_hand_qty >= 0 AND reserved_qty >= 0),

    CONSTRAINT fk_inventory_warehouse
        FOREIGN KEY (warehouse_id)
        REFERENCES warehouses(warehouse_id)
        ON UPDATE CASCADE
        ON DELETE NO ACTION
);
GO

CREATE INDEX idx_inventory_warehouse ON inventory_balance (warehouse_id);
CREATE INDEX idx_inventory_product ON inventory_balance (product_id);
CREATE INDEX idx_inventory_expiry ON inventory_balance (expiry_date);
CREATE INDEX idx_inventory_reorder ON inventory_balance (reorder_point);
GO

CREATE TRIGGER trg_inventory_balance_set_last_updated
ON inventory_balance
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE ib
    SET last_updated = SYSDATETIME()
    FROM inventory_balance ib
    INNER JOIN inserted i
        ON ib.warehouse_id = i.warehouse_id
        AND ib.product_id = i.product_id
        AND ib.batch_no = i.batch_no;
END
GO


-- 21. STOCK_MOVEMENTS (log mọi biến động tồn kho tại kho. Điều chuyển
--     giữa 2 kho được ghi bằng 1 cặp dòng TRANSFER_OUT (kho nguồn) +
--     TRANSFER_IN (kho đích) có cùng reference_type = 'TRANSFER' và cùng
--     reference_id để nối cặp; không dùng bảng chứng từ riêng.
--     reference_type khác: 'PURCHASE_RECEIPT', 'INVENTORY_COUNT'...)
CREATE TABLE stock_movements (
    movement_id BIGINT IDENTITY(1,1) NOT NULL,
    warehouse_id BIGINT NOT NULL,
    product_id BIGINT NOT NULL,
    batch_no NVARCHAR(100) NOT NULL,
    movement_type NVARCHAR(15) NOT NULL,
    quantity DECIMAL(12,3) NOT NULL,
    reference_type NVARCHAR(50) NULL,
    reference_id BIGINT NULL,
    movement_datetime DATETIME2 NOT NULL DEFAULT SYSDATETIME(),
    note NVARCHAR(255) NULL,

    CONSTRAINT pk_stock_movements PRIMARY KEY (movement_id),
    CONSTRAINT chk_movement_type
        CHECK (movement_type IN (
            'RECEIPT', 'TRANSFER_IN', 'TRANSFER_OUT',
            'ADJUSTMENT', 'RETURN', 'EXPIRED', 'DAMAGE'
        )),
    CONSTRAINT chk_movement_quantity CHECK (quantity > 0),

    CONSTRAINT fk_movement_warehouse
        FOREIGN KEY (warehouse_id)
        REFERENCES warehouses(warehouse_id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);
GO

CREATE INDEX idx_movement_warehouse ON stock_movements (warehouse_id);
CREATE INDEX idx_movement_product ON stock_movements (product_id);
CREATE INDEX idx_movement_batch ON stock_movements (batch_no);
CREATE INDEX idx_movement_type ON stock_movements (movement_type);
CREATE INDEX idx_movement_datetime ON stock_movements (movement_datetime);
CREATE INDEX idx_movement_reference ON stock_movements (reference_type, reference_id);
GO


-- 22. INVENTORY_COUNTS
CREATE TABLE inventory_counts (
    count_id BIGINT IDENTITY(1,1) NOT NULL,
    warehouse_id BIGINT NOT NULL,
    count_code NVARCHAR(50) NOT NULL UNIQUE,
    count_date DATETIME2 NOT NULL DEFAULT SYSDATETIME(),
    status NVARCHAR(15) NOT NULL DEFAULT 'DRAFT',

    CONSTRAINT pk_inventory_counts PRIMARY KEY (count_id),
    CONSTRAINT chk_count_status
        CHECK (status IN ('DRAFT', 'IN_PROGRESS', 'COMPLETED', 'CANCELLED')),

    CONSTRAINT fk_count_warehouse
        FOREIGN KEY (warehouse_id)
        REFERENCES warehouses(warehouse_id)
        ON UPDATE CASCADE
        ON DELETE NO ACTION
);
GO

CREATE INDEX idx_count_warehouse ON inventory_counts (warehouse_id);
CREATE INDEX idx_count_date ON inventory_counts (count_date);
CREATE INDEX idx_count_status ON inventory_counts (status);
GO


-- 23. INVENTORY_COUNT_ITEMS
CREATE TABLE inventory_count_items (
    count_item_id BIGINT IDENTITY(1,1) NOT NULL,
    count_id BIGINT NOT NULL,
    product_id BIGINT NOT NULL,
    batch_no NVARCHAR(100) NOT NULL,
    system_qty DECIMAL(12,3) NOT NULL DEFAULT 0,
    actual_qty DECIMAL(12,3) NOT NULL DEFAULT 0,
    variance_qty AS (actual_qty - system_qty) PERSISTED,

    CONSTRAINT pk_inventory_count_items PRIMARY KEY (count_item_id),

    CONSTRAINT fk_count_item_count
        FOREIGN KEY (count_id)
        REFERENCES inventory_counts(count_id)
        ON UPDATE CASCADE
        ON DELETE CASCADE
);
GO

CREATE INDEX idx_count_item_count ON inventory_count_items (count_id);
CREATE INDEX idx_count_item_product ON inventory_count_items (product_id);
CREATE INDEX idx_count_item_batch ON inventory_count_items (batch_no);
GO
