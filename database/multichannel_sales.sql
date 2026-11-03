-- =====================================================================
-- PHIÊN BẢN SQL SERVER (T-SQL)
-- =====================================================================
--
-- 2.1.3. Phòng ban sử dụng
-- - Khối Kinh doanh bán lẻ (Retail Operations Team): quản lý dữ liệu
--   khách hàng, đơn hàng, khuyến mãi và vận hành hệ thống bán hàng tại
--   các điểm bán WinMart/WinMart+/WiN.
-- - Ban Lãnh đạo (Leadership/Management Board): theo dõi, đánh giá
--   doanh thu theo cửa hàng và theo vùng để ra quyết định kinh doanh.
-- =====================================================================

-- 1. CUSTOMERS
CREATE TABLE customers (
    customer_id BIGINT IDENTITY(1,1) NOT NULL,
    full_name NVARCHAR(150) NOT NULL,
    gender NVARCHAR(10) NULL,
    date_of_birth DATE NULL,
    phone NVARCHAR(20) NOT NULL UNIQUE,
    email NVARCHAR(150) NULL UNIQUE,
    province NVARCHAR(100) NULL,
    registration_channel NVARCHAR(50) NULL,
    join_date DATE NOT NULL,
    member_tier NVARCHAR(10) NOT NULL DEFAULT 'STANDARD',
    loyalty_points INT NOT NULL DEFAULT 0,
    status NVARCHAR(10) NOT NULL DEFAULT 'ACTIVE',
    updated_at DATETIME2 NOT NULL DEFAULT SYSDATETIME(),

    CONSTRAINT pk_customers PRIMARY KEY (customer_id),
    CONSTRAINT chk_customer_gender CHECK (gender IN ('MALE', 'FEMALE', 'OTHER')),
    CONSTRAINT chk_customer_tier CHECK (member_tier IN ('STANDARD', 'SILVER', 'GOLD', 'PLATINUM')),
    CONSTRAINT chk_customer_status CHECK (status IN ('ACTIVE', 'INACTIVE', 'BLOCKED')),
    CONSTRAINT chk_customer_loyalty_points CHECK (loyalty_points >= 0)
);
GO

CREATE INDEX idx_customer_province ON customers (province);
CREATE INDEX idx_customer_tier ON customers (member_tier);
CREATE INDEX idx_customer_status ON customers (status);
GO


-- 2. REGIONS
CREATE TABLE regions (
    region_id BIGINT IDENTITY(1,1) NOT NULL,
    region_code NVARCHAR(30) NOT NULL UNIQUE,
    region_name NVARCHAR(100) NOT NULL,
    status NVARCHAR(10) NOT NULL DEFAULT 'ACTIVE',

    CONSTRAINT pk_regions PRIMARY KEY (region_id),
    CONSTRAINT chk_region_status CHECK (status IN ('ACTIVE', 'INACTIVE'))
);
GO

CREATE INDEX idx_region_status ON regions (status);
GO


-- 3. STORES
CREATE TABLE stores (
    store_id BIGINT IDENTITY(1,1) NOT NULL,
    region_id BIGINT NULL,
    store_code NVARCHAR(30) NOT NULL UNIQUE,
    store_name NVARCHAR(150) NOT NULL,
    store_type NVARCHAR(20) NOT NULL,
    address NVARCHAR(255) NOT NULL,
    district NVARCHAR(100) NULL,
    province NVARCHAR(100) NOT NULL,
    open_date DATE NOT NULL,
    close_date DATE NULL,
    floor_area DECIMAL(10,2) NULL,
    status NVARCHAR(10) NOT NULL DEFAULT 'ACTIVE',

    CONSTRAINT pk_stores PRIMARY KEY (store_id),
    CONSTRAINT chk_store_type CHECK (store_type IN ('WINMART', 'WINMART_PLUS', 'WIN')),
    CONSTRAINT chk_store_status CHECK (status IN ('ACTIVE', 'INACTIVE', 'CLOSED')),

    CONSTRAINT fk_store_region
        FOREIGN KEY (region_id)
        REFERENCES regions(region_id)
        ON UPDATE CASCADE
        ON DELETE SET NULL
);
GO

CREATE INDEX idx_store_region ON stores (region_id);
CREATE INDEX idx_store_province ON stores (province);
CREATE INDEX idx_store_type ON stores (store_type);
CREATE INDEX idx_store_status ON stores (status);
GO


-- 4. CATEGORIES
CREATE TABLE categories (
    category_id BIGINT IDENTITY(1,1) NOT NULL,
    parent_category_id BIGINT NULL,
    category_name NVARCHAR(150) NOT NULL,
    category_level TINYINT NOT NULL DEFAULT 1,
    status NVARCHAR(10) NOT NULL DEFAULT 'ACTIVE',

    CONSTRAINT pk_categories PRIMARY KEY (category_id),
    CONSTRAINT chk_category_status CHECK (status IN ('ACTIVE', 'INACTIVE')),

    -- Tự tham chiếu: SQL Server không cho phép CASCADE trên FK tự tham chiếu
    CONSTRAINT fk_category_parent
        FOREIGN KEY (parent_category_id)
        REFERENCES categories(category_id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);
GO

CREATE INDEX idx_category_parent ON categories (parent_category_id);
CREATE INDEX idx_category_status ON categories (status);
GO


-- 5. PRODUCTS
CREATE TABLE products (
    product_id BIGINT IDENTITY(1,1) NOT NULL,
    category_id BIGINT NOT NULL,
    sku NVARCHAR(50) NOT NULL UNIQUE,
    product_name NVARCHAR(200) NOT NULL,
    brand NVARCHAR(100) NULL,
    unit NVARCHAR(50) NOT NULL,
    list_price DECIMAL(15,2) NOT NULL DEFAULT 0,
    standard_cost DECIMAL(15,2) NOT NULL DEFAULT 0,
    vat_rate DECIMAL(5,2) NOT NULL DEFAULT 0,
    shelf_life_days INT NULL,
    is_fresh BIT NOT NULL DEFAULT 0,
    status NVARCHAR(15) NOT NULL DEFAULT 'ACTIVE',
    updated_at DATETIME2 NOT NULL DEFAULT SYSDATETIME(),

    CONSTRAINT pk_products PRIMARY KEY (product_id),
    CONSTRAINT chk_product_status CHECK (status IN ('ACTIVE', 'INACTIVE', 'DISCONTINUED')),

    CONSTRAINT fk_product_category
        FOREIGN KEY (category_id)
        REFERENCES categories(category_id)
        ON UPDATE CASCADE
        ON DELETE NO ACTION
);
GO

CREATE INDEX idx_product_category ON products (category_id);
CREATE INDEX idx_product_brand ON products (brand);
CREATE INDEX idx_product_status ON products (status);
GO


-- 6. PROMOTIONS
CREATE TABLE promotions (
    promotion_id BIGINT IDENTITY(1,1) NOT NULL,
    promotion_name NVARCHAR(200) NOT NULL,
    promotion_type NVARCHAR(15) NOT NULL,
    discount_value DECIMAL(15,2) NOT NULL DEFAULT 0,
    start_datetime DATETIME2 NOT NULL,
    end_datetime DATETIME2 NOT NULL,
    min_order_value DECIMAL(15,2) NOT NULL DEFAULT 0,
    status NVARCHAR(10) NOT NULL DEFAULT 'DRAFT',

    CONSTRAINT pk_promotions PRIMARY KEY (promotion_id),
    CONSTRAINT chk_promotion_type CHECK (promotion_type IN ('PERCENTAGE', 'FIXED_AMOUNT', 'BUY_X_GET_Y')),
    CONSTRAINT chk_promotion_status CHECK (status IN ('DRAFT', 'ACTIVE', 'INACTIVE', 'EXPIRED')),
    CONSTRAINT chk_promotion_datetime CHECK (end_datetime > start_datetime)
);
GO

CREATE INDEX idx_promotion_period ON promotions (start_datetime, end_datetime);
CREATE INDEX idx_promotion_status ON promotions (status);
GO


-- 7. PROMOTION_PRODUCTS
CREATE TABLE promotion_products (
    promotion_id BIGINT NOT NULL,
    product_id BIGINT NOT NULL,

    CONSTRAINT pk_promotion_products PRIMARY KEY (promotion_id, product_id),

    CONSTRAINT fk_promotion_product_promotion
        FOREIGN KEY (promotion_id)
        REFERENCES promotions(promotion_id)
        ON UPDATE CASCADE
        ON DELETE CASCADE,

    CONSTRAINT fk_promotion_product_product
        FOREIGN KEY (product_id)
        REFERENCES products(product_id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);
GO


-- 8. SALES_ORDERS
CREATE TABLE sales_orders (
    order_id BIGINT IDENTITY(1,1) NOT NULL,
    customer_id BIGINT NULL,
    store_id BIGINT NOT NULL,
    channel NVARCHAR(10) NOT NULL,
    order_datetime DATETIME2 NOT NULL DEFAULT SYSDATETIME(),
    order_status NVARCHAR(15) NOT NULL DEFAULT 'PENDING',
    gross_amount DECIMAL(15,2) NOT NULL DEFAULT 0,
    discount_amount DECIMAL(15,2) NOT NULL DEFAULT 0,
    tax_amount DECIMAL(15,2) NOT NULL DEFAULT 0,
    net_amount DECIMAL(15,2) NOT NULL DEFAULT 0,

    CONSTRAINT pk_sales_orders PRIMARY KEY (order_id),
    CONSTRAINT chk_order_channel CHECK (channel IN ('OFFLINE', 'ONLINE', 'APP', 'OTHER')),
    CONSTRAINT chk_order_status CHECK (order_status IN ('PENDING', 'CONFIRMED', 'PROCESSING', 'COMPLETED', 'CANCELLED')),

    CONSTRAINT fk_order_customer
        FOREIGN KEY (customer_id)
        REFERENCES customers(customer_id)
        ON UPDATE NO ACTION
        ON DELETE SET NULL,

    CONSTRAINT fk_order_store
        FOREIGN KEY (store_id)
        REFERENCES stores(store_id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);
GO

CREATE INDEX idx_order_customer ON sales_orders (customer_id);
CREATE INDEX idx_order_store ON sales_orders (store_id);
CREATE INDEX idx_order_datetime ON sales_orders (order_datetime);
CREATE INDEX idx_order_channel ON sales_orders (channel);
CREATE INDEX idx_order_status ON sales_orders (order_status);
GO


-- 9. SALES_ORDER_ITEMS
CREATE TABLE sales_order_items (
    order_item_id BIGINT IDENTITY(1,1) NOT NULL,
    order_id BIGINT NOT NULL,
    product_id BIGINT NOT NULL,
    promotion_id BIGINT NULL,
    quantity DECIMAL(12,3) NOT NULL,
    unit_price DECIMAL(15,2) NOT NULL,
    gross_amount DECIMAL(15,2) NOT NULL DEFAULT 0,
    discount_amount DECIMAL(15,2) NOT NULL DEFAULT 0,
    tax_amount DECIMAL(15,2) NOT NULL DEFAULT 0,
    cost_amount DECIMAL(15,2) NOT NULL DEFAULT 0,
    line_amount DECIMAL(15,2) NOT NULL DEFAULT 0,

    CONSTRAINT pk_sales_order_items PRIMARY KEY (order_item_id),
    CONSTRAINT chk_order_item_quantity CHECK (quantity > 0),

    CONSTRAINT fk_order_item_order
        FOREIGN KEY (order_id)
        REFERENCES sales_orders(order_id)
        ON UPDATE CASCADE
        ON DELETE CASCADE,

    CONSTRAINT fk_order_item_product
        FOREIGN KEY (product_id)
        REFERENCES products(product_id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,

    CONSTRAINT fk_order_item_promotion
        FOREIGN KEY (promotion_id)
        REFERENCES promotions(promotion_id)
        ON UPDATE NO ACTION
        ON DELETE SET NULL
);
GO

CREATE INDEX idx_order_item_order ON sales_order_items (order_id);
CREATE INDEX idx_order_item_product ON sales_order_items (product_id);
CREATE INDEX idx_order_item_promotion ON sales_order_items (promotion_id);
GO


-- 10. PAYMENTS
CREATE TABLE payments (
    payment_id BIGINT IDENTITY(1,1) NOT NULL,
    order_id BIGINT NOT NULL,
    payment_method NVARCHAR(20) NOT NULL,
    payment_provider NVARCHAR(100) NULL,
    paid_amount DECIMAL(15,2) NOT NULL,
    payment_datetime DATETIME2 NULL,
    payment_status NVARCHAR(10) NOT NULL DEFAULT 'PENDING',

    CONSTRAINT pk_payments PRIMARY KEY (payment_id),
    CONSTRAINT chk_payment_method CHECK (payment_method IN ('CASH', 'BANK_TRANSFER', 'CARD', 'E_WALLET', 'OTHER')),
    CONSTRAINT chk_payment_status CHECK (payment_status IN ('PENDING', 'PAID', 'FAILED', 'REFUNDED')),

    CONSTRAINT fk_payment_order
        FOREIGN KEY (order_id)
        REFERENCES sales_orders(order_id)
        ON UPDATE NO ACTION
        ON DELETE CASCADE
);
GO

CREATE INDEX idx_payment_order ON payments (order_id);
CREATE INDEX idx_payment_status ON payments (payment_status);
CREATE INDEX idx_payment_datetime ON payments (payment_datetime);
GO


-- 11. STORE_REVENUE (doanh thu cửa hàng theo tháng - tổng hợp từ SALES_ORDERS,
--     dùng để theo dõi/so sánh doanh thu theo cửa hàng và theo vùng qua STORES.region_id)
CREATE TABLE store_revenue (
    revenue_id BIGINT IDENTITY(1,1) NOT NULL,
    store_id BIGINT NOT NULL,
    revenue_year SMALLINT NOT NULL,
    revenue_month TINYINT NOT NULL,
    gross_revenue DECIMAL(18,2) NOT NULL DEFAULT 0,
    discount_amount DECIMAL(18,2) NOT NULL DEFAULT 0,
    net_revenue DECIMAL(18,2) NOT NULL DEFAULT 0,
    order_count INT NOT NULL DEFAULT 0,
    updated_at DATETIME2 NOT NULL DEFAULT SYSDATETIME(),

    CONSTRAINT pk_store_revenue PRIMARY KEY (revenue_id),
    CONSTRAINT uq_revenue_store_period UNIQUE (store_id, revenue_year, revenue_month),
    CONSTRAINT chk_revenue_month CHECK (revenue_month BETWEEN 1 AND 12),

    CONSTRAINT fk_revenue_store
        FOREIGN KEY (store_id)
        REFERENCES stores(store_id)
        ON UPDATE CASCADE
        ON DELETE CASCADE
);
GO

CREATE INDEX idx_revenue_store ON store_revenue (store_id);
CREATE INDEX idx_revenue_period ON store_revenue (revenue_year, revenue_month);
GO


-- 12. REGION_REVENUE (doanh thu tổng hợp theo vùng theo tháng)
CREATE TABLE region_revenue (
    region_revenue_id BIGINT IDENTITY(1,1) NOT NULL,
    region_id BIGINT NOT NULL,
    revenue_year SMALLINT NOT NULL,
    revenue_month TINYINT NOT NULL,
    gross_revenue DECIMAL(18,2) NOT NULL DEFAULT 0,
    discount_amount DECIMAL(18,2) NOT NULL DEFAULT 0,
    net_revenue DECIMAL(18,2) NOT NULL DEFAULT 0,
    order_count INT NOT NULL DEFAULT 0,
    updated_at DATETIME2 NOT NULL DEFAULT SYSDATETIME(),

    CONSTRAINT pk_region_revenue PRIMARY KEY (region_revenue_id),
    CONSTRAINT uq_region_revenue_period UNIQUE (region_id, revenue_year, revenue_month),
    CONSTRAINT chk_region_revenue_month CHECK (revenue_month BETWEEN 1 AND 12),

    CONSTRAINT fk_region_revenue_region
        FOREIGN KEY (region_id)
        REFERENCES regions(region_id)
        ON UPDATE CASCADE
        ON DELETE CASCADE
);
GO

CREATE INDEX idx_region_revenue_region ON region_revenue (region_id);
CREATE INDEX idx_region_revenue_period ON region_revenue (revenue_year, revenue_month);
GO


-- =====================================================================
-- CƠ CHẾ ĐỒNG BỘ: STORE_REVENUE / REGION_REVENUE được tính lại tự động
-- từ SALES_ORDERS mỗi khi đơn hàng được thêm/sửa/xóa (chỉ tính đơn
-- COMPLETED). Recalc tính lại toàn bộ cho đúng (store, tháng) / (vùng,
-- tháng) bị ảnh hưởng, thay vì cộng dồn, để tránh lệch số khi sửa/xóa.
-- =====================================================================

CREATE PROCEDURE sp_recalc_store_revenue
    @store_id BIGINT,
    @year SMALLINT,
    @month TINYINT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @gross DECIMAL(18,2), @discount DECIMAL(18,2), @net DECIMAL(18,2), @cnt INT;

    SELECT
        @gross = COALESCE(SUM(gross_amount), 0),
        @discount = COALESCE(SUM(discount_amount), 0),
        @net = COALESCE(SUM(net_amount), 0),
        @cnt = COUNT(*)
    FROM sales_orders
    WHERE store_id = @store_id
        AND order_status = 'COMPLETED'
        AND YEAR(order_datetime) = @year
        AND MONTH(order_datetime) = @month;

    MERGE store_revenue AS target
    USING (SELECT @store_id AS store_id, @year AS revenue_year, @month AS revenue_month) AS src
        ON target.store_id = src.store_id
        AND target.revenue_year = src.revenue_year
        AND target.revenue_month = src.revenue_month
    WHEN MATCHED THEN
        UPDATE SET
            gross_revenue = @gross,
            discount_amount = @discount,
            net_revenue = @net,
            order_count = @cnt,
            updated_at = SYSDATETIME()
    WHEN NOT MATCHED THEN
        INSERT (store_id, revenue_year, revenue_month, gross_revenue, discount_amount, net_revenue, order_count)
        VALUES (@store_id, @year, @month, @gross, @discount, @net, @cnt);
END
GO

CREATE PROCEDURE sp_recalc_region_revenue
    @region_id BIGINT,
    @year SMALLINT,
    @month TINYINT
AS
BEGIN
    SET NOCOUNT ON;

    IF @region_id IS NULL
        RETURN;

    DECLARE @gross DECIMAL(18,2), @discount DECIMAL(18,2), @net DECIMAL(18,2), @cnt INT;

    SELECT
        @gross = COALESCE(SUM(o.gross_amount), 0),
        @discount = COALESCE(SUM(o.discount_amount), 0),
        @net = COALESCE(SUM(o.net_amount), 0),
        @cnt = COUNT(*)
    FROM sales_orders o
    INNER JOIN stores s ON s.store_id = o.store_id
    WHERE s.region_id = @region_id
        AND o.order_status = 'COMPLETED'
        AND YEAR(o.order_datetime) = @year
        AND MONTH(o.order_datetime) = @month;

    MERGE region_revenue AS target
    USING (SELECT @region_id AS region_id, @year AS revenue_year, @month AS revenue_month) AS src
        ON target.region_id = src.region_id
        AND target.revenue_year = src.revenue_year
        AND target.revenue_month = src.revenue_month
    WHEN MATCHED THEN
        UPDATE SET
            gross_revenue = @gross,
            discount_amount = @discount,
            net_revenue = @net,
            order_count = @cnt,
            updated_at = SYSDATETIME()
    WHEN NOT MATCHED THEN
        INSERT (region_id, revenue_year, revenue_month, gross_revenue, discount_amount, net_revenue, order_count)
        VALUES (@region_id, @year, @month, @gross, @discount, @net, @cnt);
END
GO

-- Trigger: SQL Server hoạt động theo tập hợp dòng (inserted/deleted có thể
-- chứa nhiều dòng), nên phải duyệt qua từng tổ hợp (cửa hàng, năm, tháng)
-- riêng biệt xuất hiện trong batch insert/update/delete.

CREATE TRIGGER trg_sales_orders_after_insert
ON sales_orders
AFTER INSERT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @store_id BIGINT, @year SMALLINT, @month TINYINT, @region_id BIGINT;

    DECLARE cur CURSOR LOCAL FAST_FORWARD FOR
        SELECT DISTINCT store_id, YEAR(order_datetime), MONTH(order_datetime)
        FROM inserted;

    OPEN cur;
    FETCH NEXT FROM cur INTO @store_id, @year, @month;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @region_id = region_id FROM stores WHERE store_id = @store_id;

        EXEC sp_recalc_store_revenue @store_id, @year, @month;
        EXEC sp_recalc_region_revenue @region_id, @year, @month;

        FETCH NEXT FROM cur INTO @store_id, @year, @month;
    END

    CLOSE cur;
    DEALLOCATE cur;
END
GO

CREATE TRIGGER trg_sales_orders_after_update
ON sales_orders
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @store_id BIGINT, @year SMALLINT, @month TINYINT, @region_id BIGINT;

    -- Gộp tổ hợp (cửa hàng, năm, tháng) từ cả dữ liệu cũ (deleted) và mới
    -- (inserted), vì đơn hàng có thể đổi cửa hàng hoặc đổi ngày sang kỳ khác.
    DECLARE cur CURSOR LOCAL FAST_FORWARD FOR
        SELECT store_id, YEAR(order_datetime) AS y, MONTH(order_datetime) AS m FROM deleted
        UNION
        SELECT store_id, YEAR(order_datetime), MONTH(order_datetime) FROM inserted;

    OPEN cur;
    FETCH NEXT FROM cur INTO @store_id, @year, @month;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @region_id = region_id FROM stores WHERE store_id = @store_id;

        EXEC sp_recalc_store_revenue @store_id, @year, @month;
        EXEC sp_recalc_region_revenue @region_id, @year, @month;

        FETCH NEXT FROM cur INTO @store_id, @year, @month;
    END

    CLOSE cur;
    DEALLOCATE cur;
END
GO

CREATE TRIGGER trg_sales_orders_after_delete
ON sales_orders
AFTER DELETE
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @store_id BIGINT, @year SMALLINT, @month TINYINT, @region_id BIGINT;

    DECLARE cur CURSOR LOCAL FAST_FORWARD FOR
        SELECT DISTINCT store_id, YEAR(order_datetime), MONTH(order_datetime)
        FROM deleted;

    OPEN cur;
    FETCH NEXT FROM cur INTO @store_id, @year, @month;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @region_id = region_id FROM stores WHERE store_id = @store_id;

        EXEC sp_recalc_store_revenue @store_id, @year, @month;
        EXEC sp_recalc_region_revenue @region_id, @year, @month;

        FETCH NEXT FROM cur INTO @store_id, @year, @month;
    END

    CLOSE cur;
    DEALLOCATE cur;
END
GO


-- =====================================================================
-- Trigger cập nhật UPDATED_AT (tương đương ON UPDATE CURRENT_TIMESTAMP
-- của MySQL, SQL Server không có sẵn cơ chế này)
-- =====================================================================

CREATE TRIGGER trg_customers_set_updated_at
ON customers
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE c
    SET updated_at = SYSDATETIME()
    FROM customers c
    INNER JOIN inserted i ON c.customer_id = i.customer_id;
END
GO

CREATE TRIGGER trg_products_set_updated_at
ON products
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE p
    SET updated_at = SYSDATETIME()
    FROM products p
    INNER JOIN inserted i ON p.product_id = i.product_id;
END
GO
