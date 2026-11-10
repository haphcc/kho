import pandas as pd
import numpy as np
from db_setup import get_dw_engine

dw_engine = get_dw_engine()

# 1. Extract
df_orders     = pd.read_sql_table("stg_sales_orders", con=dw_engine, schema="staging")
df_items      = pd.read_sql_table("stg_sales_order_items", con=dw_engine, schema="staging")
dim_store     = pd.read_sql("SELECT store_id, store_dim_id FROM dw.dim_store WHERE is_current = TRUE", dw_engine)
dim_product   = pd.read_sql("SELECT product_id, product_dim_id FROM dw.dim_product WHERE is_current = TRUE", dw_engine)
dim_customer  = pd.read_sql("SELECT customer_id, customer_dim_id, member_level FROM dw.dim_customer WHERE is_current = TRUE", dw_engine)
dim_promotion = pd.read_sql("SELECT promotion_id, promotion_dim_id FROM dw.dim_promotion", dw_engine)

# 2. Transform
df_sales = df_items.merge(
    df_orders[["order_id", "customer_id", "store_id", "channel", "order_datetime", "order_status"]],
    on="order_id",
    how="inner"
)
df_sales["order_ts"] = pd.to_datetime(df_sales["order_datetime"])

# 3. Join
df_sales_joined = (
    df_sales
    .merge(dim_store, on="store_id", how="left")
    .merge(dim_product, on="product_id", how="left")
    .merge(dim_customer, on="customer_id", how="left")
    .merge(dim_promotion, on="promotion_id", how="left")
)

# 4. Build Fact
fact_sales_transaction_final = pd.DataFrame({
    "date_dim_id":       df_sales_joined["order_ts"].dt.strftime("%Y%m%d").astype(int),
    "time_dim_id":       df_sales_joined["order_ts"].dt.strftime("%H%M").astype(int),
    "store_dim_id":      df_sales_joined["store_dim_id"].fillna(-1).astype(int),
    "customer_dim_id":   df_sales_joined["customer_dim_id"].fillna(-1).astype(int),
    "product_dim_id":    df_sales_joined["product_dim_id"].fillna(-1).astype(int),
    "promotion_dim_id":  df_sales_joined["promotion_dim_id"].fillna(-1).astype(int),
    "sales_order_id":    df_sales_joined["order_id"].astype(str),
    "channel":           df_sales_joined["channel"].astype(str),
    "quantity":          df_sales_joined["quantity"].astype(float).round(2),
    "unit_price":        df_sales_joined["unit_price"].astype(float).round(2),
    "discount_amount":   df_sales_joined["discount_amount"].fillna(0).astype(float).round(2),
    "line_total_amount": df_sales_joined["line_amount"].astype(float).round(2),
    "line_cost_amount":  df_sales_joined["cost_amount"].astype(float).round(2),
    "line_gross_profit": (df_sales_joined["line_amount"] - df_sales_joined["cost_amount"]).astype(float).round(2),
    "is_cancelled":      np.where(df_sales_joined["order_status"].isin(["CANCELLED", "RETURNED"]), 1, 0),
    "is_win_member":     np.where(df_sales_joined["member_level"].notna() & (df_sales_joined["member_level"] != "STANDARD"), 1, 0)
})

# 5. Load
fact_sales_transaction_final.to_sql(
    name="fact_sales_transaction",
    con=dw_engine,
    schema="dw",
    if_exists="replace",
    index=False,
    chunksize=1000,
    method="multi"
)

if __name__ == "__main__":
    df_check = pd.read_sql("SELECT * FROM dw.fact_sales_transaction LIMIT 15", dw_engine)
    print(f"Đã đổ thành công {len(fact_sales_transaction_final)} dòng vào dw.fact_sales_transaction:")
    print(df_check.to_string(index=False))
