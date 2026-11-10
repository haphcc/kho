import pandas as pd
import numpy as np
from db_setup import get_dw_engine

dw_engine = get_dw_engine()

# 1. Extract
df_orders    = pd.read_sql_table("stg_sales_orders", con=dw_engine, schema="staging")
df_items     = pd.read_sql_table("stg_sales_order_items", con=dw_engine, schema="staging")
dim_store    = pd.read_sql("SELECT store_id, store_dim_id FROM dw.dim_store WHERE is_current = TRUE", dw_engine)
dim_customer = pd.read_sql("SELECT customer_id, member_level FROM dw.dim_customer WHERE is_current = TRUE", dw_engine)

# 2. Transform
df_order_profit = (
    df_items.assign(item_profit=df_items["line_amount"] - df_items["cost_amount"])
    .groupby("order_id", as_index=False)
    .agg(order_profit=("item_profit", "sum"))
)

df_orders_cleaned = df_orders.merge(df_order_profit, on="order_id", how="left")
df_orders_cleaned["order_ts"]     = pd.to_datetime(df_orders_cleaned["order_datetime"])
df_orders_cleaned["date_dim_id"]  = df_orders_cleaned["order_ts"].dt.strftime("%Y%m%d").astype(int)
df_orders_cleaned["is_cancelled"] = np.where(df_orders_cleaned["order_status"].isin(["CANCELLED", "RETURNED"]), 1, 0)

# 3. Join
df_orders_joined = (
    df_orders_cleaned
    .merge(dim_store, on="store_id", how="left")
    .merge(dim_customer, on="customer_id", how="left")
)
df_orders_joined["store_dim_id"]  = df_orders_joined["store_dim_id"].fillna(-1).astype(int)
df_orders_joined["is_win_member"] = np.where(
    df_orders_joined["member_level"].notna() & (df_orders_joined["member_level"] != "STANDARD"), 1, 0
)

# 4. Aggregate
df_orders_valid = df_orders_joined.assign(
    valid_net_amount=np.where(df_orders_joined["is_cancelled"] == 0, df_orders_joined["net_amount"], 0.0),
    valid_discount=np.where(df_orders_joined["is_cancelled"] == 0, df_orders_joined["discount_amount"], 0.0),
    valid_profit=np.where(df_orders_joined["is_cancelled"] == 0, df_orders_joined["order_profit"].fillna(0), 0.0),
)

fact_retail_daily_final = (
    df_orders_valid
    .groupby(["date_dim_id", "store_dim_id"], as_index=False)
    .agg(
        total_orders_daily      = ("order_id", "nunique"),
        total_revenue_daily     = ("valid_net_amount", "sum"),
        total_discount_daily    = ("valid_discount", "sum"),
        total_profit_daily      = ("valid_profit", "sum"),
        total_customers_daily   = ("customer_id", "nunique"),
        win_member_orders_daily = ("is_win_member", "sum"),
        cancelled_orders_daily  = ("is_cancelled", "sum")
    )
)

valid_orders = np.maximum(
    fact_retail_daily_final["total_orders_daily"] - fact_retail_daily_final["cancelled_orders_daily"], 1
)
fact_retail_daily_final["avg_order_value_daily"] = (
    fact_retail_daily_final["total_revenue_daily"] / valid_orders
).round(2)
fact_retail_daily_final["total_revenue_daily"]  = fact_retail_daily_final["total_revenue_daily"].round(2)
fact_retail_daily_final["total_discount_daily"] = fact_retail_daily_final["total_discount_daily"].round(2)
fact_retail_daily_final["total_profit_daily"]   = fact_retail_daily_final["total_profit_daily"].round(2)

# 5. Load
fact_retail_daily_final.to_sql(
    name="fact_retail_daily",
    con=dw_engine,
    schema="dw",
    if_exists="replace",
    index=False,
    chunksize=1000,
    method="multi"
)

if __name__ == "__main__":
    df_check = pd.read_sql("SELECT * FROM dw.fact_retail_daily LIMIT 15", dw_engine)
    print(f"Đã đổ thành công {len(fact_retail_daily_final)} dòng vào dw.fact_retail_daily:")
    print(df_check.to_string(index=False))
