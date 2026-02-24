import streamlit as st
import pandas as pd
import os
from sqlalchemy import create_engine

# -------------------------------
# Page Config
# -------------------------------
st.set_page_config(page_title="E-Commerce Analytics", layout="wide")

# -------------------------------
# Database Connection
# -------------------------------
engine = create_engine(
    f"mysql+pymysql://root:{os.getenv('DB_PASSWORD')}@mysql-etl/ecommerce_pipeline"
)


@st.cache_data
def fetch_data(query):
    return pd.read_sql(query, engine)

st.title("📊 E-Commerce Sales Dashboard")

# -------------------------------
# Sidebar Filters
# -------------------------------
st.sidebar.header("Filters")

date_range_query = """
SELECT MIN(order_date) AS min_date,
       MAX(order_date) AS max_date
FROM sales_data;
"""

date_range = fetch_data(date_range_query)

min_date = date_range["min_date"][0]
max_date = date_range["max_date"][0]

selected_dates = st.sidebar.date_input(
    "Select Date Range",
    [min_date, max_date]
)

start_date = selected_dates[0]
end_date = selected_dates[1]

# -------------------------------
# KPI QUERIES
# -------------------------------

total_revenue_query = f"""
SELECT IFNULL(SUM(amount),0) AS total_revenue
FROM sales_data
WHERE order_date BETWEEN '{start_date}' AND '{end_date}';
"""

total_orders_query = f"""
SELECT COUNT(DISTINCT order_id) AS total_orders
FROM sales_data
WHERE order_date BETWEEN '{start_date}' AND '{end_date}';
"""

b2b_query = f"""
SELECT COUNT(*) AS b2b_orders
FROM sales_data
WHERE b2b = 1
AND order_date BETWEEN '{start_date}' AND '{end_date}';
"""

cancel_query = f"""
SELECT IFNULL(
ROUND(
SUM(CASE WHEN status LIKE '%%Cancelled%%' THEN 1 ELSE 0 END)
/ COUNT(*) * 100, 2
),0) AS cancellation_rate
FROM sales_data
WHERE order_date BETWEEN '{start_date}' AND '{end_date}';
"""

aov_query = f"""
SELECT IFNULL(
ROUND(
SUM(amount) / COUNT(DISTINCT order_id), 2
),0) AS aov
FROM sales_data
WHERE order_date BETWEEN '{start_date}' AND '{end_date}';
"""

# -------------------------------
# Fetch KPI Data
# -------------------------------
total_revenue = fetch_data(total_revenue_query)["total_revenue"][0]
total_orders = fetch_data(total_orders_query)["total_orders"][0]
b2b_orders = fetch_data(b2b_query)["b2b_orders"][0]
cancellation_rate = fetch_data(cancel_query)["cancellation_rate"][0]
aov = fetch_data(aov_query)["aov"][0]

# -------------------------------
# KPI Layout
# -------------------------------
st.subheader("📌 Key Business Metrics")

col1, col2, col3 = st.columns(3)
col4, col5 = st.columns(2)

col1.metric("💰 Total Revenue", f"₹ {total_revenue:,.2f}")
col2.metric("📦 Total Orders", total_orders)
col3.metric("🏢 B2B Orders", b2b_orders)
col4.metric("❌ Cancellation Rate", f"{cancellation_rate:.2f}%")
col5.metric("🛒 Avg Order Value (AOV)", f"₹ {aov:,.2f}")

st.divider()

# -------------------------------
# Fulfilment Revenue Split
# -------------------------------
st.subheader("🚚 Fulfilment Revenue Split")

fulfil_query = f"""
SELECT fulfilment, SUM(amount) AS revenue
FROM sales_data
WHERE order_date BETWEEN '{start_date}' AND '{end_date}'
GROUP BY fulfilment;
"""

fulfil_df = fetch_data(fulfil_query)

if not fulfil_df.empty:
    st.bar_chart(fulfil_df.set_index("fulfilment"))
else:
    st.info("No fulfilment data available.")

st.divider()

# -------------------------------
# Revenue by State
# -------------------------------
st.subheader("📍 Revenue by State")

state_query = f"""
SELECT ship_state, SUM(amount) AS revenue
FROM sales_data
WHERE order_date BETWEEN '{start_date}' AND '{end_date}'
GROUP BY ship_state
ORDER BY revenue DESC
LIMIT 15;
"""

state_df = fetch_data(state_query)

if not state_df.empty:
    st.bar_chart(state_df.set_index("ship_state"))
else:
    st.info("No state data available.")

st.divider()



# -------------------------------
# Monthly Revenue Trend
# -------------------------------
st.subheader("📈 Monthly Revenue Trend")

monthly_query = f"""
SELECT DATE_FORMAT(order_date, '%%Y-%%m') AS month,
       SUM(amount) AS revenue
FROM sales_data
WHERE order_date BETWEEN '{start_date}' AND '{end_date}'
GROUP BY month
ORDER BY month;
"""

monthly_df = fetch_data(monthly_query)

if not monthly_df.empty:
    st.line_chart(monthly_df.set_index("month")["revenue"])
else:
    st.warning("No data available for selected date range.")

st.divider()

# -------------------------------
# Top Categories
# -------------------------------
st.subheader("🏷 Top 10 Categories")

category_query = f"""
SELECT category, SUM(amount) AS revenue
FROM sales_data
WHERE order_date BETWEEN '{start_date}' AND '{end_date}'
GROUP BY category
ORDER BY revenue DESC
LIMIT 10;
"""

category_df = fetch_data(category_query)

if not category_df.empty:
    st.bar_chart(category_df.set_index("category"))
else:
    st.info("No category data available.")

# -------------------------------
# Order Status Distribution (Business View)
# -------------------------------

st.subheader("📦 Order Status Distribution (Business View)")

business_status_query = f"""
SELECT business_status, COUNT(*) AS count
FROM sales_data
WHERE order_date BETWEEN '{selected_dates[0]}' AND '{selected_dates[1]}'
GROUP BY business_status
ORDER BY count DESC;
"""

business_status_df = fetch_data(business_status_query)

if not business_status_df.empty:
    st.bar_chart(business_status_df.set_index("business_status"))
else:
    st.warning("No order status data available for selected date range.")


