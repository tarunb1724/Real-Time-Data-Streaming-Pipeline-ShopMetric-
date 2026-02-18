import streamlit as st
import snowflake.connector
import pandas as pd
import plotly.express as px
import time

# --- ❄️ CONFIGURATION (FILL THESE IN) ---
SNOWFLAKE_USER = "YOUR_USERNAME"
SNOWFLAKE_PASSWORD = "YOUR_PASSWORD"
SNOWFLAKE_ACCOUNT = "YOUR_ACCOUNT_URL"  # e.g. bac71951.us-east-1
SNOWFLAKE_DATABASE = "SHOPMETRIC_DB"
SNOWFLAKE_SCHEMA = "PUBLIC"
SNOWFLAKE_WAREHOUSE = "SHOP_WH"

# --- 1. CONNECT TO SNOWFLAKE ---
def get_data():
    conn = snowflake.connector.connect(
        user="TARUN",
        password="TarunSnowflake123",
        account="VFNFKPJ-BAC71951",
        warehouse="SHOP_WH",
        database="SHOPMETRIC_DB",
        schema="PUBLIC"
    )
    
    # Query: Get the last 100 orders
    query = """
    SELECT * FROM ORDERS 
    ORDER BY TIMESTAMP DESC 
    LIMIT 100
    """
    
    # Load into Pandas DataFrame
    cur = conn.cursor()
    cur.execute(query)
    df = cur.fetch_pandas_all()
    conn.close()
    return df

# --- 2. STREAMLIT LAYOUT ---
st.set_page_config(page_title="ShopMetric Live Dashboard", layout="wide")
st.title("🍕 ShopMetric: Real-Time Sales Command Center")

# Create placeholders for live updates
kpi1, kpi2, kpi3 = st.columns(3)
chart_placeholder = st.empty()
data_placeholder = st.empty()

# --- 3. THE REAL-TIME LOOP ---
while True:
    # A. Fetch fresh data
    df = get_data()
    
    # B. Calculate Metrics
    total_sales = df['AMOUNT'].sum()
    total_orders = df.shape[0]
    avg_order = df['AMOUNT'].mean()

    # C. Update KPIs
    with kpi1:
        st.metric(label="💰 Total Revenue (Last 100)", value=f"${total_sales:,.2f}")
    with kpi2:
        st.metric(label="📦 Total Orders", value=total_orders)
    with kpi3:
        st.metric(label="🏷️ Avg Order Value", value=f"${avg_order:,.2f}")

    # D. Draw Chart (Sales Over Time)
    fig = px.bar(df, x='TIMESTAMP', y='AMOUNT', color='PRODUCT', title="Live Sales Stream")
    chart_placeholder.plotly_chart(fig, use_container_width=True)

    # E. Show Raw Data Table
    data_placeholder.dataframe(df)

    # F. Sleep for 2 seconds before refreshing
    time.sleep(2)
    st.rerun()

    