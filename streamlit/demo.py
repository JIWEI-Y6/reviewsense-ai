import streamlit as st
from snowflake.snowpark.context import get_active_session

st.set_page_config(page_title="ReviewSense AI Demo", layout="wide")
st.title("📊 ReviewSense AI Demo")
st.caption("Interactive product-level review analysis powered by Snowflake and Cortex")

session = get_active_session()

# Get available products from final summary table
asin_list = session.sql("""
    SELECT ASIN
    FROM ANALYTICS.DEMO_PRODUCT_SUMMARY
    ORDER BY TOTAL_REVIEWS DESC
""").to_pandas()["ASIN"].tolist()

selected_asin = st.selectbox("Select a Product", asin_list)

# Load selected product summary
df = session.sql(f"""
    SELECT *
    FROM ANALYTICS.DEMO_PRODUCT_SUMMARY
    WHERE ASIN = '{selected_asin}'
""").to_pandas()

if df.empty:
    st.warning("No summary is available for this product yet.")
else:
    row = df.iloc[0]

    st.header(f"Product ASIN: {row['ASIN']}")

    st.subheader("📈 Overview")
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Reviews", int(row["TOTAL_REVIEWS"]))
    col2.metric("Average Rating", float(row["AVG_RATING"]))
    col3.metric("Positive %", f"{float(row['POSITIVE_PCT']):.2f}%")
    col4.metric("Negative %", f"{float(row['NEGATIVE_PCT']):.2f}%")

    st.subheader("💬 Complaint Summary")
    st.write(row["COMPLAINT_SUMMARY"])

    st.subheader("✅ Strengths")
    st.write(row["STRENGTH_SUMMARY"])

    st.subheader("🛠 Improvement Suggestions")
    st.write(row["IMPROVEMENT_RECOMMENDATIONS"])