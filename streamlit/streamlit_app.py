import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session

session = get_active_session()

st.set_page_config(
    page_title="ReviewSense AI",
    layout="wide"
)

st.title("ReviewSense AI")
st.markdown("**Customer Review Intelligence Copilot** — Powered by Snowflake Cortex")
st.divider()

mode = st.radio(
    "Select Mode",
    ["Consumer Q&A", "Business Analysis"],
    horizontal=True
)

st.divider()

def run_consumer_rag(question):
    query = f"""
    WITH retrieved_reviews AS (
        SELECT
            value:REVIEW_ID::VARCHAR         AS review_id,
            value:ASIN::VARCHAR              AS asin,
            value:RATING::NUMBER             AS rating,
            value:REVIEW_TEXT_CLEAN::VARCHAR AS review_text
        FROM (
            SELECT PARSE_JSON(
                SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
                    'REVIEWSENSE_DB.ANALYTICS.REVIEW_SEARCH',
                    '{{"query": "{question}", "columns": ["REVIEW_ID", "ASIN", "RATING", "REVIEW_TEXT_CLEAN"], "limit": 5}}'
                )
            ) AS results
        ),
        LATERAL FLATTEN(input => results:results)
    ),
    prompt_context AS (
        SELECT LISTAGG(
            '- Review (Rating: ' || rating || '/5): ' || review_text,
            '\\n'
        ) AS context
        FROM retrieved_reviews
    )
    SELECT SNOWFLAKE.CORTEX.COMPLETE(
        'llama3.1-70b',
        CONCAT(
            'You are ReviewSense AI, a helpful shopping assistant. ',
            'Answer the user question based ONLY on the following Amazon customer reviews. ',
            'Be concise, factual, and highlight both positives and negatives. ',
            'Keep your answer to 4-5 sentences maximum. ',
            'If the reviews do not contain enough information, say so honestly.\\n\\n',
            'CUSTOMER REVIEWS:\\n',
            context,
            '\\n\\nUSER QUESTION: {question}',
            '\\n\\nANSWER:'
        )
    ) AS answer
    FROM prompt_context
    """
    result = session.sql(query).collect()
    return result[0]['ANSWER'] if result else "No answer generated."


def get_complaint_breakdown(asin):
    query = f"""
    WITH complaint_breakdown AS (
        SELECT
            ROUND(COUNT(CASE WHEN LOWER(REVIEW_TEXT) LIKE '%sound%'
                OR LOWER(REVIEW_TEXT) LIKE '%speaker%'
                OR LOWER(REVIEW_TEXT) LIKE '%audio%'
                OR LOWER(REVIEW_TEXT) LIKE '%volume%'
                OR LOWER(REVIEW_TEXT) LIKE '%build%'
                OR LOWER(REVIEW_TEXT) LIKE '%broke%'
                OR LOWER(REVIEW_TEXT) LIKE '%broken%'
                OR LOWER(REVIEW_TEXT) LIKE '%quality%' THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0), 1) AS hardware_pct,
            ROUND(COUNT(CASE WHEN LOWER(REVIEW_TEXT) LIKE '%wifi%'
                OR LOWER(REVIEW_TEXT) LIKE '%connect%'
                OR LOWER(REVIEW_TEXT) LIKE '%bluetooth%'
                OR LOWER(REVIEW_TEXT) LIKE '%pair%'
                OR LOWER(REVIEW_TEXT) LIKE '%app%'
                OR LOWER(REVIEW_TEXT) LIKE '%software%'
                OR LOWER(REVIEW_TEXT) LIKE '%update%' THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0), 1) AS software_pct,
            ROUND(COUNT(CASE WHEN LOWER(REVIEW_TEXT) LIKE '%last%'
                OR LOWER(REVIEW_TEXT) LIKE '%durable%'
                OR LOWER(REVIEW_TEXT) LIKE '%stopped working%'
                OR LOWER(REVIEW_TEXT) LIKE '%died%'
                OR LOWER(REVIEW_TEXT) LIKE '%month%' THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0), 1) AS durability_pct,
            ROUND(COUNT(CASE WHEN LOWER(REVIEW_TEXT) LIKE '%price%'
                OR LOWER(REVIEW_TEXT) LIKE '%expensive%'
                OR LOWER(REVIEW_TEXT) LIKE '%worth%'
                OR LOWER(REVIEW_TEXT) LIKE '%cheap%'
                OR LOWER(REVIEW_TEXT) LIKE '%overpriced%' THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0), 1) AS value_pct,
            ROUND(COUNT(CASE WHEN LOWER(REVIEW_TEXT) LIKE '%customer service%'
                OR LOWER(REVIEW_TEXT) LIKE '%support%'
                OR LOWER(REVIEW_TEXT) LIKE '%return%'
                OR LOWER(REVIEW_TEXT) LIKE '%refund%'
                OR LOWER(REVIEW_TEXT) LIKE '%warranty%' THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0), 1) AS service_pct,
            COUNT(*) AS total_negative_reviews
        FROM REVIEWSENSE_DB.ANALYTICS.REVIEWS_FOR_GENAI
        WHERE ASIN = '{asin}'
        AND RATING <= 2
    ),
    signal_calc AS (
        SELECT
            ROUND(AVG(RATING), 2) AS avg_rating,
            ROUND(COUNT(CASE WHEN RATING <= 2 THEN 1 END) * 100.0 / COUNT(*), 1) AS negative_pct,
            COUNT(CASE WHEN RATING <= 2
                AND REVIEW_TS >= DATEADD('month', -6, CURRENT_DATE())
                THEN 1 END) AS recent_negative,
            COUNT(CASE WHEN RATING <= 2 THEN 1 END) AS total_negative
        FROM REVIEWSENSE_DB.ANALYTICS.REVIEWS_FOR_GENAI
        WHERE ASIN = '{asin}'
    )
    SELECT
        c.hardware_pct,
        c.software_pct,
        c.durability_pct,
        c.value_pct,
        c.service_pct,
        c.total_negative_reviews,
        s.avg_rating,
        s.negative_pct,
        s.recent_negative,
        s.total_negative,
        CASE
            WHEN s.avg_rating < 3.5 OR s.negative_pct > 30 THEN 'RED'
            WHEN s.avg_rating BETWEEN 3.5 AND 4.0 THEN 'YELLOW'
            WHEN s.negative_pct BETWEEN 15 AND 30 THEN 'YELLOW'
            WHEN s.recent_negative > (s.total_negative * 0.4) THEN 'YELLOW'
            ELSE 'GREEN'
        END AS business_signal
    FROM complaint_breakdown c
    CROSS JOIN signal_calc s
    """
    result = session.sql(query).collect()
    return result[0] if result else None


def run_business_rag(asin, breakdown):
    signal_color = breakdown['BUSINESS_SIGNAL']

    query = f"""
    WITH product_info AS (
        SELECT
            ASIN,
            COUNT(*) AS total_reviews,
            ROUND(AVG(RATING), 2) AS avg_rating,
            COUNT(CASE WHEN RATING <= 2 THEN 1 END) AS negative_reviews,
            COUNT(CASE WHEN RATING >= 4 THEN 1 END) AS positive_reviews,
            ROUND(COUNT(CASE WHEN RATING <= 2 THEN 1 END) * 100.0 / COUNT(*), 1) AS negative_pct,
            ROUND(COUNT(CASE WHEN RATING >= 4 THEN 1 END) * 100.0 / COUNT(*), 1) AS positive_pct,
            COUNT(CASE WHEN RATING = 3 THEN 1 END) AS neutral_reviews,
            ROUND(COUNT(CASE WHEN RATING = 3 THEN 1 END) * 100.0 / COUNT(*), 1) AS neutral_pct
        FROM REVIEWSENSE_DB.ANALYTICS.REVIEWS_FOR_GENAI
        WHERE ASIN = '{asin}'
        GROUP BY ASIN
    ),
    retrieved_reviews AS (
        SELECT
            value:REVIEW_ID::VARCHAR         AS review_id,
            value:ASIN::VARCHAR              AS asin,
            value:RATING::NUMBER             AS rating,
            value:REVIEW_TEXT_CLEAN::VARCHAR AS review_text
        FROM (
            SELECT PARSE_JSON(
                SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
                    'REVIEWSENSE_DB.ANALYTICS.REVIEW_SEARCH',
                    '{{"query": "product quality issues complaints broken failed disappointed", "columns": ["REVIEW_ID", "ASIN", "RATING", "REVIEW_TEXT_CLEAN"], "filter": {{"@eq": {{"ASIN": "{asin}"}}}}, "limit": 15}}'
                )
            ) AS results
        ),
        LATERAL FLATTEN(input => results:results)
    ),
    prompt_context AS (
        SELECT
            COUNT(*) AS review_count,
            LISTAGG(
                '- Review (Rating: ' || rating || '/5): ' || review_text,
                '\\n'
            ) AS context
        FROM retrieved_reviews
    )
    SELECT SNOWFLAKE.CORTEX.COMPLETE(
        'llama3.1-70b',
        CONCAT(
            'You are ReviewSense AI, a business intelligence analyst.\\n\\n',
            'IMPORTANT: Use ONLY these authoritative statistics. DO NOT calculate your own.\\n\\n',
            '=== AUTHORITATIVE PRODUCT STATISTICS ===\\n',
            'ASIN: ', p.ASIN, '\\n',
            'TOTAL REVIEWS: ', p.total_reviews::VARCHAR, '\\n',
            'AVERAGE RATING: ', p.avg_rating::VARCHAR, '/5\\n',
            'POSITIVE (4-5 stars): ', p.positive_pct::VARCHAR, '%\\n',
            'NEUTRAL (3 stars): ', p.neutral_pct::VARCHAR, '%\\n',
            'NEGATIVE (1-2 stars): ', p.negative_pct::VARCHAR, '%\\n',
            'BUSINESS SIGNAL: {signal_color}\\n\\n',
            '=== AUTHORITATIVE COMPLAINT BREAKDOWN ===\\n',
            'NOTE: Categories overlap as reviews can mention multiple issues.\\n',
            'Hardware/Build Quality: {breakdown["HARDWARE_PCT"]}% of negative reviews\\n',
            'Software/Connectivity: {breakdown["SOFTWARE_PCT"]}% of negative reviews\\n',
            'Durability/Longevity: {breakdown["DURABILITY_PCT"]}% of negative reviews\\n',
            'Value for Money: {breakdown["VALUE_PCT"]}% of negative reviews\\n',
            'Customer Service: {breakdown["SERVICE_PCT"]}% of negative reviews\\n',
            'Total negative reviews: {breakdown["TOTAL_NEGATIVE_REVIEWS"]}\\n',
            '=========================================\\n\\n',
            'Generate a business intelligence report. First identify product type from reviews.\\n\\n',
            'SIGNAL RULES:\\n',
            '- RED: urgent tone, immediate action required\\n',
            '- YELLOW: cautionary tone, monitor and plan fixes\\n',
            '- GREEN: positive tone but still flag known issues honestly\\n',
            '- Executive summary MUST be consistent with the signal\\n\\n',
            'IMPORTANT: Do NOT include Section 2 in your response. ',
            'Skip directly from Section 1 to Section 3.\\n\\n',
            'Your response MUST follow this EXACT markdown format:\\n\\n',
            '## ReviewSense AI — Business Intelligence Report\\n\\n',
            '**ASIN:** ', p.ASIN, ' | **Product Type:** [Identify from reviews]\\n\\n',
            '**Avg Rating:** ', p.avg_rating::VARCHAR, '/5 | **Total Reviews:** ', p.total_reviews::VARCHAR, '\\n\\n',
            '**Positive:** ', p.positive_pct::VARCHAR, '% | **Neutral:** ', p.neutral_pct::VARCHAR, '% | **Negative:** ', p.negative_pct::VARCHAR, '%\\n\\n',
            '---\\n\\n',
            '**Business Signal: {signal_color}**\\n\\n',
            '**Executive Summary**\\n\\n',
            '[2-3 sentences consistent with {signal_color} signal. Acknowledge overall health but flag top issue honestly.]\\n\\n',
            '---\\n\\n',
            '### Section 1: Root Cause Analysis \\n\\n',
            '**Primary Issue:** [One specific sentence]\\n\\n',
            '**Confidence:** [High / Medium / Low] | **Basis:** [How many of the {breakdown["TOTAL_NEGATIVE_REVIEWS"]} negative reviews show this]\\n\\n',
            '**Evidence from customers:**\\n',
            '- *"[Direct customer quote]"* — Rating: X/5\\n',
            '- *"[Direct customer quote]"* — Rating: X/5\\n',
            '- *"[Direct customer quote]"* — Rating: X/5\\n\\n',
            '**Pattern Type:** [Design Flaw / Manufacturing Defect / Durability Issue / Expectation Mismatch / Multiple Issues]\\n\\n',
            '**Failure Timeline:** [Immediately / Within 1 month / 2-3 months / 6+ months / Varies]\\n\\n',
            '**Immediate Flag:** [If RED or Design Flaw - escalation action. Otherwise - None]\\n\\n',
            '---\\n\\n',
            '### Section 3: Improvement Roadmap\\n\\n',
            '**Priority 1 — [Title]**\\n',
            '- Fix: [Specific action]\\n',
            '- Owner: [Engineering / Design / QA / Marketing / Support]\\n',
            '- Urgency: [This week / This month / This quarter]\\n',
            '- Customer evidence: *"[Direct quote]"*\\n\\n',
            '**Priority 2 — [Title]**\\n',
            '- Fix: [Specific action]\\n',
            '- Owner: [Different team from Priority 1]\\n',
            '- Urgency: [Timeline]\\n',
            '- Customer evidence: *"[Direct quote]"*\\n\\n',
            '**Priority 3 — [Title]**\\n',
            '- Fix: [Specific action]\\n',
            '- Owner: [Team]\\n',
            '- Urgency: [Timeline]\\n',
            '- Customer evidence: *"[Direct quote]"*\\n\\n',
            '---\\n\\n',
            '**Impact If Not Fixed:** [Specific risk: return rate / rating decline / churn / brand damage]\\n\\n',
            'Base ALL qualitative analysis ONLY on these sample reviews:\\n\\n',
            c.context,
            '\\n\\nREPORT:'
        )
    ) AS business_report
    FROM prompt_context c
    CROSS JOIN product_info p
    """
    result = session.sql(query).collect()
    return result[0]['BUSINESS_REPORT'] if result else "No report generated."


def render_complaint_breakdown(breakdown):
    st.markdown("### Section 2: Complaint Breakdown")
    st.markdown(f"*Based on all {breakdown['TOTAL_NEGATIVE_REVIEWS']} negative reviews — categories overlap*")

    complaint_data = {
        "Category": [
            "Hardware / Build Quality",
            "Software / Connectivity",
            "Durability / Longevity",
            "Value for Money",
            "Customer Service / Support"
        ],
        "% of Negative Reviews": [
            breakdown['HARDWARE_PCT'],
            breakdown['SOFTWARE_PCT'],
            breakdown['DURABILITY_PCT'],
            breakdown['VALUE_PCT'],
            breakdown['SERVICE_PCT']
        ]
    }

    df_complaints = pd.DataFrame(complaint_data)
    df_complaints = df_complaints.sort_values("% of Negative Reviews", ascending=False)

    st.dataframe(
        df_complaints,
        column_config={
            "Category": "Complaint Category",
            "% of Negative Reviews": st.column_config.ProgressColumn(
                "% of Negative Reviews",
                min_value=0,
                max_value=100,
                format="%.1f%%"
            )
        },
        hide_index=True,
        use_container_width=True
    )

    biggest = df_complaints.iloc[0]['Category']
    st.markdown(f"**Biggest Complaint Category:** {biggest}")
    st.markdown("**Note:** Categories overlap — a single review can mention multiple issues")


def get_top_products():
    query = """
    SELECT
        ASIN,
        COUNT(*) AS total_reviews,
        ROUND(AVG(RATING), 2) AS avg_rating,
        ROUND(COUNT(CASE WHEN RATING <= 2 THEN 1 END) * 100.0 / COUNT(*), 1) AS negative_pct
    FROM REVIEWSENSE_DB.ANALYTICS.REVIEWS_FOR_GENAI
    GROUP BY ASIN
    HAVING COUNT(*) >= 100
    ORDER BY negative_pct DESC
    LIMIT 10
    """
    return session.sql(query).to_pandas()


if mode == "Consumer Q&A":
    st.subheader("Ask a Question About Any Product")
    st.markdown("Ask anything about Amazon Electronics products based on real customer reviews.")

    question = st.text_input(
        "Your question",
        placeholder="e.g. What do people think about noise cancelling headphones?"
    )

    if st.button("Search Reviews", type="primary"):
        if question:
            with st.spinner("Searching reviews and generating answer..."):
                answer = run_consumer_rag(question)
            st.success("Answer generated!")
            st.markdown("### Answer")
            st.write(answer)
        else:
            st.warning("Please enter a question first.")

elif mode == "Business Analysis":
    st.subheader("Product Intelligence Dashboard")

    col1, col2 = st.columns([1, 1])

    with col1:
        st.markdown("### Products Needing Attention")
        st.markdown("*Top 10 products by negative review percentage*")
        with st.spinner("Loading product data..."):
            df = get_top_products()
        st.dataframe(
            df,
            column_config={
                "ASIN": "Product ASIN",
                "TOTAL_REVIEWS": "Total Reviews",
                "AVG_RATING": st.column_config.NumberColumn("Avg Rating", format="%.2f / 5"),
                "NEGATIVE_PCT": st.column_config.NumberColumn("Negative %", format="%.1f%%")
            },
            hide_index=True
        )

    with col2:
        st.markdown("### Generate Full Business Intelligence Report")
        st.markdown("Enter any ASIN to get a complete analysis — root cause, complaint breakdown, and improvement roadmap.")

        asin_input = st.text_input(
            "Enter ASIN to analyze",
            placeholder="e.g. B01G8JO5F2",
            help="Copy an ASIN from the table on the left"
        )

        generate_clicked = st.button("Generate Full Report", type="primary")

    if generate_clicked:
        if asin_input:
            st.divider()
            with st.spinner("Calculating real complaint statistics..."):
                breakdown = get_complaint_breakdown(asin_input.strip().upper())
            if breakdown:
                with st.spinner("Generating business intelligence report... This may take 15-20 seconds"):
                    report = run_business_rag(asin_input.strip().upper(), breakdown)
                st.success("Report complete!")
                # Split report at Section 3 to insert complaint breakdown in between
                if "### Section 3" in report:
                    parts = report.split("### Section 3")
                    st.markdown(parts[0])
                    render_complaint_breakdown(breakdown)
                    st.markdown("### Section 3: Improvement Roadmap")
                    st.markdown(parts[1])
                else:
                    st.markdown(report)
                    render_complaint_breakdown(breakdown)

            else:
                st.error("No data found for this ASIN. Please check and try again.")
        else:
            st.warning("Please enter an ASIN first.")

st.divider()
st.markdown(
    "<div style='text-align: center; color: grey; font-size: 12px;'>"
    "ReviewSense AI | Powered by Snowflake Cortex | 183,447 Amazon Electronics Reviews"
    "</div>",
    unsafe_allow_html=True
)