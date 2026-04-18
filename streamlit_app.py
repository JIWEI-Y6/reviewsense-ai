"""ReviewSense AI — Streamlit Frontend.

Calls the FastAPI backend for all data. Start the API first:
    python -m uvicorn api.main:app --reload
Then run:
    streamlit run streamlit_app.py
"""

import streamlit as st
import requests
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from datetime import datetime

API_BASE = "http://localhost:8000"

# ============================================
# PAGE CONFIG & THEME
# ============================================
st.set_page_config(
    page_title="ReviewSense AI",
    page_icon="",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS for beautiful dark theme with accent colors
st.markdown("""
<style>
    /* Main background */
    .stApp {
        background: linear-gradient(180deg, #0f0f23 0%, #1a1a2e 100%);
    }

    /* Sidebar */
    section[data-testid="stSidebar"] {
        background: linear-gradient(180deg, #16213e 0%, #0f3460 100%);
    }
    section[data-testid="stSidebar"] .stMarkdown p,
    section[data-testid="stSidebar"] .stMarkdown li {
        color: #e0e0e0;
    }

    /* Cards */
    div[data-testid="stMetric"] {
        background: linear-gradient(135deg, #1a1a3e 0%, #16213e 100%);
        border: 1px solid #533483;
        border-radius: 12px;
        padding: 16px;
        box-shadow: 0 4px 15px rgba(83, 52, 131, 0.2);
    }
    div[data-testid="stMetric"] label {
        color: #a78bfa !important;
    }
    div[data-testid="stMetric"] div[data-testid="stMetricValue"] {
        color: #e0e7ff !important;
    }

    /* Headers */
    h1, h2, h3 {
        color: #c4b5fd !important;
    }

    /* Alert severity badges */
    .severity-high {
        background: linear-gradient(135deg, #dc2626 0%, #991b1b 100%);
        color: white;
        padding: 4px 12px;
        border-radius: 20px;
        font-weight: 600;
        font-size: 12px;
    }
    .severity-medium {
        background: linear-gradient(135deg, #d97706 0%, #92400e 100%);
        color: white;
        padding: 4px 12px;
        border-radius: 20px;
        font-weight: 600;
        font-size: 12px;
    }
    .severity-low {
        background: linear-gradient(135deg, #059669 0%, #065f46 100%);
        color: white;
        padding: 4px 12px;
        border-radius: 20px;
        font-weight: 600;
        font-size: 12px;
    }

    /* Signal badges */
    .signal-red { color: #ef4444; font-weight: 700; font-size: 18px; }
    .signal-yellow { color: #f59e0b; font-weight: 700; font-size: 18px; }
    .signal-green { color: #10b981; font-weight: 700; font-size: 18px; }

    /* Chat messages */
    .chat-user {
        background: linear-gradient(135deg, #312e81 0%, #4c1d95 100%);
        border-radius: 16px 16px 4px 16px;
        padding: 12px 16px;
        margin: 8px 0;
        color: #e0e7ff;
    }
    .chat-assistant {
        background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%);
        border: 1px solid #334155;
        border-radius: 16px 16px 16px 4px;
        padding: 12px 16px;
        margin: 8px 0;
        color: #e2e8f0;
    }

    /* Tabs */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
    }
    .stTabs [data-baseweb="tab"] {
        border-radius: 8px;
        padding: 8px 16px;
    }

    /* Expanders */
    details {
        border: 1px solid #334155 !important;
        border-radius: 8px !important;
    }
</style>
""", unsafe_allow_html=True)

# ============================================
# PLOTLY THEME
# ============================================
PLOTLY_COLORS = {
    "primary": "#8b5cf6",
    "secondary": "#06b6d4",
    "accent": "#f43f5e",
    "success": "#10b981",
    "warning": "#f59e0b",
    "danger": "#ef4444",
    "bg": "#0f0f23",
    "card_bg": "#1a1a3e",
    "text": "#e0e7ff",
    "grid": "#1e293b",
}

PLOTLY_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(color=PLOTLY_COLORS["text"], family="Inter, sans-serif"),
    xaxis=dict(gridcolor=PLOTLY_COLORS["grid"], zerolinecolor=PLOTLY_COLORS["grid"]),
    yaxis=dict(gridcolor=PLOTLY_COLORS["grid"], zerolinecolor=PLOTLY_COLORS["grid"]),
    margin=dict(l=40, r=40, t=40, b=40),
)

CATEGORY_COLORS = px.colors.qualitative.Plotly


# ============================================
# API HELPERS
# ============================================
@st.cache_data(ttl=300)
def api_get(endpoint):
    try:
        r = requests.get(f"{API_BASE}{endpoint}", timeout=30)
        return r.json() if r.status_code == 200 else None
    except Exception:
        return None


def api_post(endpoint, data):
    try:
        r = requests.post(f"{API_BASE}{endpoint}", json=data, timeout=120)
        return r.json() if r.status_code == 200 else {"error": r.text[:300]}
    except Exception as e:
        return {"error": str(e)[:300]}


def severity_badge(severity):
    cls = f"severity-{severity.lower()}"
    return f'<span class="{cls}">{severity}</span>'


def business_signal(avg_rating, negative_rate):
    if avg_rating < 3.5 or negative_rate > 0.30:
        return '<span class="signal-red">RED</span>', "red"
    elif avg_rating < 4.0 or negative_rate > 0.15:
        return '<span class="signal-yellow">YELLOW</span>', "yellow"
    return '<span class="signal-green">GREEN</span>', "green"


# ============================================
# SIDEBAR
# ============================================
with st.sidebar:
    st.markdown("## ReviewSense AI")
    st.markdown("*Product Intelligence Platform*")
    st.divider()

    page = st.radio(
        "Navigate",
        ["Intelligence Chat", "Category Explorer", "Product Analysis",
         "Business Intelligence", "Monitoring & Alerts"],
        label_visibility="collapsed",
    )

    st.divider()
    st.markdown("##### System Status")
    health = api_get("/health")
    if health:
        col1, col2 = st.columns(2)
        col1.markdown(f"Snowflake: {'connected' if health.get('snowflake_connected') else 'disconnected'}")
        col2.markdown(f"Search: {'active' if health.get('search_available') else 'down'}")
    else:
        st.error("API offline — start with `uvicorn api.main:app`")

    st.divider()
    st.caption("183K+ Amazon Electronics Reviews")
    st.caption("Powered by Snowflake Cortex AI")


# ============================================
# PAGE: INTELLIGENCE CHAT
# ============================================
# ============================================
# SHARED UI COMPONENTS
# ============================================
def render_rating_stars(rating):
    try:
        r = int(float(rating))
    except (ValueError, TypeError):
        return ""
    return "★" * r + "☆" * (5 - r)


def render_review_card(source, idx):
    rating = source.get("rating", "?")
    text = source.get("text", "")[:250]
    asin = source.get("asin", "")
    try:
        r = int(float(rating))
        color = "#10b981" if r >= 4 else "#f59e0b" if r == 3 else "#ef4444"
    except (ValueError, TypeError):
        color = "#64748b"

    st.markdown(f"""
    <div style="
        background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%);
        border-left: 4px solid {color};
        border-radius: 8px;
        padding: 12px 16px;
        margin: 6px 0;
    ">
        <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 6px;">
            <span style="color: {color}; font-size: 16px;">{render_rating_stars(rating)}</span>
            <span style="color: #64748b; font-size: 12px;">{asin}</span>
        </div>
        <p style="color: #cbd5e1; font-size: 14px; line-height: 1.5; margin: 0;">{text}</p>
    </div>
    """, unsafe_allow_html=True)


# ============================================
# PAGE: INTELLIGENCE CHAT
# ============================================
if page == "Intelligence Chat":
    st.markdown("## Intelligence Chat")

    if "chat_history" not in st.session_state:
        st.session_state.chat_history = []
    if "session_context" not in st.session_state:
        st.session_state.session_context = {
            "products_discussed": [],
            "categories_discussed": [],
            "brands_discussed": [],
        }

    def _extract_context_from_response(result):
        """Extract products, categories, brands mentioned in the response."""
        import re
        answer = result.get("answer", "")
        data = result.get("data") or []

        # Extract ASINs from answer or data
        asins = re.findall(r'\bB0[A-Z0-9]{8,}\b', answer)
        for row in data:
            for v in row.values():
                if isinstance(v, str) and re.match(r'^B0[A-Z0-9]{8,}$', v):
                    asins.append(v)

        # Extract categories from data
        categories = []
        for row in data:
            cat = row.get("DERIVED_CATEGORY") or row.get("derived_category")
            if cat and isinstance(cat, str):
                categories.append(cat)

        # Extract brands from answer
        known_brands = ['amazon', 'logitech', 'sony', 'samsung', 'anker', 'bose',
                        'apple', 'senso', 'jbl', 'canon', 'panasonic', 'ring']
        brands = [b for b in known_brands if b.lower() in answer.lower()]

        return asins, categories, brands

    # Display chat history
    for msg in st.session_state.chat_history:
        if msg["role"] == "user":
            with st.chat_message("user", avatar="🧑"):
                st.markdown(msg["content"])
        else:
            with st.chat_message("assistant", avatar="🔮"):
                st.markdown(msg["content"])

                if msg.get("meta"):
                    meta = msg["meta"]

                    # Tool trace (Claude Code / Copilot style)
                    tool_trace = meta.get("tool_trace")
                    if tool_trace:
                        tool_icons = {
                            "intent_classifier": "🧠",
                            "cortex_analyst": "📊",
                            "cortex_search": "🔍",
                            "cortex_complete": "✨",
                        }
                        for step in tool_trace:
                            icon = tool_icons.get(step["tool"], "⚙️")
                            status_icon = {"done": "✅", "running": "⏳", "error": "❌"}.get(step["status"], "")
                            summary = f' — {step["result_summary"]}' if step.get("result_summary") else ""
                            st.markdown(
                                f'<div style="color: #64748b; font-size: 13px; padding: 2px 0; '
                                f'border-left: 2px solid #334155; padding-left: 10px; margin: 2px 0;">'
                                f'{icon} {step["description"]}{summary} {status_icon}</div>',
                                unsafe_allow_html=True
                            )
                        # Latency at end of trace
                        latency = meta.get("latency_ms", 0)
                        st.markdown(
                            f'<div style="color: #475569; font-size: 12px; margin-top: 4px;">'
                            f'Completed in {latency:.0f}ms</div>',
                            unsafe_allow_html=True
                        )
                    else:
                        # Fallback: just show intent + latency
                        intent = meta.get("intent", "—")
                        latency = meta.get("latency_ms", 0)
                        intent_icon = {"structured": "📊", "semantic": "🔍", "synthesis": "🔄"}.get(
                            intent.split(" ")[0] if intent else "", "🤖")
                        st.caption(f"{intent_icon} {intent}  |  {latency:.0f}ms")

                    # Data table only for structured SQL queries (not agent tool results)
                    intent = meta.get("intent", "")
                    if (meta.get("data") and isinstance(meta["data"], list) and len(meta["data"]) > 0
                            and meta.get("sql")
                            and "agent" not in intent):
                        data_df = pd.DataFrame(meta["data"])
                        st.dataframe(
                            data_df,
                            use_container_width=True,
                            hide_index=True,
                            height=min(len(data_df) * 35 + 50, 400),
                        )

                    # Source review cards for semantic queries
                    if meta.get("sources"):
                        st.markdown("**Source Reviews**")
                        for idx, s in enumerate(meta["sources"][:5]):
                            render_review_card(s, idx)

                    # SQL in collapsible
                    if meta.get("sql"):
                        with st.expander("View SQL"):
                            st.code(meta["sql"], language="sql")

    # Input
    question = st.chat_input("Ask about product reviews...")
    if question:
        st.session_state.chat_history.append({"role": "user", "content": question})

        with st.spinner("Analyzing..."):
            # Build conversation history (last 3 exchanges)
            history = []
            for msg in st.session_state.chat_history[-6:]:
                if msg["role"] in ("user", "assistant"):
                    content = msg.get("content", "")
                    # Assistant messages get more room — entity extraction needs full product details
                    max_len = 1500 if msg["role"] == "assistant" else 500
                    history.append({"role": msg["role"], "content": content[:max_len]})

            payload = {
                "question": question,
                "conversation_history": history if history else None,
                "session_context": st.session_state.session_context,
            }
            result = api_post("/query", payload)

        if "error" in result:
            answer = f"Error: {result['error']}"
            meta = {}
        else:
            answer = result.get("answer", "No answer generated.")
            meta = {
                "intent": result.get("intent"),
                "latency_ms": result.get("latency_ms"),
                "sql": result.get("sql"),
                "data": result.get("data"),
                "sources": result.get("sources"),
                "tools_used": result.get("tools_used"),
                "tool_trace": result.get("tool_trace"),
            }

            # Update session context with entities from this response
            asins, categories, brands = _extract_context_from_response(result)
            ctx = st.session_state.session_context
            ctx["products_discussed"].extend(asins)
            ctx["categories_discussed"].extend(categories)
            ctx["brands_discussed"].extend(brands)
            ctx["products_discussed"] = list(dict.fromkeys(ctx["products_discussed"]))[-10:]
            ctx["categories_discussed"] = list(dict.fromkeys(ctx["categories_discussed"]))[-10:]
            ctx["brands_discussed"] = list(dict.fromkeys(ctx["brands_discussed"]))[-10:]

        st.session_state.chat_history.append({"role": "assistant", "content": answer, "meta": meta})
        st.rerun()

    # Suggested questions
    if not st.session_state.chat_history:
        st.markdown("#### Try these questions:")
        suggestions = [
            "Which product categories have the worst reviews?",
            "What do people complain about in headphones?",
            "What do people say about battery life in wireless earbuds?",
            "How has smart home sentiment changed over time?",
        ]
        cols = st.columns(2)
        for i, s in enumerate(suggestions):
            if cols[i % 2].button(s, key=f"suggest_{i}"):
                st.session_state.chat_history.append({"role": "user", "content": s})
                st.rerun()


# ============================================
# PAGE: CATEGORY EXPLORER
# ============================================
elif page == "Category Explorer":
    st.markdown("## Category Explorer")

    categories = api_get("/categories")
    if not categories:
        st.error("Failed to load categories from API.")
        st.stop()

    df = pd.DataFrame(categories)

    # Overview metrics
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Categories", len(df))
    col2.metric("Total Reviews", f"{df['review_count'].sum():,}")
    col3.metric("Avg Rating", f"{df['avg_rating'].mean():.2f}")
    col4.metric("Avg Negative Rate", f"{df['negative_rate'].mean()*100:.1f}%")

    st.divider()

    # Charts row
    chart_col1, chart_col2 = st.columns(2)

    with chart_col1:
        fig = px.bar(
            df.sort_values("avg_rating"),
            x="avg_rating", y="derived_category",
            orientation="h",
            color="avg_rating",
            color_continuous_scale=["#ef4444", "#f59e0b", "#10b981"],
            title="Average Rating by Category",
        )
        fig.update_layout(**PLOTLY_LAYOUT, showlegend=False, coloraxis_showscale=False)
        st.plotly_chart(fig, use_container_width=True)

    with chart_col2:
        fig = px.bar(
            df.sort_values("negative_rate", ascending=False),
            x="negative_rate", y="derived_category",
            orientation="h",
            color="negative_rate",
            color_continuous_scale=["#10b981", "#f59e0b", "#ef4444"],
            title="Negative Review Rate by Category",
        )
        fig.update_layout(**PLOTLY_LAYOUT, showlegend=False, coloraxis_showscale=False)
        fig.update_traces(texttemplate="%{x:.1%}", textposition="outside")
        st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # Category drill-down
    st.markdown("### Category Detail")
    selected = st.selectbox(
        "Select a category",
        df.sort_values("review_count", ascending=False)["derived_category"].tolist()
    )

    if selected:
        detail = api_get(f"/categories/{selected}")
        if detail:
            signal_html, signal_color = business_signal(detail["avg_rating"], detail["negative_rate"])

            col1, col2, col3, col4, col5 = st.columns(5)
            col1.metric("Reviews", f"{detail['review_count']:,}")
            col2.metric("Avg Rating", f"{detail['avg_rating']:.2f}")
            col3.metric("Sentiment", f"{detail['avg_sentiment']:.3f}")
            col4.metric("Negative Rate", f"{detail['negative_rate']*100:.1f}%")
            col5.markdown(f"**Signal:** {signal_html}", unsafe_allow_html=True)

            tab1, tab2, tab3 = st.tabs(["Themes", "Complaints", "Trends"])

            with tab1:
                if detail.get("top_themes"):
                    theme_df = pd.DataFrame(detail["top_themes"])
                    fig = px.treemap(
                        theme_df, path=["theme"], values="review_count",
                        color="avg_sentiment",
                        color_continuous_scale=["#ef4444", "#f59e0b", "#10b981"],
                        title=f"Review Themes for {selected}",
                    )
                    fig.update_layout(**PLOTLY_LAYOUT)
                    st.plotly_chart(fig, use_container_width=True)

            with tab2:
                if detail.get("top_complaints"):
                    comp_df = pd.DataFrame(detail["top_complaints"])
                    fig = px.bar(
                        comp_df.sort_values("complaint_count", ascending=True),
                        x="complaint_count", y="theme",
                        orientation="h",
                        color="avg_sentiment",
                        color_continuous_scale=["#ef4444", "#f59e0b", "#10b981"],
                        title=f"Complaints for {selected}",
                    )
                    fig.update_layout(**PLOTLY_LAYOUT, coloraxis_showscale=False)
                    st.plotly_chart(fig, use_container_width=True)

            with tab3:
                if detail.get("monthly_trends"):
                    trend_df = pd.DataFrame(detail["monthly_trends"])
                    trend_df["month"] = pd.to_datetime(trend_df["month"])

                    fig = go.Figure()
                    fig.add_trace(go.Scatter(
                        x=trend_df["month"], y=trend_df["avg_sentiment"],
                        mode="lines+markers", name="Sentiment",
                        line=dict(color=PLOTLY_COLORS["primary"], width=2),
                        marker=dict(size=4),
                    ))
                    fig.add_trace(go.Scatter(
                        x=trend_df["month"], y=trend_df["negative_rate"],
                        mode="lines+markers", name="Negative Rate",
                        line=dict(color=PLOTLY_COLORS["accent"], width=2),
                        marker=dict(size=4), yaxis="y2",
                    ))
                    fig.update_layout(
                        paper_bgcolor="rgba(0,0,0,0)",
                        plot_bgcolor="rgba(0,0,0,0)",
                        font=dict(color=PLOTLY_COLORS["text"], family="Inter, sans-serif"),
                        margin=dict(l=40, r=40, t=40, b=40),
                        title=f"Monthly Trends for {selected}",
                        yaxis=dict(title="Sentiment", gridcolor=PLOTLY_COLORS["grid"],
                                   zerolinecolor=PLOTLY_COLORS["grid"]),
                        yaxis2=dict(title="Negative Rate", overlaying="y", side="right",
                                    gridcolor=PLOTLY_COLORS["grid"]),
                        legend=dict(x=0.01, y=0.99, bgcolor="rgba(0,0,0,0)"),
                    )
                    st.plotly_chart(fig, use_container_width=True)


# ============================================
# PAGE: PRODUCT ANALYSIS
# ============================================
elif page == "Product Analysis":
    st.markdown("## Product Analysis")
    st.markdown("Lookup products by ASIN (only products with 20+ reviews).")

    col1, col2 = st.columns([2, 1])
    with col1:
        asin = st.text_input("Enter ASIN", placeholder="e.g., B01G8JO5F2")
    with col2:
        st.markdown("")
        st.markdown("")
        lookup = st.button("Analyze", type="primary")

    # Quick product list
    with st.expander("Top Products by Review Count"):
        st.markdown("""
        | ASIN | Reviews | Description |
        |------|---------|-------------|
        | B01G8JO5F2 | 4,526 | Wireless Earbuds |
        | B00ZV9RDKK | 551 | Amazon Fire TV Stick |
        | B079QHML21 | 465 | Streaming Device |
        | B01DFKC2SO | 370 | Amazon Echo Dot |
        | B0791TX5P5 | 317 | Amazon Fire TV Stick |
        """)

    if asin and lookup:
        with st.spinner("Loading product data..."):
            product = api_get(f"/products/{asin}")

        if not product:
            st.error(f"Product '{asin}' not found. Only products with 20+ reviews are available.")
        else:
            signal_html, signal_color = business_signal(product["avg_rating"], product["negative_rate"])

            st.divider()
            st.markdown(f"### {product.get('derived_category', 'Unknown Category')} — {asin}")

            col1, col2, col3, col4, col5 = st.columns(5)
            col1.metric("Reviews", f"{product['review_count']:,}")
            col2.metric("Avg Rating", f"{product['avg_rating']:.2f}")
            col3.metric("Sentiment", f"{product['avg_sentiment']:.3f}")
            col4.metric("Negative Rate", f"{product['negative_rate']*100:.1f}%")
            col5.markdown(f"**Signal:** {signal_html}", unsafe_allow_html=True)

            if product.get("top_theme"):
                st.info(f"Top review theme: **{product['top_theme']}**")

            # Ask a question about this product
            st.divider()
            st.markdown("#### Ask about this product")
            product_q = st.text_input("Question", placeholder=f"What do people say about {asin}?", key="product_q")
            if st.button("Ask", key="product_ask"):
                with st.spinner("Searching reviews..."):
                    result = api_post("/query", {"question": product_q})
                if "error" not in result:
                    st.markdown(f'<div class="chat-assistant">{result.get("answer", "")}</div>', unsafe_allow_html=True)
                    if result.get("sources"):
                        with st.expander("Source Reviews"):
                            for s in result["sources"]:
                                st.markdown(f"- [{s.get('rating', '?')}/5] *{s.get('text', '')[:200]}...*")


# ============================================
# PAGE: BUSINESS INTELLIGENCE
# ============================================
elif page == "Business Intelligence":
    st.markdown("## Business Intelligence Reports")

    report_type = st.radio("Report Type", ["Category Report", "Product Report"], horizontal=True)

    if report_type == "Category Report":
        categories = api_get("/categories")
        if categories:
            cat_names = [c["derived_category"] for c in sorted(categories, key=lambda x: -x["review_count"])]
            selected_cat = st.selectbox("Select Category", cat_names)

            if st.button("Generate Report", type="primary"):
                with st.spinner("Generating business intelligence report... (10-15 seconds)"):
                    report = api_get(f"/report/category/{selected_cat}")

                if report:
                    # Signal header
                    signal = report["signal"]
                    signal_colors = {"RED": "#ef4444", "YELLOW": "#f59e0b", "GREEN": "#10b981"}
                    signal_color = signal_colors.get(signal, "#64748b")

                    st.markdown(
                        f'<div style="text-align: center; padding: 16px; '
                        f'background: linear-gradient(135deg, {signal_color}22 0%, {signal_color}11 100%); '
                        f'border: 2px solid {signal_color}; border-radius: 12px; margin-bottom: 20px;">'
                        f'<span style="font-size: 32px; color: {signal_color}; font-weight: 700;">'
                        f'{signal} SIGNAL</span><br>'
                        f'<span style="color: #cbd5e1; font-size: 16px;">{selected_cat}</span>'
                        f'</div>',
                        unsafe_allow_html=True
                    )

                    # Stats comparison
                    stats = report["stats"]
                    comp = report["overall_comparison"]
                    col1, col2, col3, col4 = st.columns(4)
                    col1.metric("Reviews", f"{stats['review_count']:,}")
                    col2.metric("Avg Rating",
                                f"{stats['avg_rating']:.2f}",
                                f"{stats['avg_rating'] - comp['avg_rating']:.2f} vs avg")
                    col3.metric("Sentiment",
                                f"{stats['avg_sentiment']:.3f}",
                                f"{stats['avg_sentiment'] - comp['avg_sentiment']:.3f} vs avg")
                    col4.metric("Negative Rate",
                                f"{stats['negative_rate']*100:.1f}%",
                                f"{(stats['negative_rate'] - comp['negative_rate'])*100:.1f}% vs avg",
                                delta_color="inverse")

                    st.divider()

                    # AI Narrative
                    st.markdown("### Analysis Report")
                    st.markdown(report["narrative"])

                    st.divider()

                    # Theme & Complaint charts side by side
                    chart_col1, chart_col2 = st.columns(2)

                    with chart_col1:
                        if report.get("themes"):
                            theme_df = pd.DataFrame(report["themes"])
                            fig = px.bar(
                                theme_df.sort_values("review_count", ascending=True),
                                x="review_count", y="theme", orientation="h",
                                color="avg_sentiment",
                                color_continuous_scale=["#ef4444", "#f59e0b", "#10b981"],
                                title="Review Themes",
                            )
                            fig.update_layout(**PLOTLY_LAYOUT, coloraxis_showscale=False)
                            st.plotly_chart(fig, use_container_width=True)

                    with chart_col2:
                        if report.get("complaints"):
                            comp_df = pd.DataFrame(report["complaints"])
                            fig = px.bar(
                                comp_df.sort_values("complaint_count", ascending=True),
                                x="complaint_count", y="theme", orientation="h",
                                color="avg_sentiment",
                                color_continuous_scale=["#ef4444", "#f59e0b", "#10b981"],
                                title="Complaint Breakdown (1-2 star only)",
                            )
                            fig.update_layout(**PLOTLY_LAYOUT, coloraxis_showscale=False)
                            st.plotly_chart(fig, use_container_width=True)

                    # Evidence quotes
                    st.divider()
                    ev_col1, ev_col2 = st.columns(2)
                    with ev_col1:
                        st.markdown("### Customer Complaints")
                        for e in report.get("evidence", {}).get("negative", []):
                            render_review_card(e, 0)
                    with ev_col2:
                        st.markdown("### Customer Praise")
                        for e in report.get("evidence", {}).get("positive", []):
                            render_review_card(e, 0)
                else:
                    st.error("Failed to generate report.")

    elif report_type == "Product Report":
        col1, col2 = st.columns([2, 1])
        with col1:
            asin = st.text_input("Enter ASIN", placeholder="e.g., B01G8JO5F2", key="bi_asin")
        with col2:
            st.markdown("")
            st.markdown("")
            generate = st.button("Generate Report", type="primary", key="bi_generate")

        if asin and generate:
            with st.spinner("Generating product intelligence report... (10-15 seconds)"):
                report = api_get(f"/report/product/{asin}")

            if report:
                signal = report["signal"]
                signal_colors = {"RED": "#ef4444", "YELLOW": "#f59e0b", "GREEN": "#10b981"}
                signal_color = signal_colors.get(signal, "#64748b")

                # Product header
                st.markdown(
                    f'<div style="text-align: center; padding: 16px; '
                    f'background: linear-gradient(135deg, {signal_color}22 0%, {signal_color}11 100%); '
                    f'border: 2px solid {signal_color}; border-radius: 12px; margin-bottom: 20px;">'
                    f'<span style="font-size: 24px; color: {signal_color}; font-weight: 700;">'
                    f'{signal} SIGNAL</span><br>'
                    f'<span style="color: #e0e7ff; font-size: 18px;">{report.get("product_name", asin)}</span><br>'
                    f'<span style="color: #94a3b8; font-size: 14px;">{report.get("category", "")} | {asin}</span>'
                    f'</div>',
                    unsafe_allow_html=True
                )

                # Stats vs category
                stats = report["stats"]
                comp = report["category_comparison"]
                col1, col2, col3, col4 = st.columns(4)
                col1.metric("Reviews", f"{stats['review_count']:,}")
                col2.metric("Avg Rating",
                            f"{stats['avg_rating']:.2f}",
                            f"{stats['avg_rating'] - comp['avg_rating']:.2f} vs category")
                col3.metric("Sentiment",
                            f"{stats['avg_sentiment']:.3f}",
                            f"{stats['avg_sentiment'] - comp['avg_sentiment']:.3f} vs category")
                col4.metric("Negative Rate",
                            f"{stats['negative_rate']*100:.1f}%",
                            f"{(stats['negative_rate'] - comp['negative_rate'])*100:.1f}% vs category",
                            delta_color="inverse")

                st.divider()

                # AI Narrative
                st.markdown("### Product Analysis Report")
                st.markdown(report["narrative"])

                st.divider()

                # Theme breakdown
                if report.get("themes"):
                    theme_df = pd.DataFrame(report["themes"])
                    fig = px.bar(
                        theme_df.sort_values("review_count", ascending=True),
                        x="review_count", y="theme", orientation="h",
                        color="avg_sentiment",
                        color_continuous_scale=["#ef4444", "#f59e0b", "#10b981"],
                        title=f"Theme Breakdown for {report.get('product_name', asin)}",
                    )
                    fig.update_layout(**PLOTLY_LAYOUT, coloraxis_showscale=False)
                    st.plotly_chart(fig, use_container_width=True)

                # Evidence
                st.divider()
                ev_col1, ev_col2 = st.columns(2)
                with ev_col1:
                    st.markdown("### Customer Complaints")
                    for e in report.get("evidence", {}).get("negative", []):
                        render_review_card(e, 0)
                with ev_col2:
                    st.markdown("### Customer Praise")
                    for e in report.get("evidence", {}).get("positive", []):
                        render_review_card(e, 0)
            else:
                st.error(f"Product '{asin}' not found. Only products with 20+ reviews are available.")


# ============================================
# PAGE: MONITORING & ALERTS
# ============================================
elif page == "Monitoring & Alerts":
    st.markdown("## Monitoring & Alerts")

    # Alert summary
    alerts_data = api_get("/alerts?limit=200")
    if not alerts_data:
        st.warning("No alerts available. Run `CALL GOLD.GENERATE_ALERTS()` in Snowflake first.")
        st.stop()

    # Summary cards
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Alerts", alerts_data["total"])
    col2.metric("HIGH", alerts_data["high_count"])
    col3.metric("MEDIUM", alerts_data["medium_count"])
    col4.metric("LOW", alerts_data["low_count"])

    st.divider()

    # Filters
    filter_col1, filter_col2, filter_col3 = st.columns(3)
    with filter_col1:
        sev_filter = st.selectbox("Severity", ["All", "HIGH", "MEDIUM", "LOW"])
    with filter_col2:
        source_filter = st.selectbox("Source", ["All", "anomaly", "cross_category", "emerging_theme", "product", "data_quality"])
    with filter_col3:
        ack_filter = st.selectbox("Status", ["All", "Open", "Acknowledged"])

    # Build filtered URL
    params = []
    if sev_filter != "All":
        params.append(f"severity={sev_filter}")
    if source_filter != "All":
        params.append(f"alert_source={source_filter}")
    if ack_filter == "Open":
        params.append("acknowledged=false")
    elif ack_filter == "Acknowledged":
        params.append("acknowledged=true")

    param_str = "&".join(params)
    filtered = api_get(f"/alerts?{param_str}&limit=100")

    if filtered and filtered.get("alerts"):
        for alert in filtered["alerts"]:
            sev = alert["severity"]
            sev_icon = {"HIGH": "🔴", "MEDIUM": "🟡", "LOW": "🟢"}.get(sev, "⚪")
            source = alert["alert_source"]
            category = alert.get("derived_category", "—")
            theme = alert.get("review_theme") or ""
            anomaly_type = alert["anomaly_type"]

            title = f"{anomaly_type}"
            if theme:
                title += f" — {theme}"
            title += f" in {category}"

            with st.expander(f"{sev_icon} [{sev}] {title}", expanded=(sev == "HIGH")):
                st.markdown(alert.get("ai_summary", "No summary available."))

                detail_cols = st.columns(4)
                if alert.get("current_value") is not None:
                    detail_cols[0].metric("Current", f"{alert['current_value']:.2f}")
                if alert.get("baseline_value") is not None:
                    detail_cols[1].metric("Baseline", f"{alert['baseline_value']:.2f}")
                if alert.get("deviation_score") is not None:
                    detail_cols[2].metric("Deviation", f"{alert['deviation_score']:.2f}")
                if alert.get("affected_reviews") is not None:
                    detail_cols[3].metric("Affected", f"{alert['affected_reviews']:,}")

                st.caption(f"Source: {source} | Created: {alert['created_at']}")
    else:
        st.info("No alerts match the current filters.")

    st.divider()

    # On-demand analysis
    st.markdown("### Run Fresh Analysis")
    if st.button("Analyze Now", type="primary"):
        with st.spinner("Running anomaly scan..."):
            analysis = api_post("/alerts/analyze", {})

        if "error" not in analysis:
            st.success(f"Found {analysis.get('anomalies_detected', 0)} anomalies")

            tab1, tab2, tab3 = st.tabs(["Cross-Category", "Emerging Themes", "Data Quality"])

            with tab1:
                patterns = analysis.get("cross_category_patterns", [])
                if patterns:
                    pat_df = pd.DataFrame(patterns)
                    fig = px.bar(
                        pat_df.sort_values("affected_categories", ascending=True),
                        x="affected_categories", y="review_theme",
                        orientation="h",
                        color="total_affected_reviews",
                        color_continuous_scale=["#8b5cf6", "#f43f5e"],
                        title="Cross-Category Complaint Patterns",
                    )
                    fig.update_layout(**PLOTLY_LAYOUT, coloraxis_showscale=False)
                    st.plotly_chart(fig, use_container_width=True)

            with tab2:
                emerging = analysis.get("emerging_themes", [])
                if emerging:
                    em_df = pd.DataFrame(emerging)
                    fig = px.scatter(
                        em_df, x="historical_share", y="recent_share",
                        size="growth_factor", color="severity",
                        color_discrete_map={"HIGH": "#ef4444", "MEDIUM": "#f59e0b", "LOW": "#10b981"},
                        hover_data=["derived_category", "review_theme"],
                        title="Emerging Themes (size = growth factor)",
                    )
                    fig.add_trace(go.Scatter(
                        x=[0, em_df["historical_share"].max()],
                        y=[0, em_df["historical_share"].max()],
                        mode="lines", name="No Change",
                        line=dict(dash="dash", color="#475569"),
                    ))
                    fig.update_layout(**PLOTLY_LAYOUT)
                    st.plotly_chart(fig, use_container_width=True)

            with tab3:
                quality = analysis.get("data_quality", [])
                if quality:
                    for check in quality:
                        status = check["status"]
                        icon = {"PASS": "", "WARN": "", "FAIL": ""}.get(status, "")
                        color = {"PASS": "green", "WARN": "orange", "FAIL": "red"}.get(status, "gray")
                        st.markdown(
                            f":{color}[{icon} **{check['check_name']}**] — "
                            f"{check['description']} (current: {check['current_value']}, expected: {check['expected_value']})"
                        )
        else:
            st.error(f"Analysis failed: {analysis.get('error', 'Unknown error')}")

