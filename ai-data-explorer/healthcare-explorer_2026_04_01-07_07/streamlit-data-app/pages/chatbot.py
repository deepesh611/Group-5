import streamlit as st
import pandas as pd
import re
from core.pipeline import run_query

# ── HELPER: CHECK IF DATA IS GRAPHABLE ─────────────
def is_graphable(df):
    if df is None or df.empty:
        return False
    if len(df.columns) < 2:
        return False
    numeric_cols     = df.select_dtypes(include=["number"]).columns
    non_numeric_cols = df.select_dtypes(exclude=["number"]).columns
    if len(numeric_cols) >= 1:
        return True
    return False


def chatbot_page():
    st.title("🏥 Healthcare Data Explorer")
    st.caption("Ask questions about Synthea healthcare data in plain English.")

    # ── SESSION STATE ─────────────────────────────
    if "messages" not in st.session_state:
        st.session_state.messages = []

    if "chart_data" not in st.session_state:
        st.session_state.chart_data = None

    if "show_chart" not in st.session_state:
        st.session_state.show_chart = False

    # ── SHOW CHAT HISTORY ─────────────────────────
    for i, msg in enumerate(st.session_state.messages):
        avatar = "👤" if msg["role"] == "user" else "🤖"
        with st.chat_message(msg["role"], avatar=avatar):
            st.markdown(msg["content"])

            if msg["role"] == "assistant":
                table     = msg.get("table")
                sql_query = msg.get("sql")
                summary   = msg.get("content")
                is_error  = summary.startswith("❌") or summary.startswith("⛔")

                # Show table
                if table is not None:
                    st.dataframe(table, use_container_width=True)

                # Show SQL expander
                if sql_query:
                    with st.expander("🔍 View SQL Query"):
                        st.code(sql_query, language="sql")

                # Show chart button only if table is graphable
                if not is_error and table is not None and is_graphable(table):
                    if st.button("📊 Show Chart", key=f"chart_btn_{i}"):
                        st.session_state.chart_data = table
                        st.session_state.show_chart = True

    # ── USER INPUT ────────────────────────────────
    if prompt := st.chat_input("Ask a question about the data..."):
        st.session_state.show_chart = False
        st.session_state.chart_data = None

        st.session_state.messages.append({
            "role":    "user",
            "content": prompt
        })

        with st.chat_message("user", avatar="👤"):
            st.markdown(prompt)

        with st.chat_message("assistant", avatar="🤖"):
            with st.spinner("Thinking..."):
                response, table, sql_query = run_query(prompt)

            st.markdown(response)

            if table is not None:
                st.dataframe(table, use_container_width=True)

            if sql_query:
                with st.expander("🔍 View SQL Query"):
                    st.code(sql_query, language="sql")

        st.session_state.messages.append({
            "role":    "assistant",
            "content": response,
            "table":   table,
            "sql":     sql_query
        })

        st.rerun()

    # ── CHART RENDERING ───────────────────────────
    if st.session_state.show_chart and st.session_state.chart_data is not None:
        df = st.session_state.chart_data
        st.subheader("📊 Visualization")
        try:
            if len(df.columns) >= 2:
                x_col = df.columns[0]
                y_col = df.columns[1]
                st.bar_chart(df.set_index(x_col)[y_col])
            else:
                st.warning("Not enough data to plot.")
        except:
            st.warning("Couldn't generate chart.")