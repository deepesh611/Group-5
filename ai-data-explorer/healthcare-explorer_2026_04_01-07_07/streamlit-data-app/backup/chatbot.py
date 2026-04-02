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

    numeric_cols = df.select_dtypes(include=["number"]).columns
    non_numeric_cols = df.select_dtypes(exclude=["number"]).columns

    # ✅ Must have:
    # 1 numeric (aggregation)
    # 1 non-numeric (group/category)
    if len(numeric_cols) >= 1 and len(non_numeric_cols) >= 1:
        return True

    return False


# ── HELPER: EXTRACT NUMBER FROM SUMMARY ───────────
def extract_number(text):
    match = re.search(r'\d[\d,]*', text)
    if match:
        return int(match.group().replace(",", ""))
    return None


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
                table = msg.get("table")
                summary = msg.get("content")

                # ✅ SHOW TABLE FIRST (IMPORTANT FIX)
                if table is not None:
                    st.dataframe(table, use_container_width=True)

                is_error = summary.startswith("❌")

                if not is_error:

                    # ✅ CASE 1: table exists
                    if table is not None and is_graphable(table):
                        if st.button("📊 Show Chart", key=f"chart_btn_{i}"):
                            st.session_state.chart_data = table
                            st.session_state.show_chart = True

                    # ✅ CASE 2: summary only
                    elif table is None:
                        num = extract_number(summary)
                        if num is not None:
                            if st.button("📊 Show Chart", key=f"chart_btn_{i}"):
                                df = pd.DataFrame({
                                    "Metric": ["Result"],
                                    "Value": [num]
                                })
                                st.session_state.chart_data = df
                                st.session_state.show_chart = True

    # ── USER INPUT ────────────────────────────────
    if prompt := st.chat_input("Ask a question about the data..."):

        st.session_state.show_chart = False
        st.session_state.chart_data = None

        st.session_state.messages.append({
            "role": "user",
            "content": prompt,
            "avatar": "👤"
        })

        with st.chat_message("user"):
            st.markdown(prompt)

        with st.chat_message("assistant"):
            with st.spinner("Thinking..."):
                response, table = run_query(prompt)

            st.markdown(response)

            if table is not None:
                pass

        st.session_state.messages.append({
            "role": "assistant",
            "content": response,
            "table": table
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