import streamlit as st
from streamlit_option_menu import option_menu

# Import pages
from pages.chatbot import chatbot_page
from pages.dashboard import dashboard_page
from pages.ai_advisor import ai_advisor_page

# ── CONFIG ─────────────────────────────────────
st.set_page_config(
    page_title="Healthcare Data Explorer",
    page_icon="🏥",
    layout="wide"
)

# ── NAVBAR ─────────────────────────────────────
selected = option_menu(
    menu_title=None,
    options=["Chatbot", "Dashboard", "AI Advisor"],
    icons=["chat-dots", "bar-chart", "cpu"],
    orientation="horizontal",
    default_index=0
)

# ── ROUTING ────────────────────────────────────
if selected == "Chatbot":
    chatbot_page()

elif selected == "Dashboard":
    dashboard_page()

elif selected == "AI Advisor":
    ai_advisor_page()