import streamlit as st
import base64

def dashboard_page():
    st.title("📊 Analytics Dashboard")

    pdf_file = "dashboard.pdf"

    with open(pdf_file, "rb") as f:
        pdf_bytes = f.read()

    base64_pdf = base64.b64encode(pdf_bytes).decode("utf-8")

    # 👇 IMPORTANT: add #zoom=page-width
    pdf_display = f"""
        <iframe 
            src="data:application/pdf;base64,{base64_pdf}#zoom=page-width"
            width="100%" 
            height="1000px"
            style="border:none;">
        </iframe>
    """

    st.markdown(pdf_display, unsafe_allow_html=True)