import streamlit as st
import pandas as pd
import os

# ===============================
# Paths
# ===============================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(
    os.path.dirname(BASE_DIR),
    "data", "analytics"
)
background_image = os.path.join(
    os.path.dirname(BASE_DIR),
    "streamlit_app", "bg.png"
)

if os.path.exists(background_image):
    with open(background_image, "rb") as img_file:
        import base64
        img_base64 = base64.b64encode(img_file.read()).decode()

    bg_css = f"""
    <style>
    .stApp {{
        background-image: url("data:image/png;base64,{img_base64}");
        background-size: cover;
        background-attachment: fixed;
    }}

    h1, h3 {{
        color: white !important;
    }}
    </style>
    """
    st.markdown(bg_css, unsafe_allow_html=True)

# ===============================
# Load analytics
# ===============================
df_month = pd.read_parquet(os.path.join(DATA_DIR, "flights_per_month.parquet"))
df_dep = pd.read_parquet(os.path.join(DATA_DIR, "flights_per_adep.parquet"))
df_arr = pd.read_parquet(os.path.join(DATA_DIR, "flights_per_ades.parquet"))
df_type = pd.read_parquet(os.path.join(DATA_DIR, "flights_per_typecode.parquet"))
df_total_dur = pd.read_parquet(os.path.join(DATA_DIR, "total_duration_by_type.parquet"))
df_avg_dur = pd.read_parquet(os.path.join(DATA_DIR, "avg_duration_by_type.parquet"))

# ===============================
# Dashboard
# ===============================
st.title("Flight Analytics Dashboard")

# -------- Flights per month --------
st.markdown("<h3>Flights per Month</h3>", unsafe_allow_html=True)
df_month = df_month.sort_values("month")
st.bar_chart(df_month.set_index("month")["count"])

# -------- Departure airports --------
st.markdown("<h3>Top Departure Airports</h3>", unsafe_allow_html=True)
st.bar_chart(
    df_dep.sort_values("count", ascending=False)
          .head(20)
          .set_index("adep_p")["count"]
)

# -------- Arrival airports --------
st.markdown("<h3>Top Arrival Airports</h3>", unsafe_allow_html=True)
st.bar_chart(
    df_arr.sort_values("count", ascending=False)
          .head(20)
          .set_index("ades_p")["count"]
)

# -------- Aircraft types --------
st.markdown("<h3>Flights per Aircraft Type</h3>", unsafe_allow_html=True)
st.bar_chart(
    df_type.sort_values("count", ascending=False)
           .head(20)
           .set_index("typecode")["count"]
)

# -------- Total duration --------
st.markdown(
    "<h3>Total Flight Duration by Aircraft Type (seconds)</h3>",
    unsafe_allow_html=True
)
st.bar_chart(
    df_total_dur.sort_values(
        "total_flight_duration_sec", ascending=False
    )
    .head(15)
    .set_index("typecode")["total_flight_duration_sec"]
)

# -------- Average duration --------
st.markdown(
    "<h3>Average Flight Duration by Aircraft Type (seconds)</h3>",
    unsafe_allow_html=True
)
st.bar_chart(
    df_avg_dur.sort_values(
        "average_duration_sec", ascending=False
    )
    .head(20)
    .set_index("typecode")["average_duration_sec"]
)
