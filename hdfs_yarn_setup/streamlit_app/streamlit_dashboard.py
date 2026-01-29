import streamlit as st
import pandas as pd
import glob
import os

# ===============================
# Paths
# ===============================
current_dir = os.path.dirname(os.path.abspath(__file__))
processed_dir = os.path.join(os.path.dirname(current_dir), "data")
background_image = os.path.join(current_dir, "bg.png")

# ===============================
# CSS Background
# ===============================
# Only set background if image exists
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
    </style>
    """
    st.markdown(bg_css, unsafe_allow_html=True)

st.title("Flight Analytics Dashboard")

# ===============================
# Load & aggregate counts file-by-file
# ===============================
def load_stats():
    files = glob.glob(os.path.join(processed_dir, "*.parquet"))
    if not files:
        st.error("Không tìm thấy file processed nào!")
        return None

    # dict để giữ tổng counts
    month_counts = {}
    dep_counts = {}
    arr_counts = {}
    type_counts = {}
    reg_counts = {}
    unknown_flights = []

    for f in files:
        # load chỉ các cột cần thiết
        df = pd.read_parquet(f, columns=[
            "flt_id", "dof", "adep_p", "ades_p", "typecode", "registration"
        ])
        # Month counts
        df["month"] = pd.to_datetime(df["dof"], errors="coerce").dt.month
        for m, c in df["month"].value_counts(dropna=True).items():
            month_counts[m] = month_counts.get(m, 0) + c

        # Departure counts
        for k, v in df["adep_p"].value_counts(dropna=True).items():
            dep_counts[k] = dep_counts.get(k, 0) + v

        # Arrival counts
        for k, v in df["ades_p"].value_counts(dropna=True).items():
            arr_counts[k] = arr_counts.get(k, 0) + v

        # Typecode counts
        for k, v in df["typecode"].value_counts(dropna=True).items():
            type_counts[k] = type_counts.get(k, 0) + v

        # Registration counts
        for k, v in df["registration"].value_counts(dropna=True).items():
            reg_counts[k] = reg_counts.get(k, 0) + v

        # Flights with UNKNOWN flight_id
        unknown = df[df["flt_id"] == "UNKNOWN"]
        if not unknown.empty:
            unknown_flights.append(unknown[["flt_id","dof","adep_p","ades_p"]])

    return {
        "month": pd.Series(month_counts).sort_index(),
        "dep": pd.Series(dep_counts).sort_values(ascending=False).head(20),
        "arr": pd.Series(arr_counts).sort_values(ascending=False).head(20),
        "typecode": pd.Series(type_counts).sort_values(ascending=False).head(20),
        "registration": pd.Series(reg_counts).sort_values(ascending=False).head(20),
        "unknown": pd.concat(unknown_flights) if unknown_flights else pd.DataFrame()
    }

stats = load_stats()
if stats is None:
    st.stop()

# ===============================
# Sidebar Filters (từ các keys đã tổng hợp)
# ===============================
st.sidebar.header("Filters")
month_list = sorted(stats["month"].index.tolist())
selected_month = st.sidebar.multiselect("Month", month_list, default=month_list)

type_list = sorted(stats["typecode"].index.tolist())
selected_type = st.sidebar.multiselect("Aircraft Type", type_list, default=type_list)

dep_list = sorted(stats["dep"].index.tolist())
selected_dep = st.sidebar.multiselect("Departure Airport", dep_list, default=dep_list)

arr_list = sorted(stats["arr"].index.tolist())
selected_arr = st.sidebar.multiselect("Arrival Airport", arr_list, default=arr_list)

# ===============================
# Apply filters
# ===============================
def filter_series(series, selected_keys):
    return series[series.index.isin(selected_keys)]

filtered_month = filter_series(stats["month"], selected_month)
filtered_type = filter_series(stats["typecode"], selected_type)
filtered_dep = filter_series(stats["dep"], selected_dep)
filtered_arr = filter_series(stats["arr"], selected_arr)
filtered_reg = stats["registration"]  # top20 vẫn hiển thị
unknown_flights = stats["unknown"]

# ===============================
# Visualization
# ===============================
st.subheader("Flights per Month")
st.bar_chart(filtered_month)

st.subheader("Flights per Departure Airport")
st.bar_chart(filtered_dep)

st.subheader("Flights per Arrival Airport")
st.bar_chart(filtered_arr)

st.subheader("Flights per Aircraft Type (typecode)")
st.bar_chart(filtered_type)

st.subheader("Flights per Aircraft Registration")
st.dataframe(filtered_reg)

st.subheader("Flights with UNKNOWN flight_id")
st.dataframe(unknown_flights.head(20))
