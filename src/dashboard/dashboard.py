import os
import time
from pathlib import Path

import pandas as pd
import streamlit as st
from deltalake import DeltaTable

PROJECT_ROOT = Path(__file__).resolve().parents[2]

OUTPUT_BASE = Path(os.environ.get("OUTPUT_BASE_PATH", PROJECT_ROOT / "data" / "output"))
WORD_COUNTS_PATH    = OUTPUT_BASE / "word_counts"
MONTHLY_COUNTS_PATH = OUTPUT_BASE / "monthly_counts"
USER_STATS_PATH     = OUTPUT_BASE / "user_stats"
MEMBERS_CSV_PATH    = PROJECT_ROOT / "data" / "members.csv"


st.set_page_config(page_title="Discord Streaming Analytics", layout="wide")


@st.cache_data(ttl=2)
def load_members() -> pd.DataFrame:
    if not MEMBERS_CSV_PATH.exists():
        return pd.DataFrame(columns=["Member ID", "Member Name", "Nickname"])
    return pd.read_csv(MEMBERS_CSV_PATH, sep=";", dtype=str)


def read_delta(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return DeltaTable(str(path)).to_pandas()
    except Exception as exc:
        st.warning(f"Could not read Delta table at {path}: {exc}")
        return pd.DataFrame()


def render_dashboard() -> None:
    st.title("Discord Streaming Analytics")
    st.caption("Real-time view of Spark Structured Streaming output (Delta Lake)")

    word_counts    = read_delta(WORD_COUNTS_PATH)
    monthly_counts = read_delta(MONTHLY_COUNTS_PATH)
    user_stats     = read_delta(USER_STATS_PATH)
    members        = load_members()

    if word_counts.empty and monthly_counts.empty and user_stats.empty:
        st.info(
            "No data yet. Make sure the producer and the stream processor are running, "
            f"and that Delta tables exist under `{OUTPUT_BASE}`."
        )
        return

    col_top_words, col_top_users = st.columns(2)

    with col_top_words:
        st.subheader("Top 10 most used words")
        if word_counts.empty:
            st.write("No data.")
        else:
            top_words = (
                word_counts.sort_values("count", ascending=False)
                           .head(10)
                           .reset_index(drop=True)
            )
            st.dataframe(top_words, use_container_width=True)

    with col_top_users:
        st.subheader("Top 3 most active users")
        if user_stats.empty:
            st.write("No data.")
        else:
            enriched = user_stats.merge(
                members, left_on="UserID", right_on="Member ID", how="left"
            )
            enriched["User"] = (
                enriched["Nickname"]
                .fillna(enriched.get("Member Name"))
                .fillna(enriched["UserID"])
            )
            top_users = (
                enriched[["User", "count"]]
                .sort_values("count", ascending=False)
                .head(3)
                .reset_index(drop=True)
            )
            st.dataframe(top_users, use_container_width=True)

    st.subheader("Messages per month")
    if monthly_counts.empty:
        st.write("No data.")
    else:
        df = monthly_counts.copy()
        df[["Year", "Month"]] = df["Month"].str.split("-", expand=True)
        years = sorted(df["Year"].unique())
        selected_year = st.sidebar.selectbox(
            "Year", options=years, index=len(years) - 1
        )

        all_months = pd.DataFrame({"Month": [f"{m:02d}" for m in range(1, 13)]})
        yearly = (
            df[df["Year"] == selected_year]
            .merge(all_months, on="Month", how="right")
            .fillna({"count": 0})
            .sort_values("Month")
        )
        st.line_chart(yearly.set_index("Month")["count"])


with st.sidebar:
    st.header("Settings")
    auto_refresh     = st.checkbox("Auto-refresh", value=True)
    refresh_interval = st.slider("Refresh interval (s)", 1, 60, 5)

render_dashboard()

if auto_refresh:
    time.sleep(refresh_interval)
    st.rerun()
