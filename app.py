# app.py
"""
Streamlit application for exploring transactions by Index, Concept and Date.
─────────────────────────────────────────────────────────────────────────────
• Sidebar:
    – Index section (multi-select, search, scroll, Select-All / Clear-All)
    – Concept section (multi-select, search, scroll, Select-All / Clear-All)
• Top of main page:
    – Date-range picker (inclusive)
• Main body:
    – Table of filtered `transactions`
─────────────────────────────────────────────────────────────────────────────
Database schema assumed (tables already created):
    transactions(date, symbol, open, close, high, low, volume, amount, amplitude,
                 change_rate, change, turnover_rate)
    stocks(index, symbol)                      -- PK(symbol)
    concept_names_em(concept_name, concept_symbol)   -- PK(concept_symbol)
    concept_cons_em(symbol, concept_symbol)
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from datetime import date, timedelta

import pandas as pd
import streamlit as st

# ── CONFIG ────────────────────────────────────────────────────────────────────
DB_PATH = Path("market_data.db")
PAGE_TITLE = "Market Transactions Explorer"

# ── DATA HELPERS ──────────────────────────────────────────────────────────────
@st.cache_data  # refreshes only if DB file timestamp changes
def load_lookup_options(db: Path) -> tuple[list[str], list[str]]:
    """Return (index_options, concept_options) sorted alphabetically."""
    with sqlite3.connect(db) as con:
        idx = pd.read_sql("SELECT DISTINCT `index` FROM stocks ORDER BY `index`", con)
        cpt = pd.read_sql(
            "SELECT DISTINCT concept_name FROM concept_names_em ORDER BY concept_name",
            con,
        )
    return idx["index"].tolist(), cpt["concept_name"].tolist()


@st.cache_data
def load_core_tables(db: Path):
    """Load the four core tables into DataFrames (dates parsed)."""
    with sqlite3.connect(db) as con:
        tx = pd.read_sql("SELECT * FROM transactions", con, parse_dates=["date"])
        stocks = pd.read_sql("SELECT `index`, symbol FROM stocks", con)
        c_names = pd.read_sql(
            "SELECT concept_name, concept_symbol FROM concept_names_em", con
        )
        c_cons = pd.read_sql(
            "SELECT symbol, concept_symbol FROM concept_cons_em", con
        )
    return tx, stocks, c_names, c_cons


def allowed_symbols(
    stocks_df: pd.DataFrame,
    c_names_df: pd.DataFrame,
    c_cons_df: pd.DataFrame,
    sel_indices: list[str],
    sel_concepts: list[str],
) -> set[str]:
    """Return symbols that satisfy current Index & Concept selections."""
    # Filter by index
    sym_by_idx = (
        set(stocks_df[stocks_df["index"].isin(sel_indices)]["symbol"])
        if sel_indices
        else set(stocks_df["symbol"])
    )

    # Filter by concept
    if sel_concepts:
        sel_c_symbols = c_names_df.loc[
            c_names_df["concept_name"].isin(sel_concepts), "concept_symbol"
        ]
        sym_by_cpt = set(
            c_cons_df[c_cons_df["concept_symbol"].isin(sel_c_symbols)]["symbol"]
        )
    else:
        sym_by_cpt = set(stocks_df["symbol"])

    return sym_by_idx & sym_by_cpt


# ── STREAMLIT APP ─────────────────────────────────────────────────────────────
def main() -> None:
    st.set_page_config(PAGE_TITLE, layout="wide")
    st.title(PAGE_TITLE)

    # ── Load DB data (cached) ────────────────────────────────────────────────
    idx_options, cpt_options = load_lookup_options(DB_PATH)
    tx_df, stocks_df, c_names_df, c_cons_df = load_core_tables(DB_PATH)

    # ── Sidebar: Index & Concept filters ────────────────────────────────────
    st.sidebar.header("Filters")

    # 🔸 Session state keeps selections consistent after reruns
    if "idx_sel" not in st.session_state:
        st.session_state.idx_sel = idx_options.copy()
    if "cpt_sel" not in st.session_state:
        st.session_state.cpt_sel = cpt_options.copy()

    # ----- Index section ----------------------------------------------------
    st.sidebar.subheader("Index")
    col_idx1, col_idx2 = st.sidebar.columns(2)
    if col_idx1.button("Select All", key="idx_all"):
        st.session_state.idx_sel = idx_options.copy()
    if col_idx2.button("Clear All", key="idx_clear"):
        st.session_state.idx_sel = []
    idx_selected = st.sidebar.multiselect(
        "Choose index values",
        options=idx_options,
        default=st.session_state.idx_sel,
        key="idx_ms",
    )
    st.session_state.idx_sel = idx_selected

    # ----- Concept section --------------------------------------------------
    st.sidebar.subheader("Concept")
    col_cpt1, col_cpt2 = st.sidebar.columns(2)
    if col_cpt1.button("Select All", key="cpt_all"):
        st.session_state.cpt_sel = cpt_options.copy()
    if col_cpt2.button("Clear All", key="cpt_clear"):
        st.session_state.cpt_sel = []
    cpt_selected = st.sidebar.multiselect(
        "Choose concept names",
        options=cpt_options,
        default=st.session_state.cpt_sel,
        key="cpt_ms",
    )
    st.session_state.cpt_sel = cpt_selected

    # ── Main area: Date range picker ────────────────────────────────────────
    st.subheader("Date Range")
    today = date.today()
    default_start = today - timedelta(days=30)
    d_start, d_end = st.date_input(
        "Select start and end dates (inclusive)",
        (default_start, today),
        key="date_range",
    )
    if d_start > d_end:
        st.error("⚠️ Start date must be on or before End date.")
        st.stop()

    # ── Filter logic ────────────────────────────────────────────────────────
    sym_allowed = allowed_symbols(
        stocks_df, c_names_df, c_cons_df, idx_selected, cpt_selected
    )

    tx_mask = (
        (tx_df["date"] >= pd.Timestamp(d_start))
        & (tx_df["date"] <= pd.Timestamp(d_end))
        & (tx_df["symbol"].isin(sym_allowed))
    )
    tx_filtered = tx_df.loc[tx_mask].sort_values(["date", "symbol"])

    # ── Display results ─────────────────────────────────────────────────────
    st.markdown(
        f"**{len(tx_filtered):,}** transactions from **{len(sym_allowed):,}** "
        f"symbols between **{d_start}** and **{d_end}**."
    )
    st.dataframe(tx_filtered, use_container_width=True)


if __name__ == "__main__":
    main()
