# app.py
"""
Streamlit application for exploring transactions by Index, Concept and Date.

Modes
─────
1. Normal  – Start/End date range, shows full rows.
2. 五日阳   – Single End-Date picker, applies 6-day pattern and shows
             summary columns per symbol:
             date_start, date_end, symbol,
             day 5 close/open, day 4 close, …, day 0 close
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from datetime import date, timedelta

import pandas as pd
import streamlit as st

# ── CONFIG ────────────────────────────────────────────────────────────────────
DB_PATH    = Path("market_data.db")
PAGE_TITLE = "Market Transactions Explorer"

# ── DATA HELPERS ──────────────────────────────────────────────────────────────
@st.cache_data
def load_lookup_options(db: Path) -> tuple[list[str], list[str]]:
    with sqlite3.connect(db) as con:
        idx = pd.read_sql("SELECT DISTINCT `index` FROM stocks ORDER BY `index`", con)
        cpt = pd.read_sql(
            "SELECT DISTINCT concept_name FROM concept_names_em ORDER BY concept_name",
            con,
        )
    return idx["index"].tolist(), cpt["concept_name"].tolist()


@st.cache_data
def load_core_tables(db: Path):
    with sqlite3.connect(db) as con:
        tx  = pd.read_sql("SELECT * FROM transactions", con, parse_dates=["date"])
        stk = pd.read_sql("SELECT `index`, symbol FROM stocks", con)
        cnm = pd.read_sql("SELECT concept_name, concept_symbol FROM concept_names_em", con)
        ccs = pd.read_sql("SELECT symbol, concept_symbol FROM concept_cons_em",   con)
    return tx, stk, cnm, ccs


def allowed_symbols_idx_cpt(
    stocks_df: pd.DataFrame,
    c_names_df: pd.DataFrame,
    c_cons_df: pd.DataFrame,
    sel_indices: list[str],
    sel_concepts: list[str],
) -> set[str]:
    """Symbols that satisfy Index/Concept selections (ignoring Mode)."""
    sym_by_idx = (
        set(stocks_df[stocks_df["index"].isin(sel_indices)]["symbol"])
        if sel_indices
        else set(stocks_df["symbol"])
    )
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


def five_day_yang_symbols(tx_df: pd.DataFrame, end_dt: pd.Timestamp) -> set[str]:
    """
    Return symbols that meet the '五日阳' pattern ending on `end_dt`.
    Pattern:
        • last 6 trading days (day0…day5) ≤ end_dt
        • day1.amount ≥ 1.25 × day0.amount
        • day1…day5 change_rate ≥ -1
    """
    tx_cut = tx_df[tx_df["date"] <= end_dt]

    def qualifies(g: pd.DataFrame) -> bool:
        if len(g) < 6:
            return False
        last6 = g.sort_values("date").iloc[-6:].reset_index(drop=True)
        cond_amt = last6.loc[1, "amount"] >= 1.25 * last6.loc[0, "amount"]
        cond_cr  = (last6.loc[1:, "change_rate"] >= -1).all()
        return cond_amt and cond_cr

    qual = tx_cut.groupby("symbol").apply(qualifies)
    return set(qual[qual].index)


def summary_for_yang(
    tx_df: pd.DataFrame, symbols: set[str], end_dt: pd.Timestamp
) -> pd.DataFrame:
    """
    Build one summary row per symbol with the requested columns
    for the 6-day window ending at `end_dt`.
    """
    rows = []
    for sym in symbols:
        g = (
            tx_df[(tx_df["symbol"] == sym) & (tx_df["date"] <= end_dt)]
            .sort_values("date")
            .tail(6)
            .reset_index(drop=True)
        )
        if len(g) < 6:
            continue  # safety, though pattern check already ensured length
        rows.append(
            {
                "date_start": g.loc[0, "date"].date(),
                "date_end":   g.loc[5, "date"].date(),
                "symbol": sym,
                "day 5 close": g.loc[5, "close"],
                "day 5 open":  g.loc[5, "open"],
                "day 4 close": g.loc[4, "close"],
                "day 3 close": g.loc[3, "close"],
                "day 2 close": g.loc[2, "close"],
                "day 1 close": g.loc[1, "close"],
                "day 0 close": g.loc[0, "close"],
            }
        )
    return pd.DataFrame(rows)


# ── STREAMLIT APP ─────────────────────────────────────────────────────────────
def main() -> None:
    st.set_page_config(PAGE_TITLE, layout="wide")
    st.title(PAGE_TITLE)

    # Load core data
    tx_df, stocks_df, c_names_df, c_cons_df = load_core_tables(DB_PATH)
    idx_options, cpt_options = load_lookup_options(DB_PATH)

    # Sidebar – Filters
    st.sidebar.header("Filters")

    # Mode selector
    mode = st.sidebar.radio("Mode", ("Normal", "五日阳"))

    # Session-state selections
    if "idx_sel" not in st.session_state:
        st.session_state.idx_sel = idx_options.copy()
    if "cpt_sel" not in st.session_state:
        st.session_state.cpt_sel = cpt_options.copy()

    # Index
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

    # Concept
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

    # Main area – date input(s) & filtering
    today = date.today()

    if mode == "Normal":
        st.subheader("Date Range")
        default_start = today - timedelta(days=30)
        d_start, d_end = st.date_input(
            "Select start and end dates (inclusive)",
            (default_start, today),
            key="date_range",
        )
        if d_start > d_end:
            st.error("⚠️ Start date must be on or before End date.")
            st.stop()

        sym_allowed = allowed_symbols_idx_cpt(
            stocks_df, c_names_df, c_cons_df, idx_selected, cpt_selected
        )
        tx_mask = (
            (tx_df["date"] >= pd.Timestamp(d_start))
            & (tx_df["date"] <= pd.Timestamp(d_end))
            & (tx_df["symbol"].isin(sym_allowed))
        )
        tx_filtered = tx_df.loc[tx_mask].sort_values(["date", "symbol"])

        st.markdown(
            f"**{len(tx_filtered):,}** transactions from **{len(sym_allowed):,}** "
            f"symbols between **{d_start}** and **{d_end}**."
        )
        st.dataframe(tx_filtered, use_container_width=True)

    else:  # 五日阳
        st.subheader("五日阳 – End Date")
        end_date = st.date_input("End date (day 5)", today, key="end_date")

        base_allowed = allowed_symbols_idx_cpt(
            stocks_df, c_names_df, c_cons_df, idx_selected, cpt_selected
        )
        yang_syms = five_day_yang_symbols(tx_df, pd.Timestamp(end_date))
        final_syms = base_allowed & yang_syms

        if not final_syms:
            st.warning("No symbols match Index/Concept filters *and* 五日阳 pattern.")
            st.stop()

        summary_df = summary_for_yang(tx_df, final_syms, pd.Timestamp(end_date))

        st.markdown(
            f"五日阳 symbols found: **{len(final_syms):,}** "
            f"(displaying summary for each)."
        )
        st.dataframe(summary_df, use_container_width=True)


if __name__ == "__main__":
    main()
