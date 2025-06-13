# app.py
"""
Market Transactions Explorer  ── SQLite-backed filtering
────────────────────────────────────────────────────────
Sidebar filters
• Mode      : Normal  |  五日阳        (single-choice radio)
• Index     : multi-select  (values from STOCKS.index)
• Concept   : multi-select  (values from CONCEPT_NAMES_EM.concept_name)

Normal mode
───────────
• Start / End date picker.
• Query built as:
      SELECT * FROM transactions
      WHERE date BETWEEN ? AND ?
        AND symbol IN (final_symbol_set)   -- optional
  final_symbol_set is the intersection of:
      (symbols having chosen indices)  ∩  (symbols in chosen concepts)

五日阳 mode
──────────
• Single End-Date picker.
• Pattern checked entirely in SQL with window functions:
      – last 6 trading days ≤ end_date (end_date is day 5)
      – day1.amount ≥ 1.25 × day0.amount
      – day2…day5 change_rate ≥ −1 %
• Output one summary row / symbol:
      date_start, date_end, symbol,
      day 5 close/open, day 4 … day 0 close
"""

from __future__ import annotations

from datetime import date, timedelta

import sqlite3
import streamlit as st

from data_utils import (
    DB_PATH,
    get_distinct_options,
    fetch_symbols_by_index,
    fetch_symbols_by_concept,
    merge_symbol_sets,
    query_normal,
    query_five_day_yang,
)

PAGE_TITLE = "Market Transactions Explorer"



# ═════════════════════════════════════════════════════════════════════════════
#  STREAMLIT UI
# ═════════════════════════════════════════════════════════════════════════════
def main() -> None:
    st.set_page_config(PAGE_TITLE, layout="wide")
    st.title(PAGE_TITLE)

    idx_all, cpt_all = get_distinct_options()

    # ── Sidebar widgets ─────────────────────────────────────────────────────
    st.sidebar.header("Filters")

    mode = st.sidebar.radio("Mode", ("Normal", "五日阳"))

    # Keep selections across reruns
    if "sel_idx" not in st.session_state:
        st.session_state.sel_idx = idx_all.copy()
    if "sel_cpt" not in st.session_state:
        st.session_state.sel_cpt = cpt_all.copy()

    # Index section
    st.sidebar.subheader("Index")
    b_si, b_ci = st.sidebar.columns(2)
    if b_si.button("Select All", key="idx_all"):
        st.session_state.sel_idx = idx_all.copy()
    if b_ci.button("Clear All", key="idx_clear"):
        st.session_state.sel_idx = []
    sel_idx = st.sidebar.multiselect(
        "Select Index values",
        idx_all,
        default=st.session_state.sel_idx,
        key="idx_ms",
    )
    st.session_state.sel_idx = sel_idx

    # Concept section
    st.sidebar.subheader("Concept")
    b_sc, b_cc = st.sidebar.columns(2)
    if b_sc.button("Select All", key="cpt_all"):
        st.session_state.sel_cpt = cpt_all.copy()
    if b_cc.button("Clear All", key="cpt_clear"):
        st.session_state.sel_cpt = []
    sel_cpt = st.sidebar.multiselect(
        "Select Concept names",
        cpt_all,
        default=st.session_state.sel_cpt,
        key="cpt_ms",
    )
    st.session_state.sel_cpt = sel_cpt

    # ── Resolve symbol set (done once) ──────────────────────────────────────
    with sqlite3.connect(DB_PATH) as con:
        syms_idx = fetch_symbols_by_index(con, sel_idx)
        syms_cpt = fetch_symbols_by_concept(con, sel_cpt)
    final_syms = merge_symbol_sets(syms_idx, syms_cpt)

    # If filter yields empty intersection → nothing to do
    if final_syms is not None and not final_syms:
        st.warning("No symbols satisfy the chosen Index + Concept filters.")
        st.stop()

    # ── Main pane by Mode ───────────────────────────────────────────────────
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

        df = query_normal(d_start, d_end, final_syms)

        st.markdown(
            f"**{len(df):,}** transactions "
            f"from **{len(final_syms) if final_syms is not None else 'all'}** symbols "
            f"between **{d_start}** and **{d_end}**."
        )
        st.dataframe(df, use_container_width=True)

    else:  # 五日阳
        st.subheader("五日阳 – End Date")
        end_dt = st.date_input("End date (day 5)", today, key="end_dt")

        df = query_five_day_yang(end_dt, final_syms)

        if df.empty:
            st.warning("No symbols satisfy Index/Concept filters *and* 五日阳 pattern.")
            st.stop()

        st.markdown(
            f"五日阳 symbols found: **{len(df):,}** "
            f"(showing summary for each up to **{end_dt}**)."
        )
        st.dataframe(df, use_container_width=True)


# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    main()
