from __future__ import annotations

from datetime import date
from pathlib import Path
import sqlite3

import pandas as pd
import streamlit as st

# ── CONSTANTS ────────────────────────────────────────────────────────────────
DB_PATH = Path("market_data.db")


# ═════════════════════════════════════════════════════════════════════════════
#  HELPER FUNCTIONS
# ═════════════════════════════════════════════════════════════════════════════
@st.cache_data
def get_distinct_options() -> tuple[list[str], list[str]]:
    """Return (index_values, concept_values) for sidebar pickers."""
    with sqlite3.connect(DB_PATH) as con:
        idx_vals = pd.read_sql(
            "SELECT DISTINCT `index` FROM stocks ORDER BY `index`",
            con,
        )["index"].tolist()
        cpt_vals = pd.read_sql(
            "SELECT DISTINCT concept_name FROM concept_names_em ORDER BY concept_name",
            con,
        )["concept_name"].tolist()
    return idx_vals, cpt_vals


def fetch_symbols_by_index(con: sqlite3.Connection, indices: list[str]) -> set[str] | None:
    """Symbols whose STOCKS.index is in *indices*. None ⇒ no filter."""
    if not indices:
        return None
    q = f"SELECT DISTINCT symbol FROM stocks WHERE `index` IN ({','.join('?' * len(indices))})"
    return {row[0] for row in con.execute(q, indices)}


def fetch_symbols_by_concept(con: sqlite3.Connection, concepts: list[str]) -> set[str] | None:
    """Symbols belonging to chosen concept names. None ⇒ no filter."""
    if not concepts:
        return None
    q = f"""
        SELECT DISTINCT cc.symbol
        FROM concept_cons_em cc
        JOIN concept_names_em cn ON cc.concept_symbol = cn.concept_symbol
        WHERE cn.concept_name IN ({','.join('?' * len(concepts))})
    """
    return {row[0] for row in con.execute(q, concepts)}


def merge_symbol_sets(set_idx: set[str] | None, set_cpt: set[str] | None) -> set[str] | None:
    """Return the intersection / union logic for the two sets."""
    if set_idx is None and set_cpt is None:
        return None
    if set_idx is None:
        return set_cpt
    if set_cpt is None:
        return set_idx
    return set_idx & set_cpt


def query_normal(start_dt: date, end_dt: date, symbols: set[str] | None) -> pd.DataFrame:
    """Run the date/symbol filter entirely in SQL."""
    with sqlite3.connect(DB_PATH) as con:
        sql = """
            SELECT *
            FROM transactions
            WHERE date BETWEEN ? AND ?
        """
        params: list = [start_dt, end_dt]

        if symbols is not None:
            if not symbols:
                return pd.DataFrame(columns=[])
            sql += f" AND symbol IN ({','.join('?' * len(symbols))})"
            params.extend(symbols)

        sql += " ORDER BY date, symbol"
        return pd.read_sql(sql, con, params=params, parse_dates=["date"])


def query_five_day_yang(end_dt: date, symbols: set[str] | None) -> pd.DataFrame:
    """Return stocks matching the 五日阳 pattern ending on *end_dt*."""
    with sqlite3.connect(DB_PATH) as con:
        params: list = [end_dt]

        symbol_clause = ""
        if symbols is not None:
            if not symbols:
                return pd.DataFrame(columns=[])
            symbol_clause = f" AND symbol IN ({','.join('?' * len(symbols))})"
            params.extend(symbols)

        sql = f"""
        WITH last6_desc AS (
            SELECT symbol,
                   date,
                   open,
                   close,
                   amount,
                   change_rate,
                   ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) AS rn_desc
            FROM transactions
            WHERE date <= ?
              {symbol_clause}
        ),
        last6 AS (
            SELECT symbol,
                   date,
                   open,
                   close,
                   amount,
                   change_rate,
                   7 - rn_desc AS rn
            FROM last6_desc
            WHERE rn_desc <= 6
        ),
        pattern_syms AS (
            SELECT symbol
            FROM last6
            GROUP BY symbol
            HAVING COUNT(*) = 6
               AND MAX(CASE WHEN rn=2 THEN amount END) >= 1.25 * MAX(CASE WHEN rn=1 THEN amount END)
               AND MIN(CASE WHEN rn >= 3 THEN change_rate END) >= -1
        ),
        summary AS (
            SELECT
                MIN(date) AS date_start,
                MAX(date) AS date_end,
                symbol,
                MAX(CASE WHEN rn=6 THEN close END) AS "day 5 close",
                MAX(CASE WHEN rn=6 THEN open  END) AS "day 5 open",
                MAX(CASE WHEN rn=5 THEN close END) AS "day 4 close",
                MAX(CASE WHEN rn=4 THEN close END) AS "day 3 close",
                MAX(CASE WHEN rn=3 THEN close END) AS "day 2 close",
                MAX(CASE WHEN rn=2 THEN close END) AS "day 1 close",
                MAX(CASE WHEN rn=1 THEN close END) AS "day 0 close"
            FROM last6
            WHERE symbol IN (SELECT symbol FROM pattern_syms)
            GROUP BY symbol
        )
        SELECT * FROM summary ORDER BY symbol;
        """
        return pd.read_sql(sql, con, params=params, parse_dates=["date_start", "date_end"])
