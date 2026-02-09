
import json
import re
import pandas as pd
import streamlit as st

import firebase_admin
from firebase_admin import credentials, firestore

COLLECTION = "dm_value_signals_1x2"

# -----------------------------
# Firestore connection (Admin)
# -----------------------------
@st.cache_resource
def get_db():
    # Service account JSON viene en st.secrets["firebase"]["service_account_json"]
    sa_json = st.secrets["firebase"]["service_account_json"]
    if isinstance(sa_json, str):
        sa = json.loads(sa_json)
    else:
        sa = sa_json

    if not firebase_admin._apps:
        cred = credentials.Certificate(sa)
        firebase_admin.initialize_app(cred)

    return firestore.client()

@st.cache_data(ttl=600)
def fetch_df():
    db = get_db()
    docs = db.collection(COLLECTION).stream()
    rows = []
    for d in docs:
        r = d.to_dict()
        r["_doc_id"] = d.id
        rows.append(r)
    df = pd.DataFrame(rows)

    # types
    df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d", errors="coerce")
    for c in ["cb_stake","cb_pnl","cb_bank_after","cb_dd_after"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    return df

def apply_filters(df, league, date_from, date_to, cb_action):
    out = df.copy()
    if league != "ALL":
        out = out[out["league"] == league]
    if cb_action != "ALL":
        out = out[out["cb_action"].astype(str).str.upper() == cb_action]
    out = out[(out["date"] >= pd.to_datetime(date_from)) & (out["date"] <= pd.to_datetime(date_to))]
    return out.sort_values(["date","dia_id"], ascending=True, na_position="last")

def pick_by_action(row, prefix):
    a = str(row.get("cb_action","")).upper()
    if a == "H": return row.get(f"{prefix}_H")
    if a == "D": return row.get(f"{prefix}_D")
    if a == "A": return row.get(f"{prefix}_A")
    return None

# -----------------------------
# UI
# -----------------------------
st.set_page_config(page_title="1X2 Value Signals Dashboard", layout="wide")
st.title("1X2 Value Signals — Dashboard (Read-only)")

df = fetch_df()

# Sidebar filters (policy_version fijo)
leagues = ["ALL"] + sorted([x for x in df["league"].dropna().unique().tolist()])
actions = ["ALL"] + sorted([x for x in df["cb_action"].dropna().astype(str).str.upper().unique().tolist()])

min_date = df["date"].min().date()
max_date = df["date"].max().date()

st.sidebar.header("Filtros")
league = st.sidebar.selectbox("League", leagues, index=0)
cb_action = st.sidebar.selectbox("cb_action", actions, index=0)
date_from = st.sidebar.date_input("Date from", value=min_date, min_value=min_date, max_value=max_date)
date_to = st.sidebar.date_input("Date to", value=max_date, min_value=min_date, max_value=max_date)

df_f = apply_filters(df, league, date_from, date_to, cb_action)

# Derived
df_f["is_bet"] = df_f["cb_action"].astype(str).str.upper().ne("NO_BET")
df_bets = df_f[df_f["is_bet"]].copy()
df_bets["ev_cb"] = df_bets.apply(lambda r: pick_by_action(r, "ev"), axis=1)
df_bets["edge_cb"] = df_bets.apply(lambda r: pick_by_action(r, "edge"), axis=1)

# -----------------------------
# Home KPIs
# -----------------------------
matches = len(df_f)
bets = len(df_bets)
stake_total = float(df_bets["cb_stake"].fillna(0).sum()) if bets else 0.0
pnl_total = float(df_bets["cb_pnl"].fillna(0).sum()) if bets else 0.0
roi_simple = (pnl_total / stake_total) if stake_total > 0 else 0.0
bank_final = df_f["cb_bank_after"].dropna().iloc[-1] if df_f["cb_bank_after"].notna().any() else None
max_dd = df_f["cb_dd_after"].dropna().max() if df_f["cb_dd_after"].notna().any() else None

c1,c2,c3,c4,c5,c6 = st.columns(6)
c1.metric("#Matches", matches)
c2.metric("#Bets", bets)
c3.metric("Stake total", f"{stake_total:,.2f}")
c4.metric("PnL total", f"{pnl_total:,.2f}")
c5.metric("ROI simple", f"{roi_simple*100:,.2f}%")
c6.metric("MaxDD", "-" if max_dd is None else f"{max_dd*100:,.2f}%")

st.divider()

# -----------------------------
# Tabs (Bloques 5-8)
# -----------------------------
tab1, tab2, tab3, tab4 = st.tabs(["Explorador", "Value & Edge", "Backtest", "Bankroll & Riesgo"])

with tab1:
    st.subheader("Explorador de partidos")
    cols_explorer = [
        "date","season","league","country","dia_id","home_team","away_team",
        "pred_result","result",
        "proba_H_XG_2STAGE","proba_D_XG_2STAGE","proba_A_XG_2STAGE",
        "oH","oD","oA","overround_open",
        "ev_H","ev_D","ev_A","edge_H","edge_D","edge_A",
        "cb_action","cb_stake","cb_pnl","cb_bank_after","cb_dd_after",
    ]
    cols_explorer = [c for c in cols_explorer if c in df_f.columns]
    st.dataframe(df_f[cols_explorer], use_container_width=True, height=520)

    csv = df_f[cols_explorer].to_csv(index=False).encode("utf-8")
    st.download_button("Descargar CSV (filtrado)", data=csv, file_name="explorador_filtrado.csv", mime="text/csv")

with tab2:
    st.subheader("Value & Edge (según acción)")
    if bets == 0:
        st.info("No hay apuestas con los filtros actuales.")
    else:
        colA, colB = st.columns(2)
        with colA:
            st.write("Top 20 por EV (ev_cb)")
            top_ev = df_bets.sort_values("ev_cb", ascending=False, na_position="last")[
                ["date","league","home_team","away_team","cb_action","cb_stake","ev_cb","edge_cb","cb_pnl","result"]
            ].head(20)
            st.dataframe(top_ev, use_container_width=True, height=420)
        with colB:
            st.write("Distribución EV (ev_cb)")
            st.bar_chart(df_bets["ev_cb"].dropna())

        st.write("Distribución Edge (edge_cb)")
        st.bar_chart(df_bets["edge_cb"].dropna())

with tab3:
    st.subheader("Backtest / Resultados (solo apuestas)")
    if bets == 0:
        st.info("No hay apuestas con los filtros actuales.")
    else:
        conf = pd.crosstab(
            df_bets["cb_action"].astype(str).str.upper(),
            df_bets["result"].astype(str).str.upper(),
            dropna=False
        )
        st.write("Acción vs Resultado (conteos)")
        st.dataframe(conf, use_container_width=True)

        hit_rate = float((df_bets["cb_action"].astype(str).str.upper() == df_bets["result"].astype(str).str.upper()).mean())
        st.metric("Hit-rate (apuestas)", f"{hit_rate*100:,.2f}%")

        by_league = (df_bets
            .groupby("league", dropna=False)
            .agg(
                bets=("cb_action","size"),
                stake_total=("cb_stake","sum"),
                pnl_total=("cb_pnl","sum"),
                ev_sum=("ev_cb","sum"),
            )
            .reset_index()
        )
        by_league["roi_simple"] = by_league.apply(lambda r: (r["pnl_total"]/r["stake_total"]) if r["stake_total"] else 0.0, axis=1)
        st.write("KPIs por liga (solo apuestas)")
        st.dataframe(by_league.sort_values("pnl_total", ascending=False), use_container_width=True, height=420)

with tab4:
    st.subheader("Bankroll & Riesgo")
    df_ts = df_f.sort_values(["date","dia_id"], ascending=True, na_position="last")
    st.write("Bankroll (cb_bank_after)")
    if "cb_bank_after" in df_ts.columns:
        st.line_chart(df_ts.set_index("date")["cb_bank_after"])
    st.write("Drawdown (cb_dd_after)")
    if "cb_dd_after" in df_ts.columns:
        st.line_chart(df_ts.set_index("date")["cb_dd_after"])
