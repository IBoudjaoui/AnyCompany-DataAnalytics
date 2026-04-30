# ============================================================
# PROJET    : AnyCompany Food & Beverage – Data Analytics
# FICHIER   : streamlit/sales_dashboard.py
# AUTEUR    : Équipe Data Analytics – MBAESG 2026
# DESCRIPTION : Phase 2 – Tableau de bord des tendances de ventes
#               Visualisation interactive des performances commerciales
#               par période, région et mode de paiement.
# DÉPENDANCES : pip install streamlit snowflake-connector-python pandas plotly
# ============================================================

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import date

# ── Page config ──────────────────────────────────────────────
st.set_page_config(
    page_title="AnyCompany – Sales Dashboard",
    page_icon="📊",
    layout="wide",
)

# ── Snowflake connection ──────────────────────────────────────
@st.cache_resource(show_spinner="Connecting to Snowflake…")
def get_connection(account, user, password, warehouse, database, schema):
    import snowflake.connector
    return snowflake.connector.connect(
        account=account,
        user=user,
        password=password,
        warehouse=warehouse,
        database=database,
        schema=schema,
    )

@st.cache_data(ttl=300, show_spinner="Running query…")
def run_query(_conn, sql: str) -> pd.DataFrame:
    return pd.read_sql(sql, _conn)

# ── Sidebar – credentials ─────────────────────────────────────
with st.sidebar:
    st.image("https://upload.wikimedia.org/wikipedia/commons/thumb/f/f7/Snowflake_Logo.svg/320px-Snowflake_Logo.svg.png", width=140)
    st.title("🔐 Connexion Snowflake")
    sf_account  = st.text_input("Account",   placeholder="abc123.us-west-2")
    sf_user     = st.text_input("User",       placeholder="john_doe")
    sf_password = st.text_input("Password",   type="password")
    sf_wh       = st.text_input("Warehouse",  value="ANYCOMPANY_WH")
    sf_db       = st.text_input("Database",   value="ANYCOMPANY_LAB")
    sf_schema   = st.text_input("Schema",     value="SILVER")
    connect_btn = st.button("Se connecter", use_container_width=True, type="primary")

# ── Header ────────────────────────────────────────────────────
st.title("📊 AnyCompany Food & Beverage – Sales Dashboard")
st.caption("Phase 2 – Analyse des tendances de ventes | Données : schéma SILVER")
st.divider()

# ── Connection gate ───────────────────────────────────────────
conn = None
if connect_btn and sf_account and sf_user and sf_password:
    try:
        conn = get_connection(sf_account, sf_user, sf_password, sf_wh, sf_db, sf_schema)
        st.sidebar.success("✅ Connexion établie")
    except Exception as e:
        st.sidebar.error(f"❌ Erreur : {e}")

if conn is None:
    st.info("👈 Renseignez vos identifiants Snowflake dans la barre latérale pour charger les données réelles.")
    st.stop()

# ─────────────────────────────────────────────────────────────
# DATA LOADING
# ─────────────────────────────────────────────────────────────
SQL_MONTHLY = """
SELECT
    DATE_TRUNC('month', transaction_date)          AS mois,
    COUNT(*)                                       AS nb_transactions,
    ROUND(SUM(amount), 2)                          AS ca_total,
    ROUND(AVG(amount), 2)                          AS panier_moyen
FROM ANYCOMPANY_LAB.SILVER.FINANCIAL_TRANSACTIONS
WHERE transaction_type = 'Sale'
GROUP BY 1 ORDER BY 1
"""

SQL_ANNUAL = """
SELECT
    DATE_PART('year', transaction_date)            AS annee,
    COUNT(*)                                       AS nb_transactions,
    ROUND(SUM(amount), 2)                          AS ca_total,
    ROUND(AVG(amount), 2)                          AS panier_moyen
FROM ANYCOMPANY_LAB.SILVER.FINANCIAL_TRANSACTIONS
WHERE transaction_type = 'Sale'
GROUP BY 1 ORDER BY 1
"""

SQL_REGION = """
SELECT
    region,
    COUNT(*)                                       AS nb_transactions,
    ROUND(SUM(amount), 2)                          AS ca_total,
    ROUND(AVG(amount), 2)                          AS panier_moyen,
    ROUND(SUM(amount)*100.0/SUM(SUM(amount)) OVER(), 2) AS part_ca_pct
FROM ANYCOMPANY_LAB.SILVER.FINANCIAL_TRANSACTIONS
WHERE transaction_type = 'Sale'
GROUP BY 1 ORDER BY ca_total DESC
"""

SQL_PAYMENT = """
SELECT
    payment_method,
    COUNT(*)                                       AS nb_transactions,
    ROUND(SUM(amount), 2)                          AS ca_total
FROM ANYCOMPANY_LAB.SILVER.FINANCIAL_TRANSACTIONS
WHERE transaction_type = 'Sale'
GROUP BY 1 ORDER BY ca_total DESC
"""

SQL_REFUNDS = """
SELECT
    DATE_PART('year', transaction_date)            AS annee,
    transaction_type,
    COUNT(*)                                       AS nb_transactions,
    ROUND(SUM(amount), 2)                          AS montant_total
FROM ANYCOMPANY_LAB.SILVER.FINANCIAL_TRANSACTIONS
WHERE transaction_type IN ('Sale','Refund')
GROUP BY 1, 2 ORDER BY 1, 2
"""

SQL_DEMO = """
SELECT
    CASE
        WHEN DATEDIFF('year', date_of_birth, CURRENT_DATE) < 30 THEN '18-29'
        WHEN DATEDIFF('year', date_of_birth, CURRENT_DATE) < 45 THEN '30-44'
        WHEN DATEDIFF('year', date_of_birth, CURRENT_DATE) < 60 THEN '45-59'
        ELSE '60+'
    END          AS tranche_age,
    gender,
    COUNT(*)     AS nb_clients,
    ROUND(AVG(annual_income), 2) AS revenu_moyen
FROM ANYCOMPANY_LAB.SILVER.CUSTOMER_DEMOGRAPHICS
GROUP BY 1, 2 ORDER BY 1, 2
"""

df_monthly  = run_query(conn, SQL_MONTHLY)
df_annual   = run_query(conn, SQL_ANNUAL)
df_region   = run_query(conn, SQL_REGION)
df_payment  = run_query(conn, SQL_PAYMENT)
df_refunds  = run_query(conn, SQL_REFUNDS)
df_demo     = run_query(conn, SQL_DEMO)

# Standardize column names to lower
for df in [df_monthly, df_annual, df_region, df_payment, df_refunds, df_demo]:
    df.columns = [c.lower() for c in df.columns]

# ─────────────────────────────────────────────────────────────
# KPI ROW
# ─────────────────────────────────────────────────────────────
total_ca    = df_annual["ca_total"].sum()
total_txn   = df_annual["nb_transactions"].sum()
avg_basket  = df_monthly["panier_moyen"].mean()
top_region  = df_region.iloc[0]["region"] if not df_region.empty else "N/A"

k1, k2, k3, k4 = st.columns(4)
k1.metric("💰 CA Total (toutes années)", f"${total_ca:,.0f}")
k2.metric("🧾 Nombre de transactions",   f"{int(total_txn):,}")
k3.metric("🛒 Panier moyen",             f"${avg_basket:,.2f}")
k4.metric("🌍 Région n°1",               top_region)

st.divider()

# ─────────────────────────────────────────────────────────────
# ROW 1 – Évolution mensuelle CA + Transactions
# ─────────────────────────────────────────────────────────────
col1, col2 = st.columns(2)

with col1:
    st.subheader("📈 Évolution mensuelle du CA")
    if not df_monthly.empty:
        fig = px.area(
            df_monthly, x="mois", y="ca_total",
            labels={"mois": "Mois", "ca_total": "CA (USD)"},
            color_discrete_sequence=["#1E90FF"],
        )
        fig.update_layout(margin=dict(t=20, b=20))
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("Aucune donnée disponible.")

with col2:
    st.subheader("📊 CA annuel et panier moyen")
    if not df_annual.empty:
        fig = go.Figure()
        fig.add_bar(x=df_annual["annee"], y=df_annual["ca_total"],
                    name="CA Total", marker_color="#1E90FF")
        fig.add_scatter(x=df_annual["annee"], y=df_annual["panier_moyen"],
                        name="Panier moyen", mode="lines+markers",
                        yaxis="y2", line=dict(color="orange", width=2))
        fig.update_layout(
            yaxis=dict(title="CA (USD)"),
            yaxis2=dict(title="Panier moyen (USD)", overlaying="y", side="right"),
            legend=dict(orientation="h"),
            margin=dict(t=20, b=20),
        )
        st.plotly_chart(fig, use_container_width=True)

# ─────────────────────────────────────────────────────────────
# ROW 2 – Répartition géographique + Mode de paiement
# ─────────────────────────────────────────────────────────────
col3, col4 = st.columns(2)

with col3:
    st.subheader("🌍 Part de CA par région")
    if not df_region.empty:
        fig = px.bar(
            df_region, x="ca_total", y="region", orientation="h",
            text="part_ca_pct", labels={"ca_total": "CA (USD)", "region": "Région"},
            color="ca_total", color_continuous_scale="Blues",
        )
        fig.update_traces(texttemplate="%{text}%", textposition="outside")
        fig.update_layout(margin=dict(t=20, b=20), coloraxis_showscale=False)
        st.plotly_chart(fig, use_container_width=True)

with col4:
    st.subheader("💳 Répartition par mode de paiement")
    if not df_payment.empty:
        fig = px.pie(
            df_payment, values="ca_total", names="payment_method",
            color_discrete_sequence=px.colors.qualitative.Pastel,
            hole=0.4,
        )
        fig.update_layout(margin=dict(t=20, b=20))
        st.plotly_chart(fig, use_container_width=True)

# ─────────────────────────────────────────────────────────────
# ROW 3 – Ventes vs Remboursements + Démographie
# ─────────────────────────────────────────────────────────────
col5, col6 = st.columns(2)

with col5:
    st.subheader("⚖️ Ventes vs Remboursements par année")
    if not df_refunds.empty:
        fig = px.bar(
            df_refunds, x="annee", y="montant_total", color="transaction_type",
            barmode="group",
            labels={"annee": "Année", "montant_total": "Montant (USD)", "transaction_type": "Type"},
            color_discrete_map={"Sale": "#1E90FF", "Refund": "#FF6347"},
        )
        fig.update_layout(margin=dict(t=20, b=20))
        st.plotly_chart(fig, use_container_width=True)

with col6:
    st.subheader("👥 Clients par tranche d'âge & genre")
    if not df_demo.empty:
        fig = px.bar(
            df_demo, x="tranche_age", y="nb_clients", color="gender",
            barmode="group",
            labels={"tranche_age": "Tranche d'âge", "nb_clients": "Nb clients", "gender": "Genre"},
            color_discrete_sequence=px.colors.qualitative.Set2,
        )
        fig.update_layout(margin=dict(t=20, b=20))
        st.plotly_chart(fig, use_container_width=True)

# ─────────────────────────────────────────────────────────────
# RAW DATA EXPLORER
# ─────────────────────────────────────────────────────────────
with st.expander("🔍 Données brutes – Évolution mensuelle"):
    st.dataframe(df_monthly, use_container_width=True)

st.caption("AnyCompany Food & Beverage – MBAESG 2026 | Données : Snowflake ANYCOMPANY_LAB.SILVER")
