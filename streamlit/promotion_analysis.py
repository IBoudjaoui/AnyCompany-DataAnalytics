# ============================================================
# PROJET    : AnyCompany Food & Beverage – Data Analytics
# FICHIER   : streamlit/promotion_analysis.py
# AUTEUR    : Équipe Data Analytics – MBAESG 2026
# DESCRIPTION : Phase 2 – Analyse de l'impact des promotions
#               Visualisation de la sensibilité des catégories,
#               performance par région et durée des promotions.
# DÉPENDANCES : pip install streamlit snowflake-connector-python pandas plotly
# ============================================================

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

# ── Page config ──────────────────────────────────────────────
st.set_page_config(
    page_title="AnyCompany – Promotion Analysis",
    page_icon="🎯",
    layout="wide",
)

# ── Snowflake helpers ─────────────────────────────────────────
@st.cache_resource(show_spinner="Connecting to Snowflake…")
def get_connection(account, user, password, warehouse, database, schema):
    import snowflake.connector
    return snowflake.connector.connect(
        account=account, user=user, password=password,
        warehouse=warehouse, database=database, schema=schema,
    )

@st.cache_data(ttl=300, show_spinner="Running query…")
def run_query(_conn, sql: str) -> pd.DataFrame:
    return pd.read_sql(sql, _conn)

# ── Sidebar ───────────────────────────────────────────────────
with st.sidebar:
    st.image("https://upload.wikimedia.org/wikipedia/commons/thumb/f/f7/Snowflake_Logo.svg/320px-Snowflake_Logo.svg.png", width=140)
    st.title("🔐 Connexion Snowflake")
    sf_account  = st.text_input("Account",  placeholder="abc123.us-west-2")
    sf_user     = st.text_input("User",      placeholder="john_doe")
    sf_password = st.text_input("Password",  type="password")
    sf_wh       = st.text_input("Warehouse", value="ANYCOMPANY_WH")
    sf_db       = st.text_input("Database",  value="ANYCOMPANY_LAB")
    sf_schema   = st.text_input("Schema",    value="SILVER")
    connect_btn = st.button("Se connecter", use_container_width=True, type="primary")

# ── Header ────────────────────────────────────────────────────
st.title("🎯 AnyCompany Food & Beverage – Analyse des Promotions")
st.caption("Phase 2 – Impact des promotions sur les ventes | Données : schéma SILVER")
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
SQL_SENSITIVITY = """
SELECT
    p.product_category,
    COUNT(DISTINCT p.promotion_id)               AS nb_promos_lancees,
    ROUND(AVG(p.discount_percentage)*100, 1)     AS remise_moyenne_pct,
    COUNT(t.transaction_id)                      AS nb_ventes_totales,
    ROUND(SUM(t.amount), 2)                      AS ca_total_genere
FROM ANYCOMPANY_LAB.SILVER.PROMOTIONS_CLEAN p
LEFT JOIN ANYCOMPANY_LAB.SILVER.FINANCIAL_TRANSACTIONS t
    ON  t.transaction_date BETWEEN p.start_date AND p.end_date
    AND t.region           = p.region
    AND t.transaction_type = 'Sale'
GROUP BY 1
ORDER BY ca_total_genere DESC
"""

SQL_REGION = """
SELECT
    p.region,
    COUNT(DISTINCT p.promotion_id)               AS nb_promos,
    ROUND(AVG(p.discount_percentage)*100, 1)     AS remise_moyenne_pct,
    COUNT(t.transaction_id)                      AS nb_ventes,
    ROUND(SUM(t.amount), 2)                      AS ca_sous_promo
FROM ANYCOMPANY_LAB.SILVER.PROMOTIONS_CLEAN p
LEFT JOIN ANYCOMPANY_LAB.SILVER.FINANCIAL_TRANSACTIONS t
    ON  t.transaction_date BETWEEN p.start_date AND p.end_date
    AND t.region           = p.region
    AND t.transaction_type = 'Sale'
GROUP BY 1
ORDER BY ca_sous_promo DESC
"""

SQL_TOP_PROMOS = """
SELECT
    p.promotion_id,
    p.promotion_type,
    p.product_category,
    p.region,
    ROUND(p.discount_percentage*100, 1)          AS remise_pct,
    DATEDIFF('day', p.start_date, p.end_date)    AS duree_jours,
    COUNT(t.transaction_id)                      AS nb_ventes,
    ROUND(SUM(t.amount), 2)                      AS ca_genere
FROM ANYCOMPANY_LAB.SILVER.PROMOTIONS_CLEAN p
LEFT JOIN ANYCOMPANY_LAB.SILVER.FINANCIAL_TRANSACTIONS t
    ON  t.transaction_date BETWEEN p.start_date AND p.end_date
    AND t.region           = p.region
    AND t.transaction_type = 'Sale'
GROUP BY 1, 2, 3, 4, 5, 6
ORDER BY ca_genere DESC NULLS LAST
LIMIT 15
"""

SQL_DURATION = """
SELECT
    p.product_category,
    ROUND(AVG(DATEDIFF('day', p.start_date, p.end_date)), 1) AS duree_moy_jours,
    ROUND(AVG(p.discount_percentage)*100, 1)                 AS remise_moyenne_pct,
    ROUND(SUM(t.amount), 2)                                  AS ca_genere
FROM ANYCOMPANY_LAB.SILVER.PROMOTIONS_CLEAN p
LEFT JOIN ANYCOMPANY_LAB.SILVER.FINANCIAL_TRANSACTIONS t
    ON  t.transaction_date BETWEEN p.start_date AND p.end_date
    AND t.transaction_type = 'Sale'
GROUP BY 1
ORDER BY duree_moy_jours DESC
"""

SQL_TIMELINE = """
SELECT
    DATE_TRUNC('month', p.start_date)            AS mois,
    p.product_category,
    COUNT(DISTINCT p.promotion_id)               AS nb_promos,
    ROUND(AVG(p.discount_percentage)*100, 1)     AS remise_moy_pct
FROM ANYCOMPANY_LAB.SILVER.PROMOTIONS_CLEAN p
GROUP BY 1, 2
ORDER BY 1
"""

df_sensitivity = run_query(conn, SQL_SENSITIVITY)
df_region      = run_query(conn, SQL_REGION)
df_top_promos  = run_query(conn, SQL_TOP_PROMOS)
df_duration    = run_query(conn, SQL_DURATION)
df_timeline    = run_query(conn, SQL_TIMELINE)

for df in [df_sensitivity, df_region, df_top_promos, df_duration, df_timeline]:
    df.columns = [c.lower() for c in df.columns]

# ─────────────────────────────────────────────────────────────
# KPI ROW
# ─────────────────────────────────────────────────────────────
total_promos    = int(df_sensitivity["nb_promos_lancees"].sum()) if not df_sensitivity.empty else 0
total_ca_promo  = df_sensitivity["ca_total_genere"].sum()        if not df_sensitivity.empty else 0
avg_discount    = df_sensitivity["remise_moyenne_pct"].mean()    if not df_sensitivity.empty else 0
top_cat         = df_sensitivity.iloc[0]["product_category"]     if not df_sensitivity.empty else "N/A"

k1, k2, k3, k4 = st.columns(4)
k1.metric("🎁 Promotions lancées",        f"{total_promos:,}")
k2.metric("💰 CA généré sous promo",       f"${total_ca_promo:,.0f}")
k3.metric("🔖 Remise moyenne",            f"{avg_discount:.1f}%")
k4.metric("🏆 Catégorie la + sensible",   top_cat)

st.divider()

# ─────────────────────────────────────────────────────────────
# ROW 1 – Sensibilité par catégorie + Répartition géographique
# ─────────────────────────────────────────────────────────────
col1, col2 = st.columns(2)

with col1:
    st.subheader("📦 Sensibilité des catégories aux promotions")
    if not df_sensitivity.empty:
        fig = px.scatter(
            df_sensitivity,
            x="remise_moyenne_pct", y="ca_total_genere",
            size="nb_ventes_totales", color="product_category",
            hover_name="product_category",
            labels={
                "remise_moyenne_pct": "Remise moyenne (%)",
                "ca_total_genere":    "CA généré (USD)",
                "nb_ventes_totales":  "Nb ventes",
            },
            size_max=60,
        )
        fig.update_layout(margin=dict(t=20, b=20))
        st.plotly_chart(fig, use_container_width=True)
        st.caption("Taille = nombre de ventes | Position = efficacité de la remise")

with col2:
    st.subheader("🌍 CA généré sous promotion par région")
    if not df_region.empty:
        fig = px.bar(
            df_region, x="ca_sous_promo", y="region", orientation="h",
            color="remise_moyenne_pct",
            color_continuous_scale="YlOrRd",
            labels={"ca_sous_promo": "CA (USD)", "region": "Région", "remise_moyenne_pct": "Remise moy. (%)"},
            text="nb_promos",
        )
        fig.update_traces(texttemplate="  %{text} promos", textposition="inside")
        fig.update_layout(margin=dict(t=20, b=20))
        st.plotly_chart(fig, use_container_width=True)

# ─────────────────────────────────────────────────────────────
# ROW 2 – Durée des promos + Timeline
# ─────────────────────────────────────────────────────────────
col3, col4 = st.columns(2)

with col3:
    st.subheader("⏱️ Durée moyenne des promotions vs CA généré")
    if not df_duration.empty:
        fig = px.bar(
            df_duration, x="product_category", y="duree_moy_jours",
            color="remise_moyenne_pct", color_continuous_scale="Tealgrn",
            labels={
                "product_category": "Catégorie",
                "duree_moy_jours":  "Durée moyenne (jours)",
                "remise_moyenne_pct": "Remise moy. (%)",
            },
        )
        fig.update_layout(margin=dict(t=20, b=20), xaxis_tickangle=-30)
        st.plotly_chart(fig, use_container_width=True)

with col4:
    st.subheader("📅 Nombre de promotions actives par mois")
    if not df_timeline.empty:
        fig = px.bar(
            df_timeline, x="mois", y="nb_promos", color="product_category",
            labels={"mois": "Mois", "nb_promos": "Nb promotions", "product_category": "Catégorie"},
            barmode="stack",
        )
        fig.update_layout(margin=dict(t=20, b=20))
        st.plotly_chart(fig, use_container_width=True)

# ─────────────────────────────────────────────────────────────
# TOP PROMOTIONS TABLE
# ─────────────────────────────────────────────────────────────
st.subheader("🏆 Top 15 promotions par CA généré")
if not df_top_promos.empty:
    st.dataframe(
        df_top_promos.style.background_gradient(subset=["ca_genere"], cmap="Blues"),
        use_container_width=True,
    )

with st.expander("🔍 Données – Sensibilité par catégorie"):
    st.dataframe(df_sensitivity, use_container_width=True)

st.caption("AnyCompany Food & Beverage – MBAESG 2026 | Données : Snowflake ANYCOMPANY_LAB.SILVER")
