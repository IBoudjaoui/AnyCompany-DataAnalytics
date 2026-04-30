# ============================================================
# PROJET    : AnyCompany Food & Beverage – Data Analytics
# FICHIER   : streamlit/marketing_roi.py
# AUTEUR    : Équipe Data Analytics – MBAESG 2026
# DESCRIPTION : Phase 2 – Tableau de bord ROI Marketing
#               Performance des campagnes, ROI par type/audience,
#               lien campagnes ↔ ventes, avis clients.
# DÉPENDANCES : pip install streamlit snowflake-connector-python pandas plotly
# ============================================================

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

# ── Page config ──────────────────────────────────────────────
st.set_page_config(
    page_title="AnyCompany – Marketing ROI",
    page_icon="📣",
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
st.title("📣 AnyCompany Food & Beverage – Marketing ROI")
st.caption("Phase 2 – Performance des campagnes & ROI marketing | Données : schéma SILVER")
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
SQL_CAMPAIGN_TYPE = """
SELECT
    campaign_type,
    COUNT(*)                                         AS nb_campagnes,
    ROUND(SUM(budget), 2)                            AS budget_total,
    ROUND(AVG(budget), 2)                            AS budget_moyen,
    ROUND(SUM(reach), 0)                             AS audience_totale,
    ROUND(AVG(conversion_rate)*100, 2)               AS taux_conv_moy_pct,
    ROUND(AVG(CASE WHEN reach*conversion_rate > 0
          THEN budget / (reach*conversion_rate) END), 2) AS cout_par_conversion
FROM ANYCOMPANY_LAB.SILVER.MARKETING_CAMPAIGNS
GROUP BY 1
ORDER BY taux_conv_moy_pct DESC
"""

SQL_CAMPAIGN_CAT = """
SELECT
    product_category,
    COUNT(*)                                         AS nb_campagnes,
    ROUND(SUM(budget), 2)                            AS budget_total,
    ROUND(AVG(conversion_rate)*100, 2)               AS taux_conv_moy_pct,
    ROUND(SUM(reach), 0)                             AS audience_totale,
    ROUND(AVG(CASE WHEN reach*conversion_rate > 0
          THEN budget / (reach*conversion_rate) END), 2) AS cout_par_conversion
FROM ANYCOMPANY_LAB.SILVER.MARKETING_CAMPAIGNS
GROUP BY 1
ORDER BY taux_conv_moy_pct DESC
"""

SQL_AUDIENCE = """
SELECT
    target_audience,
    COUNT(*)                                         AS nb_campagnes,
    ROUND(AVG(conversion_rate)*100, 2)               AS taux_conv_moy_pct,
    ROUND(AVG(budget), 2)                            AS budget_moyen,
    ROUND(AVG(CASE WHEN reach*conversion_rate > 0
          THEN budget / (reach*conversion_rate) END), 2) AS cout_par_conversion
FROM ANYCOMPANY_LAB.SILVER.MARKETING_CAMPAIGNS
GROUP BY 1
ORDER BY taux_conv_moy_pct DESC
"""

SQL_REGION = """
SELECT
    region,
    COUNT(*)                                         AS nb_campagnes,
    ROUND(SUM(budget), 2)                            AS budget_total,
    ROUND(AVG(conversion_rate)*100, 2)               AS taux_conv_moy_pct,
    ROUND(SUM(reach), 0)                             AS audience_totale
FROM ANYCOMPANY_LAB.SILVER.MARKETING_CAMPAIGNS
GROUP BY 1
ORDER BY taux_conv_moy_pct DESC
"""

SQL_TOP10 = """
SELECT
    campaign_id,
    campaign_name,
    campaign_type,
    product_category,
    target_audience,
    region,
    ROUND(budget, 2)                                 AS budget,
    reach,
    ROUND(conversion_rate*100, 2)                    AS taux_conv_pct,
    ROUND(CASE WHEN reach*conversion_rate > 0
          THEN budget / (reach*conversion_rate) END, 2) AS cout_par_conversion,
    ROUND(reach * conversion_rate, 0)                AS conversions_estimees
FROM ANYCOMPANY_LAB.SILVER.MARKETING_CAMPAIGNS
ORDER BY conversion_rate DESC
LIMIT 10
"""

SQL_ANNUAL = """
SELECT
    DATE_PART('year', start_date)                    AS annee,
    COUNT(*)                                         AS nb_campagnes,
    ROUND(SUM(budget), 2)                            AS budget_total,
    ROUND(AVG(conversion_rate)*100, 2)               AS taux_conv_moy_pct
FROM ANYCOMPANY_LAB.SILVER.MARKETING_CAMPAIGNS
GROUP BY 1
ORDER BY 1
"""

SQL_COMBO = """
SELECT
    campaign_type,
    target_audience,
    COUNT(*)                                         AS nb_campagnes,
    ROUND(AVG(conversion_rate)*100, 2)               AS taux_conv_moy_pct,
    ROUND(AVG(CASE WHEN reach*conversion_rate > 0
          THEN budget / (reach*conversion_rate) END), 2) AS cout_par_conversion
FROM ANYCOMPANY_LAB.SILVER.MARKETING_CAMPAIGNS
GROUP BY 1, 2
ORDER BY taux_conv_moy_pct DESC
LIMIT 15
"""

SQL_REVIEWS = """
SELECT
    product_category,
    COUNT(*)                                         AS nb_avis,
    ROUND(AVG(rating), 2)                            AS note_moyenne,
    SUM(CASE WHEN rating >= 4 THEN 1 ELSE 0 END)    AS avis_positifs,
    SUM(CASE WHEN rating <= 2 THEN 1 ELSE 0 END)    AS avis_negatifs
FROM ANYCOMPANY_LAB.SILVER.PRODUCT_REVIEWS
GROUP BY 1
ORDER BY note_moyenne DESC
"""

SQL_LOGISTICS = """
SELECT
    destination_region,
    shipping_method,
    COUNT(*)                                               AS nb_livraisons,
    SUM(CASE WHEN status = 'Delivered' THEN 1 ELSE 0 END) AS livrees,
    SUM(CASE WHEN status = 'Returned'  THEN 1 ELSE 0 END) AS retournees,
    ROUND(AVG(DATEDIFF('day', ship_date, estimated_delivery)), 1) AS delai_moyen_jours
FROM ANYCOMPANY_LAB.SILVER.LOGISTICS_AND_SHIPPING
GROUP BY 1, 2
ORDER BY destination_region
"""

df_type     = run_query(conn, SQL_CAMPAIGN_TYPE)
df_cat      = run_query(conn, SQL_CAMPAIGN_CAT)
df_audience = run_query(conn, SQL_AUDIENCE)
df_region   = run_query(conn, SQL_REGION)
df_top10    = run_query(conn, SQL_TOP10)
df_annual   = run_query(conn, SQL_ANNUAL)
df_combo    = run_query(conn, SQL_COMBO)
df_reviews  = run_query(conn, SQL_REVIEWS)
df_logi     = run_query(conn, SQL_LOGISTICS)

for df in [df_type, df_cat, df_audience, df_region, df_top10, df_annual, df_combo, df_reviews, df_logi]:
    df.columns = [c.lower() for c in df.columns]

# ─────────────────────────────────────────────────────────────
# KPI ROW
# ─────────────────────────────────────────────────────────────
total_budget    = df_type["budget_total"].sum()      if not df_type.empty else 0
total_campaigns = int(df_type["nb_campagnes"].sum()) if not df_type.empty else 0
avg_conv_rate   = df_type["taux_conv_moy_pct"].mean()if not df_type.empty else 0
best_type       = df_type.iloc[0]["campaign_type"]   if not df_type.empty else "N/A"

k1, k2, k3, k4 = st.columns(4)
k1.metric("📋 Campagnes totales",              f"{total_campaigns:,}")
k2.metric("💸 Budget marketing total",          f"${total_budget:,.0f}")
k3.metric("🎯 Taux de conversion moyen",        f"{avg_conv_rate:.2f}%")
k4.metric("🏆 Meilleur type de campagne",       best_type)

st.divider()

# ─────────────────────────────────────────────────────────────
# SECTION 1 – Performance par type de campagne
# ─────────────────────────────────────────────────────────────
st.subheader("📊 Performance par type de campagne")
col1, col2 = st.columns(2)

with col1:
    if not df_type.empty:
        fig = px.bar(
            df_type, x="campaign_type", y="taux_conv_moy_pct",
            color="taux_conv_moy_pct", color_continuous_scale="Teal",
            labels={"campaign_type": "Type", "taux_conv_moy_pct": "Taux conversion (%)"},
            text="taux_conv_moy_pct",
        )
        fig.update_traces(texttemplate="%{text}%", textposition="outside")
        fig.update_layout(margin=dict(t=20, b=20), coloraxis_showscale=False,
                          title="Taux de conversion moyen par type")
        st.plotly_chart(fig, use_container_width=True)

with col2:
    if not df_type.empty:
        fig = px.bar(
            df_type, x="campaign_type", y="cout_par_conversion",
            color="budget_total", color_continuous_scale="Reds_r",
            labels={"campaign_type": "Type", "cout_par_conversion": "Coût/conversion (USD)"},
            text="cout_par_conversion",
        )
        fig.update_traces(texttemplate="$%{text}", textposition="outside")
        fig.update_layout(margin=dict(t=20, b=20), coloraxis_showscale=False,
                          title="Coût par conversion moyen")
        st.plotly_chart(fig, use_container_width=True)

# ─────────────────────────────────────────────────────────────
# SECTION 2 – Audience & Catégorie produit
# ─────────────────────────────────────────────────────────────
col3, col4 = st.columns(2)

with col3:
    st.subheader("👥 Performance par audience cible")
    if not df_audience.empty:
        fig = px.bar(
            df_audience, x="taux_conv_moy_pct", y="target_audience", orientation="h",
            color="cout_par_conversion", color_continuous_scale="Oranges_r",
            labels={"taux_conv_moy_pct": "Taux conv. (%)", "target_audience": "Audience",
                    "cout_par_conversion": "Coût/conv. (USD)"},
            text="taux_conv_moy_pct",
        )
        fig.update_traces(texttemplate="%{text}%", textposition="outside")
        fig.update_layout(margin=dict(t=20, b=20))
        st.plotly_chart(fig, use_container_width=True)

with col4:
    st.subheader("📦 Performance par catégorie de produit")
    if not df_cat.empty:
        fig = px.scatter(
            df_cat, x="budget_total", y="taux_conv_moy_pct",
            size="audience_totale", color="product_category",
            hover_name="product_category",
            labels={"budget_total": "Budget total (USD)", "taux_conv_moy_pct": "Taux conv. (%)"},
            size_max=50,
        )
        fig.update_layout(margin=dict(t=20, b=20))
        st.plotly_chart(fig, use_container_width=True)
        st.caption("Taille = audience totale touchée")

# ─────────────────────────────────────────────────────────────
# SECTION 3 – Évolution annuelle + Heatmap combo
# ─────────────────────────────────────────────────────────────
col5, col6 = st.columns(2)

with col5:
    st.subheader("📈 Évolution annuelle budget & conversion")
    if not df_annual.empty:
        fig = go.Figure()
        fig.add_bar(x=df_annual["annee"], y=df_annual["budget_total"],
                    name="Budget total (USD)", marker_color="#4682B4")
        fig.add_scatter(x=df_annual["annee"], y=df_annual["taux_conv_moy_pct"],
                        name="Taux conv. moy. (%)", mode="lines+markers",
                        yaxis="y2", line=dict(color="orange", width=2))
        fig.update_layout(
            yaxis=dict(title="Budget (USD)"),
            yaxis2=dict(title="Taux conversion (%)", overlaying="y", side="right"),
            legend=dict(orientation="h"),
            margin=dict(t=20, b=20),
        )
        st.plotly_chart(fig, use_container_width=True)

with col6:
    st.subheader("🔥 Matrice : Type × Audience (taux conversion)")
    if not df_combo.empty:
        pivot = df_combo.pivot_table(
            index="target_audience", columns="campaign_type",
            values="taux_conv_moy_pct", aggfunc="mean"
        )
        fig = px.imshow(
            pivot, text_auto=".2f",
            color_continuous_scale="RdYlGn",
            labels={"color": "Taux conv. (%)"},
            aspect="auto",
        )
        fig.update_layout(margin=dict(t=20, b=20))
        st.plotly_chart(fig, use_container_width=True)

# ─────────────────────────────────────────────────────────────
# SECTION 4 – Avis clients & Logistique
# ─────────────────────────────────────────────────────────────
col7, col8 = st.columns(2)

with col7:
    st.subheader("⭐ Note moyenne des produits par catégorie")
    if not df_reviews.empty:
        fig = px.bar(
            df_reviews, x="note_moyenne", y="product_category", orientation="h",
            color="note_moyenne", color_continuous_scale="RdYlGn",
            range_color=[1, 5],
            labels={"note_moyenne": "Note /5", "product_category": "Catégorie"},
            text="note_moyenne",
        )
        fig.update_traces(texttemplate="%{text:.2f} ⭐", textposition="outside")
        fig.update_layout(margin=dict(t=20, b=20), coloraxis_showscale=False)
        st.plotly_chart(fig, use_container_width=True)

with col8:
    st.subheader("🚚 Délai moyen de livraison par région & méthode")
    if not df_logi.empty:
        fig = px.bar(
            df_logi, x="destination_region", y="delai_moyen_jours",
            color="shipping_method", barmode="group",
            labels={"destination_region": "Région", "delai_moyen_jours": "Délai moyen (jours)",
                    "shipping_method": "Méthode"},
        )
        fig.update_layout(margin=dict(t=20, b=20), xaxis_tickangle=-30)
        st.plotly_chart(fig, use_container_width=True)

# ─────────────────────────────────────────────────────────────
# TOP 10 TABLE
# ─────────────────────────────────────────────────────────────
st.subheader("🏆 Top 10 campagnes (meilleur taux de conversion)")
if not df_top10.empty:
    st.dataframe(
        df_top10.style.background_gradient(subset=["taux_conv_pct"], cmap="Greens"),
        use_container_width=True,
    )

with st.expander("🔍 Données – Performance par région"):
    st.dataframe(df_region, use_container_width=True)

st.caption("AnyCompany Food & Beverage – MBAESG 2026 | Données : Snowflake ANYCOMPANY_LAB.SILVER")
