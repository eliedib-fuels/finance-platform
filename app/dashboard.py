"""
Finance Platform — Streamlit Dashboard
AD50 Management P&L view — kUSD — with drill-down
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))
from scripts.db import query

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Finance Intelligence",
    page_icon="◆",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@300;400;500&family=IBM+Plex+Sans:wght@300;400;500;600&display=swap');

html, body, [class*="css"] {
    font-family: 'IBM Plex Sans', sans-serif;
    background: #f8f9fb;
    color: #1a1f2e;
}
#MainMenu, footer, header { visibility: hidden; }
.block-container { padding: 1rem 2rem; }

/* Sidebar */
[data-testid="stSidebar"] {
    background: #1a1f2e;
}
[data-testid="stSidebar"] * { color: #94a3b8 !important; }
[data-testid="stSidebar"] label {
    font-size: 10px !important;
    text-transform: uppercase;
    letter-spacing: 0.1em;
    color: #475569 !important;
}

/* P&L Table */
.pl-table { width: 100%; border-collapse: collapse; font-family: 'IBM Plex Mono', monospace; font-size: 12px; }
.pl-table th {
    background: #1a1f2e; color: #94a3b8;
    padding: 6px 12px; text-align: right;
    font-size: 10px; letter-spacing: 0.08em;
    font-weight: 400; border-bottom: 2px solid #1a1f2e;
}
.pl-table th.col-label { text-align: left; min-width: 220px; }
.pl-table td {
    padding: 4px 12px; text-align: right;
    border-bottom: 1px solid #e8ecf0;
    color: #2d3748; white-space: nowrap;
}
.pl-table td.col-label {
    text-align: left; color: #4a5568;
    padding-left: 8px;
}
.pl-table td.col-label.indent { padding-left: 24px; color: #718096; }
.pl-table td.col-label.indent2 { padding-left: 40px; color: #718096; }
.pl-table tr.subtotal td {
    background: #edf2f7; font-weight: 600;
    color: #1a1f2e; border-top: 1px solid #cbd5e0;
    border-bottom: 1px solid #cbd5e0;
}
.pl-table tr.subtotal td.col-label { color: #1a1f2e; }
.pl-table tr.total td {
    background: #1a1f2e; color: #f1f5f9;
    font-weight: 600; border: none;
}
.pl-table tr.total td.col-label { color: #f1f5f9; }
.pl-table tr.pct td {
    color: #3b82f6; font-size: 11px;
    background: #f0f7ff; border-bottom: 1px solid #dbeafe;
}
.pl-table tr.pct td.col-label { color: #3b82f6; }
.pl-table tr:hover td { background: #f0f4f8 !important; }
.pl-table tr.subtotal:hover td { background: #dde6f0 !important; }
.pl-table tr.total:hover td { background: #1a1f2e !important; }
.neg { color: #64748b; }
.drill-btn {
    background: none; border: none; cursor: pointer;
    color: #3b82f6; font-size: 11px;
    text-decoration: underline; padding: 0;
    font-family: 'IBM Plex Mono', monospace;
}
.kpi-strip {
    display: flex; gap: 1rem; margin-bottom: 1.2rem;
}
.kpi-box {
    flex: 1; background: white;
    border: 1px solid #e2e8f0;
    border-top: 3px solid #3b82f6;
    border-radius: 6px; padding: 0.8rem 1rem;
}
.kpi-label {
    font-size: 9px; text-transform: uppercase;
    letter-spacing: 0.12em; color: #94a3b8;
    font-family: 'IBM Plex Mono', monospace;
    margin-bottom: 3px;
}
.kpi-val {
    font-size: 1.5rem; font-weight: 600;
    font-family: 'IBM Plex Mono', monospace;
    color: #1a1f2e; line-height: 1;
}
.kpi-val.neg { color: #dc2626; }
.kpi-val.pos { color: #059669; }
.section-hdr {
    font-size: 10px; text-transform: uppercase;
    letter-spacing: 0.15em; color: #3b82f6;
    font-family: 'IBM Plex Mono', monospace;
    border-bottom: 2px solid #3b82f6;
    padding-bottom: 4px; margin: 1rem 0 0.8rem;
}
</style>
""", unsafe_allow_html=True)


# ── Helpers ───────────────────────────────────────────────────────────────────

ENTITY_NAMES = {
    "00577": "USA",
    "01033": "Canada",
    "00569": "Panama",
    "00682": "Puerto Rico",
    "00684": "St Lucia",
}

PERIOD_NAMES = {
    1:"Jan",2:"Feb",3:"Mar",4:"Apr",5:"May",6:"Jun",
    7:"Jul",8:"Aug",9:"Sep",10:"Oct",11:"Nov",12:"Dec"
}

def kfmt(val, is_pct=False):
    """Format value in kUSD with brackets for negatives."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "—"
    if is_pct:
        return f"{val:.1f}%"
    k = val / 1000
    if k == 0:
        return "—"
    if k < 0:
        return f"({abs(k):,.0f})"
    return f"{k:,.0f}"

def get_periods():
    return query("""
        SELECT DISTINCT fiscal_year, fiscal_period
        FROM plan_data ORDER BY fiscal_year, fiscal_period
    """)

def get_pl_data(companies, year, period, bu_filter=None):
    """Get AD50 P&L data filtered optionally by business line BUs."""
    if not companies:
        return pd.DataFrame()
    co_ph = ",".join(["?" for _ in companies])
    bu_clause = ""
    bu_params = []
    if bu_filter:
        bu_ph = ",".join(["?" for _ in bu_filter])
        bu_clause = f"AND p.bu_code IN ({bu_ph})"
        bu_params = list(bu_filter)
    return query(f"""
        SELECT
            p.ad50_line,
            p.ad50_label,
            a.ad50_sort_key,
            a.ad50_parent,
            a.is_subtotal,
            a.is_ig,
            SUM(p.amount_usd) as amount_usd
        FROM plan_data p
        LEFT JOIN ad50_lines a ON p.ad50_line = a.ad50_line
        WHERE p.company IN ({co_ph})
          AND p.fiscal_year = ?
          AND p.fiscal_period = ?
          AND p.plan_type = 'ACTUAL'
          AND (a.line_type = 'financial' OR a.line_type IS NULL)
          AND p.ad50_line NOT IN ('16')
          {bu_clause}
        GROUP BY p.ad50_line, p.ad50_label,
                 a.ad50_sort_key, a.ad50_parent,
                 a.is_subtotal, a.is_ig
        ORDER BY COALESCE(a.ad50_sort_key, p.ad50_line)
    """, companies + [year, period] + bu_params)

def get_gl_by_account(companies, year, period, ad50_line):
    """Drill: get GL accounts for a specific AD50 line."""
    if not companies:
        return pd.DataFrame()
    co_ph = ",".join(["?" for _ in companies])
    return query(f"""
        SELECT
            g.account_code,
            g.account_desc,
            h.branch,
            h.region,
            SUM(g.amount_usd) as amount_usd
        FROM gl_transactions g
        LEFT JOIN org_hierarchy h
            ON g.bu_code = h.bu_code AND h.effective_to IS NULL
        LEFT JOIN account_master am ON g.account_code = am.account_code
        WHERE g.company IN ({co_ph})
          AND g.fiscal_year = ?
          AND g.fiscal_period = ?
          AND g.account_type = 'PL'
        GROUP BY g.account_code, g.account_desc, h.branch, h.region
        ORDER BY ABS(SUM(g.amount_usd)) DESC
        LIMIT 100
    """, companies + [year, period])

def get_gl_by_bu(companies, year, period):
    """Get P&L by BU with hierarchy."""
    if not companies:
        return pd.DataFrame()
    co_ph = ",".join(["?" for _ in companies])
    return query(f"""
        SELECT
            g.bu_code,
            h.branch,
            h.region,
            h.business,
            SUM(CASE WHEN g.account_type='PL' THEN g.amount_usd END) as pl_usd
        FROM gl_transactions g
        LEFT JOIN org_hierarchy h
            ON g.bu_code = h.bu_code AND h.effective_to IS NULL
        WHERE g.company IN ({co_ph})
          AND g.fiscal_year = ?
          AND g.fiscal_period = ?
        GROUP BY g.bu_code, h.branch, h.region, h.business
        ORDER BY ABS(SUM(CASE WHEN g.account_type='PL'
                         THEN g.amount_usd END)) DESC
    """, companies + [year, period])


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### ◆ Finance Platform")

    periods_df = get_periods()
    if periods_df.empty:
        st.error("No data loaded. Run: python run.py --ad50")
        st.stop()

    # Available periods
    period_opts = [
        f"{PERIOD_NAMES[int(r.fiscal_period)]} {int(r.fiscal_year)}"
        for _, r in periods_df.iterrows()
    ]
    period_lookup = {
        label: (int(row.fiscal_year), int(row.fiscal_period))
        for label, (_, row) in zip(period_opts, periods_df.iterrows())
    }

    # YTD or single period
    view_mode = st.radio("View", ["Single Period", "YTD"], horizontal=True)

    if view_mode == "Single Period":
        sel_label = st.selectbox("Period", period_opts,
                                  index=len(period_opts)-1)
        sel_year, sel_period = period_lookup[sel_label]
        col_header = sel_label
        # For single period, just one column
        periods_to_show = [(sel_year, sel_period,
                            PERIOD_NAMES[sel_period])]
    else:
        # YTD — show all periods up to selected
        sel_label = st.selectbox("YTD Through", period_opts,
                                  index=len(period_opts)-1)
        sel_year, sel_period = period_lookup[sel_label]
        col_header = f"YTD {PERIOD_NAMES[sel_period]} {sel_year}"
        periods_to_show = [
            (int(r.fiscal_year), int(r.fiscal_period),
             PERIOD_NAMES[int(r.fiscal_period)])
            for _, r in periods_df.iterrows()
            if int(r.fiscal_year) == sel_year
            and int(r.fiscal_period) <= sel_period
        ]

    st.divider()

    # Entity filter
    all_cos = [r for r in ["00577","01033","00569","00682","00684"]
               if query(f"SELECT 1 FROM plan_data WHERE company=? LIMIT 1",
                        (r,)).shape[0] > 0]
    co_labels = {f"{ENTITY_NAMES.get(c,c)} ({c})": c for c in all_cos}
    sel_labels = st.multiselect("Entities",
                                 list(co_labels.keys()),
                                 default=list(co_labels.keys()))
    sel_companies = [co_labels[l] for l in sel_labels]

    st.divider()

    # Hierarchy filter
    st.markdown("**Business Line**")
    sel_business_line = st.radio(
        "Business Line",
        ["Trade & OCM", "Upstream & SAM"],
        label_visibility="collapsed"
    )
    BUSINESS_MAP = {
        "Trade & OCM":    ["Trade & OCM", "Trade"],
        "Upstream & SAM": ["Upstream & SAM", "Upstream"],
    }
    sel_business = BUSINESS_MAP[sel_business_line]

    # Get BU codes for selected business line
    bus_ph = ",".join(["?" for _ in sel_business])
    active_bus = query(f"""
        SELECT DISTINCT bu_code FROM org_hierarchy
        WHERE business IN ({bus_ph})
        AND effective_to IS NULL
    """, sel_business)
    active_bu_set = set(active_bus["bu_code"].tolist()) \
        if not active_bus.empty else set()

    regions = query("""
        SELECT DISTINCT region FROM org_hierarchy
        WHERE effective_to IS NULL AND region IS NOT NULL
        ORDER BY region
    """)
    sel_region = st.multiselect(
        "Region",
        regions["region"].tolist() if not regions.empty else []
    )


# ── Filter companies by hierarchy if needed ───────────────────────────────────
# (hierarchy filter applies to GL drill-down, not AD50 which is already by BU)

# ── Main tabs ─────────────────────────────────────────────────────────────────
tab_pl, tab_bu, tab_line, tab_gl, tab_valid = st.tabs([
    "P&L  AD50", "BY BU / BRANCH", "LINE BREAKDOWN", "GL DETAIL", "VALIDATION"
])


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 1 — P&L AD50 view
# ═══════════════════════════════════════════════════════════════════════════════
with tab_pl:

    if not sel_companies:
        st.warning("Select at least one entity in the sidebar.")
        st.stop()

    # ── Get data for all requested periods ────────────────────────────────────
    period_data = {}
    for yr, per, lbl in periods_to_show:
        df = get_pl_data(sel_companies, yr, per,
                         bu_filter=active_bu_set if active_bu_set else None)
        if not df.empty:
            period_data[lbl] = df

    if not period_data:
        st.info("No P&L data for selected period/entities.")
        st.stop()

    # ── Build unified line list ────────────────────────────────────────────────
    all_lines = {}
    for lbl, df in period_data.items():
        for _, row in df.iterrows():
            code = str(row["ad50_line"]).strip()
            if code not in all_lines:
                all_lines[code] = {
                    "label": row["ad50_label"] or code,
                    "sort":  row["ad50_sort_key"] or code,
                    "is_subtotal": row["is_subtotal"],
                }

    # Sort lines
    sorted_lines = sorted(all_lines.items(), key=lambda x: x[1]["sort"])

    # ── Compute derived lines ─────────────────────────────────────────────────
    def get_amount(df, line):
        if df is None or df.empty:
            return 0.0
        row = df[df["ad50_line"] == line]
        return float(row["amount_usd"].sum()) if not row.empty else 0.0

    def sum_lines(df, lines):
        if df is None or df.empty:
            return 0.0
        return float(df[df["ad50_line"].isin(lines)]["amount_usd"].sum())

    # Columns: one per period + YTD if multiple
    col_labels = list(period_data.keys())
    if len(col_labels) > 1:
        col_labels_display = col_labels + ["YTD"]
    else:
        col_labels_display = col_labels

    def get_col_vals(line_code):
        """Get value for each display column."""
        vals = []
        for lbl in col_labels:
            vals.append(get_amount(period_data.get(lbl), line_code))
        if len(col_labels) > 1:
            vals.append(sum([get_amount(period_data.get(l), line_code)
                             for l in col_labels]))
        return vals

    def get_col_sum(lines):
        vals = []
        for lbl in col_labels:
            vals.append(sum_lines(period_data.get(lbl), lines))
        if len(col_labels) > 1:
            vals.append(sum([sum_lines(period_data.get(l), lines)
                             for l in col_labels]))
        return vals

    # ── KPI strip ─────────────────────────────────────────────────────────────
    ytd_df = list(period_data.values())[-1] if period_data else None

    # Use last period or sum all
    def ytd_sum(lines):
        return sum(
            sum_lines(period_data.get(l), lines)
            for l in col_labels
        ) if len(col_labels) > 1 else sum_lines(ytd_df, lines)

    production_lines = ["01", "02", "04", "05"]
    cost_lines       = ["07", "08", "09B", "09C", "09D", "09E",
                        "09F", "09G", "09H", "09I", "09J", "09K",
                        "09L", "09M"]
    func_lines       = ["13A","13B","13C","13D","13E","13F","13G"]

    prod_val  = ytd_sum(production_lines)
    costs_val = ytd_sum(cost_lines)
    gp_val    = prod_val + costs_val + ytd_sum(["10"])
    fc_val    = ytd_sum(func_lines)
    aop_val   = gp_val + fc_val

    gm_pct  = (gp_val  / prod_val * 100) if prod_val else 0
    aom_pct = (aop_val / prod_val * 100) if prod_val else 0

    kpis = [
        ("PRODUCTION", prod_val, False),
        ("GROSS PROFIT", gp_val, False),
        ("GROSS MARGIN", gm_pct, True),
        ("AOP", aop_val, False),
        ("AOM %", aom_pct, True),
    ]

    cols = st.columns(len(kpis))
    for col, (label, val, is_pct) in zip(cols, kpis):
        if is_pct:
            disp = f"{val:.1f}%"
            cls  = "pos" if val >= 0 else "neg"
        else:
            k = val / 1000
            disp = f"${k:,.0f}K" if abs(k) < 10000 \
                   else f"${k/1000:,.1f}M"
            cls = "pos" if val >= 0 else "neg"
        with col:
            st.markdown(f"""
            <div class="kpi-box">
                <div class="kpi-label">{label}</div>
                <div class="kpi-val {cls}">{disp}</div>
            </div>""", unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Build P&L table ───────────────────────────────────────────────────────
    # Define the full P&L structure explicitly
    PL_STRUCTURE = [
        # (line_code, display_label, row_type, indent, lines_to_sum)
        ("01",  "Billing",               "normal",   0,  None),
        ("02",  "WIP and UI",            "normal",   0,  None),
        ("04",  "IG Revenue",            "normal",   1,  None),
        ("05",  "IG Subcontracting",     "normal",   1,  None),
        ("06",  "Production",            "subtotal", 0,
                ["01","02","04","05"]),
        ("07",  "Labor Cost",            "normal",   0,  None),
        ("08",  "SubContracting",        "normal",   0,  None),
        ("09",  "Other Costs",           "normal",   0,
                ["09B","09C","09D","09E","09F","09G",
                 "09H","09I","09J","09K","09L","09M"]),
        ("09B", "Contract expense",      "detail",   1,  None),
        ("09C", "Lab Consumables",       "detail",   1,  None),
        ("09D", "Travel & living",       "detail",   1,  None),
        ("09E", "Depreciation",          "detail",   1,  None),
        ("09F", "Repairs & Maintenance", "detail",   1,  None),
        ("09G", "Rent & Utilities",      "detail",   1,  None),
        ("09H", "IT",                    "detail",   1,  None),
        ("09I", "Office Costs",          "detail",   1,  None),
        ("09J", "Bad debt",              "detail",   1,  None),
        ("09K", "Commercial Costs",      "detail",   1,  None),
        ("09L", "Professional costs",    "detail",   1,  None),
        ("09M", "Other costs",           "detail",   1,  None),
        ("10",  "Func. Neutralization",  "normal",   0,  None),
        ("11",  "Gross Profit",          "total",    0,
                ["01","02","04","05","07","08",
                 "09B","09C","09D","09E","09F","09G",
                 "09H","09I","09J","09K","09L","09M","10"]),
        ("12",  "Gross Margin %",        "pct",      0,
                ["01","02","04","05","07","08",
                 "09B","09C","09D","09E","09F","09G",
                 "09H","09I","09J","09K","09L","09M","10"]),
        ("13",  "Functional Costs",      "normal",   0,
                ["13A","13B","13C","13D","13E","13F","13G"]),
        ("13A", "Marketing & Sales",     "detail",   1,  None),
        ("13B", "Management",            "detail",   1,  None),
        ("13C", "Finance",               "detail",   1,  None),
        ("13D", "HR",                    "detail",   1,  None),
        ("13E", "IT",                    "detail",   1,  None),
        ("13F", "Fees",                  "detail",   1,  None),
        ("13G", "Legal",                 "detail",   1,  None),
        ("14",  "AOP",                   "total",    0,
                ["01","02","04","05","07","08",
                 "09B","09C","09D","09E","09F","09G",
                 "09H","09I","09J","09K","09L","09M","10",
                 "13A","13B","13C","13D","13E","13F","13G"]),
        ("15",  "AOM %",                 "pct",      0,
                ["01","02","04","05","07","08",
                 "09B","09C","09D","09E","09F","09G",
                 "09H","09I","09J","09K","09L","09M","10",
                 "13A","13B","13C","13D","13E","13F","13G"]),
    ]

    # Build HTML table
    ncols = len(col_labels_display)

    def cell(val, is_pct=False, row_type="normal"):
        """Format a table cell value."""
        if is_pct:
            if val is None or val == 0:
                return "—"
            return f"{val:.1f}%"
        if val is None or val == 0:
            return "—"
        k = val / 1000
        if k < 0:
            return f"({abs(k):,.0f})"
        return f"{k:,.0f}"

    html = ['<table class="pl-table">']

    # Header
    html.append('<thead><tr>')
    html.append(f'<th class="col-label">kUSD — {", ".join(sel_labels[:3])}</th>')
    for lbl in col_labels_display:
        html.append(f'<th>{lbl}</th>')
    html.append('</tr></thead><tbody>')

    # Rows
    show_detail = st.checkbox("Show 09 / 13 sub-lines", value=True)

    for (code, label, row_type, indent, sum_of) in PL_STRUCTURE:

        # Skip detail rows if collapsed
        if row_type == "detail" and not show_detail:
            continue

        # Calculate values
        if sum_of:
            # Computed subtotal/total
            raw_vals = get_col_sum(sum_of)
        else:
            raw_vals = get_col_vals(code)

        # For % rows — divide by production
        if row_type == "pct":
            prod_vals = get_col_sum(production_lines)
            display_vals = []
            for v, p in zip(raw_vals, prod_vals):
                display_vals.append((v / p * 100) if p else 0)
            is_pct = True
        else:
            display_vals = raw_vals
            is_pct = False

        # Row CSS class
        css = {"normal": "", "detail": "",
               "subtotal": "subtotal", "total": "total",
               "pct": "pct"}.get(row_type, "")

        # Indent class
        indent_cls = ["col-label",
                      "col-label indent",
                      "col-label indent2"][min(indent, 2)]

        html.append(f'<tr class="{css}">')
        html.append(f'<td class="{indent_cls}">{code}&nbsp;&nbsp;{label}</td>')

        for v in display_vals:
            html.append(f'<td>{cell(v, is_pct, row_type)}</td>')

        html.append('</tr>')

    html.append('</tbody></table>')

    st.markdown("\n".join(html), unsafe_allow_html=True)

    # ── Drill-down section ────────────────────────────────────────────────────
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown('<div class="section-hdr">Drill Down</div>',
                unsafe_allow_html=True)

    drill_line = st.selectbox(
        "Select AD50 line to drill into GL accounts",
        options=["—"] + [f"{c} — {l}" for c, l, *_ in PL_STRUCTURE
                         if c not in ("06","09","11","12","13",
                                      "14","15")],
    )

    if drill_line and drill_line != "—":
        drill_code = drill_line.split(" — ")[0].strip()

        # Lines to include in drill
        line_map = {r[0]: r[4] for r in PL_STRUCTURE if r[4]}
        if drill_code in line_map:
            drill_codes = line_map[drill_code]
        else:
            drill_codes = [drill_code]

        co_ph = ",".join(["?" for _ in sel_companies])
        # Sum across selected periods
        period_conds = " OR ".join(
            ["(fiscal_year=? AND fiscal_period=?)"]
            * len(periods_to_show)
        )
        period_params = []
        for yr, per, _ in periods_to_show:
            period_params += [yr, per]

        line_ph = ",".join(["?" for _ in drill_codes])

        df_drill = query(f"""
            SELECT
                g.account_code,
                g.account_desc,
                g.bu_code,
                h.branch,
                h.region,
                h.business,
                SUM(g.amount_usd) as amount_usd
            FROM gl_transactions g
            LEFT JOIN org_hierarchy h
                ON g.bu_code = h.bu_code
                AND h.effective_to IS NULL
            LEFT JOIN account_master am
                ON g.account_code = am.account_code
            WHERE g.company IN ({co_ph})
              AND ({period_conds})
              AND am.ad50_line_raw IN ({line_ph})
              AND g.account_type = 'PL'
            GROUP BY g.account_code, g.account_desc,
                     g.bu_code, h.branch, h.region, h.business
            ORDER BY ABS(SUM(g.amount_usd)) DESC
            LIMIT 200
        """, sel_companies + period_params + drill_codes)

        if df_drill.empty:
            # Fallback — no account_master mapping yet, show by BU
            df_drill = query(f"""
                SELECT
                    g.account_code,
                    g.account_desc,
                    g.bu_code,
                    h.branch,
                    h.region,
                    h.business,
                    SUM(g.amount_usd) as amount_usd
                FROM gl_transactions g
                LEFT JOIN org_hierarchy h
                    ON g.bu_code = h.bu_code
                    AND h.effective_to IS NULL
                WHERE g.company IN ({co_ph})
                  AND ({period_conds})
                  AND g.account_type = 'PL'
                GROUP BY g.account_code, g.account_desc,
                         g.bu_code, h.branch, h.region, h.business
                ORDER BY ABS(SUM(g.amount_usd)) DESC
                LIMIT 200
            """, sel_companies + period_params)
            st.caption("⚠️ Account master not loaded — "
                       "showing all P&L accounts")

        if not df_drill.empty:
            col1, col2 = st.columns(2)

            with col1:
                st.markdown("**By Account**")
                by_acct = df_drill.groupby(
                    ["account_code","account_desc"]
                )["amount_usd"].sum().reset_index()
                by_acct["kUSD"] = by_acct["amount_usd"].apply(
                    lambda v: kfmt(v)
                )
                st.dataframe(
                    by_acct[["account_code","account_desc","kUSD"]],
                    use_container_width=True,
                    height=300, hide_index=True
                )

            with col2:
                st.markdown("**By Branch**")
                by_branch = df_drill.groupby(
                    ["branch","region","business"]
                )["amount_usd"].sum().reset_index()
                by_branch["kUSD"] = by_branch["amount_usd"].apply(
                    lambda v: kfmt(v)
                )
                by_branch = by_branch.sort_values(
                    "amount_usd", key=abs, ascending=False
                )
                st.dataframe(
                    by_branch[["branch","region","business","kUSD"]],
                    use_container_width=True,
                    height=300, hide_index=True
                )


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 2 — BY BU / BRANCH
# ═══════════════════════════════════════════════════════════════════════════════
with tab_bu:

    st.markdown('<div class="section-hdr">P&L by Branch / Region</div>',
                unsafe_allow_html=True)

    if not sel_companies:
        st.warning("Select entities in sidebar")
    else:
        co_ph = ",".join(["?" for _ in sel_companies])
        period_conds = " OR ".join(
            ["(g.fiscal_year=? AND g.fiscal_period=?)"]
            * len(periods_to_show)
        )
        period_params = []
        for yr, per, _ in periods_to_show:
            period_params += [yr, per]

        bu_ph_f = ",".join(["?" for _ in active_bu_set]) \
            if active_bu_set else "NULL"
        bu_filter_clause = f"AND g.bu_code IN ({bu_ph_f})" \
            if active_bu_set else ""
        bu_filter_params = list(active_bu_set) if active_bu_set else []

        df_bu = query(f"""
            SELECT
                h.branch,
                h.region,
                h.business,
                g.bu_code,
                SUM(CASE WHEN g.account_type='PL'
                    THEN g.amount_usd END) as pl_usd,
                COUNT(*) as transactions
            FROM gl_transactions g
            LEFT JOIN org_hierarchy h
                ON g.bu_code = h.bu_code
                AND h.effective_to IS NULL
            WHERE g.company IN ({co_ph})
              AND ({period_conds})
              {bu_filter_clause}
            GROUP BY h.branch, h.region, h.business, g.bu_code
            ORDER BY ABS(SUM(CASE WHEN g.account_type='PL'
                          THEN g.amount_usd END)) DESC
        """, sel_companies + period_params + bu_filter_params)

        if sel_region and not df_bu.empty:
            df_bu = df_bu[df_bu["region"].isin(sel_region)]

        if df_bu.empty:
            st.info("No data")
        else:
            # Summary by region
            by_region = df_bu.groupby(
                ["business","region"]
            )["pl_usd"].sum().reset_index()
            by_region["kUSD"] = by_region["pl_usd"].apply(kfmt)
            by_region = by_region.sort_values(
                "pl_usd", key=abs, ascending=False
            )

            col1, col2 = st.columns([1, 2])

            with col1:
                st.markdown("**By Region**")
                st.dataframe(
                    by_region[["business","region","kUSD"]],
                    use_container_width=True,
                    height=400, hide_index=True
                )

            with col2:
                st.markdown("**By Branch**")
                by_branch = df_bu.groupby(
                    ["business","region","branch"]
                )["pl_usd"].sum().reset_index()
                by_branch["kUSD"] = by_branch["pl_usd"].apply(kfmt)
                by_branch = by_branch.sort_values(
                    "pl_usd", key=abs, ascending=False
                )

                # Bar chart
                fig = go.Figure(go.Bar(
                    x=by_branch["branch"],
                    y=by_branch["pl_usd"] / 1000,
                    marker_color=[
                        "#3b82f6" if v >= 0 else "#dc2626"
                        for v in by_branch["pl_usd"]
                    ],
                    text=[kfmt(v) for v in by_branch["pl_usd"]],
                    textposition="outside",
                    textfont=dict(size=9, family="IBM Plex Mono"),
                ))
                fig.update_layout(
                    paper_bgcolor="white",
                    plot_bgcolor="#f8f9fb",
                    font=dict(family="IBM Plex Mono",
                              color="#4a5568", size=10),
                    margin=dict(l=10, r=10, t=10, b=80),
                    height=380,
                    showlegend=False,
                    xaxis=dict(tickangle=-45,
                               gridcolor="#e8ecf0"),
                    yaxis=dict(gridcolor="#e8ecf0",
                               title="kUSD"),
                )
                st.plotly_chart(fig, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 3 — LINE BREAKDOWN (any AD50 line by BU)
# ═══════════════════════════════════════════════════════════════════════════════
with tab_line:

    st.markdown('<div class="section-hdr">AD50 Line Breakdown by BU</div>',
                unsafe_allow_html=True)

    if not sel_companies:
        st.warning("Select entities in sidebar")
    else:
        # Line selector — all financial AD50 lines
        all_ad50 = query("""
            SELECT ad50_line, ad50_label, ad50_sort_key
            FROM ad50_lines
            WHERE line_type = 'financial'
              AND is_subtotal = 0
            ORDER BY ad50_sort_key
        """)

        if all_ad50.empty:
            st.info("AD50 lines not loaded")
        else:
            line_options = {
                f"{r.ad50_line} — {r.ad50_label}": r.ad50_line
                for _, r in all_ad50.iterrows()
            }

            # Default to line 03 (Revenue)
            default_idx = next(
                (i for i, k in enumerate(line_options)
                 if "03" in k or "Revenue" in k), 0
            )

            sel_line_label = st.selectbox(
                "AD50 Line",
                list(line_options.keys()),
                index=default_idx
            )
            sel_line_code = line_options[sel_line_label]

            # Build period params
            co_ph = ",".join(["?" for _ in sel_companies])
            period_conds = " OR ".join(
                ["(p.fiscal_year=? AND p.fiscal_period=?)"]
                * len(periods_to_show)
            )
            period_params_l = []
            for yr, per, _ in periods_to_show:
                period_params_l += [yr, per]

            bu_ph_l = ",".join(["?" for _ in active_bu_set]) \
                if active_bu_set else "NULL"
            bu_clause_l = f"AND p.bu_code IN ({bu_ph_l})" \
                if active_bu_set else ""
            bu_params_l = list(active_bu_set) if active_bu_set else []

            # Get data by BU
            df_line = query(f"""
                SELECT
                    p.bu_code,
                    p.company,
                    h.branch,
                    h.region,
                    h.business,
                    SUM(p.amount_usd) as amount_usd
                FROM plan_data p
                LEFT JOIN org_hierarchy h
                    ON p.bu_code = h.bu_code
                    AND h.effective_to IS NULL
                WHERE p.company IN ({co_ph})
                  AND ({period_conds})
                  AND p.ad50_line = ?
                  AND p.plan_type = 'ACTUAL'
                  {bu_clause_l}
                GROUP BY p.bu_code, p.company,
                         h.branch, h.region, h.business
                ORDER BY ABS(SUM(p.amount_usd)) DESC
            """, sel_companies + period_params_l + [sel_line_code] + bu_params_l)

            if df_line.empty:
                st.info(f"No data for line {sel_line_code} "
                        f"in selected period/entities")
            else:
                # KPIs
                total = df_line["amount_usd"].sum()
                bu_count = df_line["bu_code"].nunique()
                top_bu = df_line.iloc[0]

                c1, c2, c3 = st.columns(3)
                with c1:
                    k = total / 1000
                    disp = f"({abs(k):,.0f})" if k < 0 \
                           else f"{k:,.0f}"
                    st.markdown(f"""
                    <div class="kpi-box">
                        <div class="kpi-label">TOTAL kUSD</div>
                        <div class="kpi-val {'neg' if k<0 else 'pos'}">{disp}</div>
                    </div>""", unsafe_allow_html=True)
                with c2:
                    st.markdown(f"""
                    <div class="kpi-box">
                        <div class="kpi-label">BUs WITH ACTIVITY</div>
                        <div class="kpi-val">{bu_count}</div>
                    </div>""", unsafe_allow_html=True)
                with c3:
                    top_name = top_bu.get("branch") or top_bu["bu_code"]
                    top_val = kfmt(top_bu["amount_usd"])
                    st.markdown(f"""
                    <div class="kpi-box">
                        <div class="kpi-label">LARGEST BU</div>
                        <div class="kpi-val" style="font-size:1rem">{top_name}</div>
                        <div class="kpi-label">{top_val} kUSD</div>
                    </div>""", unsafe_allow_html=True)

                st.markdown("<br>", unsafe_allow_html=True)

                col1, col2 = st.columns([3, 2])

                with col1:
                    # Bar chart by branch
                    df_branch = df_line.groupby(
                        ["branch","region"]
                    )["amount_usd"].sum().reset_index()
                    df_branch = df_branch.sort_values(
                        "amount_usd", ascending=True
                    )
                    df_branch = df_branch[
                        df_branch["amount_usd"].abs() > 0
                    ]

                    fig = go.Figure(go.Bar(
                        y=df_branch["branch"].fillna(
                            df_branch.get("region", "Unknown")),
                        x=df_branch["amount_usd"] / 1000,
                        orientation="h",
                        marker_color=[
                            "#3b82f6" if v >= 0 else "#dc2626"
                            for v in df_branch["amount_usd"]
                        ],
                        text=[kfmt(v) for v in df_branch["amount_usd"]],
                        textposition="outside",
                        textfont=dict(size=9, family="IBM Plex Mono"),
                    ))
                    fig.update_layout(
                        paper_bgcolor="white",
                        plot_bgcolor="#f8f9fb",
                        font=dict(family="IBM Plex Mono",
                                  color="#4a5568", size=10),
                        margin=dict(l=10, r=60, t=20, b=20),
                        height=max(300, len(df_branch) * 22),
                        showlegend=False,
                        xaxis=dict(gridcolor="#e8ecf0",
                                   title="kUSD"),
                        yaxis=dict(gridcolor="#e8ecf0"),
                        title=dict(
                            text=f"{sel_line_label} — by Branch",
                            font=dict(size=11, family="IBM Plex Mono",
                                      color="#4a5568")
                        )
                    )
                    st.plotly_chart(fig, use_container_width=True)

                with col2:
                    # Table by BU
                    st.markdown("**Detail by BU**")
                    df_line["kUSD"] = df_line["amount_usd"].apply(kfmt)
                    df_line["entity"] = df_line["company"].map(
                        lambda c: ENTITY_NAMES.get(c, c)
                    )
                    show = df_line[[
                        "bu_code", "branch", "region",
                        "entity", "kUSD"
                    ]].copy()
                    show.columns = ["BU", "Branch", "Region",
                                    "Entity", "kUSD"]
                    st.dataframe(
                        show,
                        use_container_width=True,
                        height=max(300, len(show) * 35 + 40),
                        hide_index=True
                    )

                # Region rollup
                st.markdown('<div class="section-hdr">By Region</div>',
                            unsafe_allow_html=True)
                df_region = df_line.groupby(
                    ["region","business"]
                )["amount_usd"].sum().reset_index()
                df_region["kUSD"] = df_region["amount_usd"].apply(kfmt)
                df_region["% of Total"] = df_region["amount_usd"].apply(
                    lambda v: f"{v/total*100:.1f}%" if total else "—"
                )
                df_region = df_region.sort_values(
                    "amount_usd", key=abs, ascending=False
                )
                st.dataframe(
                    df_region[["business","region","kUSD","% of Total"]],
                    use_container_width=True,
                    height=200,
                    hide_index=True
                )

                # Reconciliation note
                st.divider()
                st.markdown(
                    f"**Reconciliation:** AD50 line {sel_line_code} total = "
                    f"**{kfmt(total)} kUSD** across "
                    f"{bu_count} BUs / "
                    f"{df_line['company'].nunique()} entities"
                )


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 4 — GL DETAIL
# ═══════════════════════════════════════════════════════════════════════════════
with tab_gl:

    st.markdown('<div class="section-hdr">Transaction Detail</div>',
                unsafe_allow_html=True)

    if not sel_companies:
        st.warning("Select entities")
    else:
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            acct_type = st.selectbox("Type", ["All","PL","BS"])
        with c2:
            ledger_f = st.selectbox("Ledger", ["All","AA","GP","UE"])
        with c3:
            period_f = st.selectbox(
                "Period",
                ["All"] + [f"{PERIOD_NAMES[p]} {y}"
                           for y, p, _ in periods_to_show]
            )
        with c4:
            search = st.text_input("Search", placeholder="account / description")

        co_ph = ",".join(["?" for _ in sel_companies])
        period_conds = " OR ".join(
            ["(g.fiscal_year=? AND g.fiscal_period=?)"]
            * len(periods_to_show)
        )
        period_params_gl = []
        for yr, per, _ in periods_to_show:
            period_params_gl += [yr, per]

        extra = ""
        extra_params = []
        if acct_type != "All":
            extra += " AND g.account_type=?"
            extra_params.append(acct_type)
        if ledger_f != "All":
            extra += " AND g.ledger_type=?"
            extra_params.append(ledger_f)
        if period_f != "All":
            pname, pyr = period_f.split(" ")
            pnum = {v: k for k, v in PERIOD_NAMES.items()}[pname]
            extra = " AND g.fiscal_year=? AND g.fiscal_period=?"
            extra_params = [int(pyr), pnum]

        bu_ph_gl = ",".join(["?" for _ in active_bu_set]) \
            if active_bu_set else "NULL"
        bu_gl_clause = f"AND g.bu_code IN ({bu_ph_gl})" \
            if active_bu_set else ""
        bu_gl_params = list(active_bu_set) if active_bu_set else []

        df_tx = query(f"""
            SELECT
                g.company,
                g.ledger_type,
                g.fiscal_year || '/' ||
                    PRINTF('%02d', g.fiscal_period) as period,
                g.account_code,
                g.account_desc,
                g.account_type,
                g.bu_code,
                h.branch,
                h.region,
                g.document_type,
                CASE WHEN g.reversing_entry_code='R'
                     THEN '🔄' ELSE '' END as rev,
                g.explanation_alpha,
                ROUND(g.amount_usd/1000, 1) as kUSD,
                g.user_id
            FROM gl_transactions g
            LEFT JOIN org_hierarchy h
                ON g.bu_code = h.bu_code
                AND h.effective_to IS NULL
            WHERE g.company IN ({co_ph})
              AND ({period_conds})
              {extra}
              {bu_gl_clause}
            ORDER BY g.gl_date, g.account_code
            LIMIT 5000
        """, sel_companies + period_params_gl + extra_params + bu_gl_params)

        if search and not df_tx.empty:
            mask = (
                df_tx["explanation_alpha"].astype(str)
                .str.contains(search, case=False, na=False) |
                df_tx["account_code"].astype(str)
                .str.contains(search, case=False, na=False) |
                df_tx["account_desc"].astype(str)
                .str.contains(search, case=False, na=False)
            )
            df_tx = df_tx[mask]

        st.caption(f"{len(df_tx):,} rows (max 5,000)")

        if not df_tx.empty:
            st.dataframe(df_tx, use_container_width=True,
                         height=500, hide_index=True)
            st.download_button(
                "⬇ Export CSV",
                df_tx.to_csv(index=False),
                f"gl_detail_{sel_year}_{sel_period:02d}.csv",
                "text/csv"
            )


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 4 — VALIDATION
# ═══════════════════════════════════════════════════════════════════════════════
with tab_valid:

    st.markdown('<div class="section-hdr">Data Quality</div>',
                unsafe_allow_html=True)

    df_val = query("""
        SELECT run_date, check_name, dimension_value,
               db_total, difference, status
        FROM validation_log ORDER BY run_date DESC LIMIT 50
    """)

    if df_val.empty:
        st.info("Run: python run.py --validate")
    else:
        p = len(df_val[df_val["status"]=="PASS"])
        w = len(df_val[df_val["status"]=="WARN"])
        c1, c2, c3 = st.columns(3)
        c1.metric("✅ Passed", p)
        c2.metric("⚠️ Warnings", w)
        c3.metric("❌ Failed",
                  len(df_val[df_val["status"]=="FAIL"]))
        st.dataframe(df_val, use_container_width=True,
                     height=350, hide_index=True)

    st.markdown('<div class="section-hdr">Load History</div>',
                unsafe_allow_html=True)
    df_h = query("""
        SELECT load_date, file_type, source_file, company,
               fiscal_year, fiscal_period,
               rows_inserted, validation_status
        FROM load_history ORDER BY load_date DESC LIMIT 30
    """)
    if not df_h.empty:
        st.dataframe(df_h, use_container_width=True,
                     height=250, hide_index=True)
