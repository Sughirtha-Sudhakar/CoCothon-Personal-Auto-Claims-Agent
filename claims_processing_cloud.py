import streamlit as st
import pandas as pd
import json
import snowflake.connector


st.set_page_config(layout="wide")
st.write("App started")
st.write(st.secrets)

st.set_page_config(layout="wide")
st.write("App started")

# def get_connection():
#     try:
#         conn = snowflake.connector.connect(
#             user=st.secrets["snowflake_user"],
#             password=st.secrets["snowflake_password"],
#             account=st.secrets["snowflake_account"],
#             warehouse=st.secrets["snowflake_warehouse"],
#             database=st.secrets["snowflake_database"],
#             schema=st.secrets["snowflake_schema"]
#         )
#         return conn
#     except Exception as e:
#         st.error(f"Error connecting to Snowflake: {e}")
#         return None


# conn = get_connection()
# if conn is None:
    st.stop()
TABLE = "CLAIMS_AI_DB.CLAIMS.INSURANCE_CLAIMS_DEMO"



CUSTOM_CSS = """
<style>
    .main .block-container { padding-top: 1rem; }
    .kpi-card {
        background: linear-gradient(135deg, #0a1628 0%, #1a2744 100%);
        border: 1px solid #2a3f5f;
        border-radius: 10px;
        padding: 20px;
        text-align: center;
        color: white;
    }
    .kpi-card h2 { font-size: 2rem; margin: 0; color: #4fc3f7; }
    .kpi-card p { font-size: 0.85rem; margin: 5px 0 0 0; color: #90a4ae; }
    .badge-critical { background-color: #d32f2f; color: white; padding: 4px 12px; border-radius: 12px; font-weight: bold; }
    .badge-high { background-color: #f57c00; color: white; padding: 4px 12px; border-radius: 12px; font-weight: bold; }
    .badge-medium { background-color: #fbc02d; color: #333; padding: 4px 12px; border-radius: 12px; font-weight: bold; }
    .badge-low { background-color: #388e3c; color: white; padding: 4px 12px; border-radius: 12px; font-weight: bold; }
    .ai-panel {
        background: linear-gradient(135deg, #0d1b2a 0%, #1b2838 100%);
        border: 1px solid #2a3f5f;
        border-radius: 10px;
        padding: 20px;
        color: white;
    }
    .ai-panel h4 { color: #4fc3f7; }
    .action-box {
        background: #1a2744;
        border-left: 4px solid #4fc3f7;
        padding: 12px;
        border-radius: 4px;
        color: white;
        margin-top: 8px;
    }
    .rule-fired { color: #4caf50; font-weight: bold; }
    .rule-not-fired { color: #f44336; font-weight: bold; }
    .nav-header {
        background: linear-gradient(135deg, #0a1628 0%, #1a2744 100%);
        padding: 20px;
        border-radius: 10px;
        text-align: center;
        margin-bottom: 20px;
    }
    .nav-header h1 { color: #4fc3f7; font-size: 1.4rem; margin: 0; }
    .nav-header p { color: #90a4ae; font-size: 0.8rem; margin: 5px 0 0 0; font-style: italic; }
    div[data-testid="stSidebar"] { background: linear-gradient(180deg, #0a1628 0%, #162038 100%); }
    div[data-testid="stSidebar"] .stMarkdown h1,
    div[data-testid="stSidebar"] .stMarkdown h2,
    div[data-testid="stSidebar"] .stMarkdown h3,
    div[data-testid="stSidebar"] .stMarkdown p { color: white; }
</style>
"""
st.markdown(CUSTOM_CSS, unsafe_allow_html=True)

if "screen" not in st.session_state:
    st.session_state.screen = "queue"
if "selected_claim_id" not in st.session_state:
    st.session_state.selected_claim_id = None


def navigate(screen, claim_id=None):
    st.session_state.screen = screen
    if claim_id:
        st.session_state.selected_claim_id = claim_id


def run_query(sql):
    cur = conn.cursor()
    cur.execute(sql)
    return cur.fetch_pandas_all()


def execute_sql(sql):
    cur = conn.cursor()
    cur.execute(sql)


with st.sidebar:
    st.markdown("""
    <div class="nav-header">
        <h1>🛡️ Claims AI</h1>
        <p>Orchestrated AI — Empowering Adjusters<br>Accelerating Claims</p>
    </div>
    """, unsafe_allow_html=True)

    if st.button("📋 Claims Queue", use_container_width=True):
        navigate("queue")
    if st.button("🔍 Claim Detail", use_container_width=True, disabled=st.session_state.selected_claim_id is None):
        navigate("detail")
    if st.button("⚖️ Decision Console", use_container_width=True, disabled=st.session_state.selected_claim_id is None):
        navigate("decision")

    st.markdown("---")
    st.markdown("<p style='color:#90a4ae;font-size:0.75rem;text-align:center;'>Cortex-Powered Claims Processing</p>", unsafe_allow_html=True)


def get_triage_badge(category):
    if category == "Critical_Fraud_Suspected":
        return f'<span class="badge-critical">{category}</span>'
    elif category == "High_Escalate":
        return f'<span class="badge-high">{category}</span>'
    elif category == "Medium_Needs_Review":
        return f'<span class="badge-medium">{category}</span>'
    elif category == "Low_Standard_Review":
        return f'<span class="badge-low">{category}</span>'
    return str(category)


def get_triage_color(category):
    mapping = {
        "Critical_Fraud_Suspected": "#d32f2f",
        "High_Escalate": "#f57c00",
        "Medium_Needs_Review": "#fbc02d",
        "Low_Standard_Review": "#388e3c",
    }
    return mapping.get(category, "#666")


def screen_claims_queue():
    st.markdown("## 📋 Claims Queue — Pending Adjuster Review")

    try:
        df = run_query(f"""
            SELECT 
                CLAIM_ID, CLAIMANT_NAME, INCIDENT_TYPE, INCIDENT_SEVERITY,
                TOTAL_CLAIM_AMOUNT_USD, AI_FRAUD_SCORE, AI_TRIAGE_CATEGORY, 
                PROCESSING_STATUS
            FROM {TABLE}
            WHERE AI_FRAUD_SCORE IS NOT NULL
            ORDER BY AI_FRAUD_SCORE DESC
        """)
    except Exception as e:
        st.error(f"Error loading claims: {e}")
        return

    if df.empty:
        st.info("No claims found in the queue.")
        return

    total = len(df)
    critical = len(df[df["AI_TRIAGE_CATEGORY"] == "Critical_Fraud_Suspected"])
    high = len(df[df["AI_TRIAGE_CATEGORY"] == "High_Escalate"])
    pending = len(df[df["PROCESSING_STATUS"] == "Pending_Approval"])

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.markdown(f'<div class="kpi-card"><h2>{total}</h2><p>Total Claims Processed</p></div>', unsafe_allow_html=True)
    with c2:
        st.markdown(f'<div class="kpi-card"><h2>{critical}</h2><p>🔴 Critical Fraud Suspected</p></div>', unsafe_allow_html=True)
    with c3:
        st.markdown(f'<div class="kpi-card"><h2>{high}</h2><p>🟠 High Escalate</p></div>', unsafe_allow_html=True)
    with c4:
        st.markdown(f'<div class="kpi-card"><h2>{pending}</h2><p>🟡 Pending Approval</p></div>', unsafe_allow_html=True)

    st.markdown("---")

    fc1, fc2 = st.columns(2)
    triage_options = ["All"] + sorted(df["AI_TRIAGE_CATEGORY"].dropna().unique().tolist())
    incident_options = ["All"] + sorted(df["INCIDENT_TYPE"].dropna().unique().tolist())
    with fc1:
        sel_triage = st.selectbox("Filter by AI Triage Category", triage_options)
    with fc2:
        sel_incident = st.selectbox("Filter by Incident Type", incident_options)

    filtered = df.copy()
    if sel_triage != "All":
        filtered = filtered[filtered["AI_TRIAGE_CATEGORY"] == sel_triage]
    if sel_incident != "All":
        filtered = filtered[filtered["INCIDENT_TYPE"] == sel_incident]

    display_cols = [
        "CLAIM_ID", "CLAIMANT_NAME", "INCIDENT_TYPE", "INCIDENT_SEVERITY",
        "TOTAL_CLAIM_AMOUNT_USD", "AI_FRAUD_SCORE", "AI_TRIAGE_CATEGORY", "PROCESSING_STATUS"
    ]
    display_df = filtered[display_cols].reset_index(drop=True)

    if display_df.empty:
        st.warning("No claims match the selected filters.")
        return

    st.markdown(f"**Showing {len(display_df)} claims**")

    header_cols = st.columns([1, 1.5, 1.2, 1, 1.2, 0.8, 1.5, 1.2, 0.6])
    headers = ["Claim ID", "Claimant", "Incident Type", "Severity", "Amount (USD)", "Fraud Score", "Triage Category", "Status", "Action"]
    for col, h in zip(header_cols, headers):
        col.markdown(f"**{h}**")

    for idx, row in display_df.iterrows():
        cols = st.columns([1, 1.5, 1.2, 1, 1.2, 0.8, 1.5, 1.2, 0.6])
        cols[0].write(row["CLAIM_ID"])
        cols[1].write(row["CLAIMANT_NAME"])
        cols[2].write(row["INCIDENT_TYPE"])
        cols[3].write(row["INCIDENT_SEVERITY"])
        cols[4].write(f"${row['TOTAL_CLAIM_AMOUNT_USD']:,.2f}")
        cols[5].write(f"{row['AI_FRAUD_SCORE']:.2f}")
        cols[6].markdown(get_triage_badge(row["AI_TRIAGE_CATEGORY"]), unsafe_allow_html=True)
        cols[7].write(row["PROCESSING_STATUS"])
        if cols[8].button("📂", key=f"btn_{row['CLAIM_ID']}"):
            navigate("detail", row["CLAIM_ID"])
            st.rerun()


def screen_claim_detail():
    claim_id = st.session_state.selected_claim_id
    if not claim_id:
        st.warning("No claim selected. Please select a claim from the queue.")
        if st.button("← Back to Claims Queue"):
            navigate("queue")
            st.rerun()
        return

    st.markdown(f"## 🔍 Claim Detail — {claim_id}")

    try:
        df = run_query(f"SELECT * FROM {TABLE} WHERE CLAIM_ID = '{claim_id}'")
    except Exception as e:
        st.error(f"Error loading claim: {e}")
        return

    if df.empty:
        st.error(f"Claim {claim_id} not found.")
        return

    row = df.iloc[0]

    left_col, right_col = st.columns([1, 1])

    with left_col:
        st.markdown("### 📄 Claim Information")
        info_fields = {
            "Claimant Name": row["CLAIMANT_NAME"],
            "Incident Type": row["INCIDENT_TYPE"],
            "Collision Type": row["COLLISION_TYPE"],
            "Incident Severity": row["INCIDENT_SEVERITY"],
            "Incident Date": str(row["INCIDENT_DATE"]),
            "Incident State": row["INCIDENT_STATE"],
            "Incident City": row["INCIDENT_CITY"],
            "Auto Make": row["AUTO_MAKE"],
            "Auto Model": row["AUTO_MODEL"],
            "Auto Year": str(row["AUTO_YEAR"]),
            "Total Claim Amount": f"${row['TOTAL_CLAIM_AMOUNT_USD']:,.2f}",
            "Police Report Available": row["POLICE_REPORT_AVAILABLE"],
            "Witnesses": str(row["WITNESSES"]),
            "Bodily Injuries": str(row["BODILY_INJURIES"]),
        }
        for label, val in info_fields.items():
            st.markdown(f"**{label}:** {val}")

    with right_col:
        st.markdown('<div class="ai-panel">', unsafe_allow_html=True)
        st.markdown("### 🤖 AI Insights")

        fraud_score = float(row["AI_FRAUD_SCORE"]) if row["AI_FRAUD_SCORE"] is not None else 0.0
        st.markdown(f"**AI Fraud Score:** {fraud_score:.2f}")
        st.progress(min(fraud_score, 1.0))

        triage = row["AI_TRIAGE_CATEGORY"]
        st.markdown(f"**AI Triage Category:** {get_triage_badge(triage)}", unsafe_allow_html=True)

        st.markdown("**AI Initial Notes:**")
        st.text_area("", value=str(row["AI_INITIAL_NOTES"] or ""), height=120, disabled=True, key="ai_notes")

        st.markdown(f"""
        <div class="action-box">
            <strong>Recommended Action:</strong><br>{row["AI_RECOMMENDED_ACTION"] or "N/A"}
        </div>
        """, unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)

    st.markdown("---")

    with st.expander("📝 Claim Narrative", expanded=False):
        st.write(row["CLAIM_NARRATIVE"] or "No narrative available.")

    with st.expander("🚔 Police Report", expanded=False):
        st.write(row["POLICE_REPORT_TEXT"] or "No police report available.")

    st.markdown("---")
    st.markdown("### 🧠 AI Decision Path")
    decision_path = row["AI_DECISION_PATH"]
    if decision_path:
        try:
            if isinstance(decision_path, str):
                rules = json.loads(decision_path)
            else:
                rules = decision_path

            if isinstance(rules, list):
                hdr = st.columns([2, 1, 1])
                hdr[0].markdown("**Rule Name**")
                hdr[1].markdown("**Weight**")
                hdr[2].markdown("**Fired**")
                for rule in rules:
                    r_cols = st.columns([2, 1, 1])
                    r_cols[0].write(rule.get("rule_name", rule.get("rule", "Unknown")))
                    r_cols[1].write(str(rule.get("weight", rule.get("score", "N/A"))))
                    fired = rule.get("fired", rule.get("triggered", False))
                    if fired:
                        r_cols[2].markdown('<span class="rule-fired">✅</span>', unsafe_allow_html=True)
                    else:
                        r_cols[2].markdown('<span class="rule-not-fired">❌</span>', unsafe_allow_html=True)
            elif isinstance(rules, dict):
                for key, val in rules.items():
                    st.write(f"**{key}:** {val}")
            else:
                st.json(rules)
        except Exception:
            st.write(str(decision_path))
    else:
        st.info("No AI decision path available.")

    st.markdown("---")
    if st.button("⚖️ Proceed to Decision", type="primary", use_container_width=True):
        navigate("decision")
        st.rerun()

    if st.button("← Back to Claims Queue"):
        navigate("queue")
        st.rerun()


def screen_decision_console():
    claim_id = st.session_state.selected_claim_id
    if not claim_id:
        st.warning("No claim selected. Please select a claim from the queue.")
        if st.button("← Back to Claims Queue"):
            navigate("queue")
            st.rerun()
        return

    st.markdown(f"## ⚖️ Adjuster Decision Console — {claim_id}")

    try:
        df = run_query(f"SELECT * FROM {TABLE} WHERE CLAIM_ID = '{claim_id}'")
    except Exception as e:
        st.error(f"Error loading claim: {e}")
        return

    if df.empty:
        st.error(f"Claim {claim_id} not found.")
        return

    row = df.iloc[0]

    st.markdown("### Summary")
    sc1, sc2, sc3, sc4, sc5 = st.columns(5)
    sc1.metric("Claim ID", claim_id)
    sc2.metric("Claimant", row["CLAIMANT_NAME"])
    sc3.metric("Fraud Score", f"{float(row['AI_FRAUD_SCORE'] or 0):.2f}")
    sc4.markdown(f"**Triage**<br>{get_triage_badge(row['AI_TRIAGE_CATEGORY'])}", unsafe_allow_html=True)
    sc5.metric("AI Action", str(row["AI_RECOMMENDED_ACTION"] or "N/A")[:40])

    st.markdown("---")
    st.markdown("### Adjuster Decision Form")

    ai_action = str(row["AI_RECOMMENDED_ACTION"] or "")
    decision_options = [
        "Approve",
        "Reject",
        "Escalate_To_Senior_Adjuster",
        "Refer_To_Special_Investigations_Unit"
    ]

    with st.form("adjuster_form"):
        adjuster_id = st.text_input("Adjuster ID", placeholder="Enter your Adjuster ID")
        adjuster_decision = st.selectbox("Final Decision", decision_options)
        adjuster_remarks = st.text_area("Remarks", placeholder="Enter your assessment and notes...")

        ai_matches = adjuster_decision.lower().replace("_", " ") in ai_action.lower().replace("_", " ")
        show_override = not ai_matches

        override_ai = "No"
        override_reason = ""
        if show_override:
            st.warning("⚠️ Your decision differs from the AI recommendation.")
            override_ai = st.radio("Override AI Recommendation?", ["Yes", "No"], index=1, horizontal=True)
            if override_ai == "Yes":
                override_reason = st.text_area("Override Reason", placeholder="Explain why you are overriding the AI recommendation...")

        submitted = st.form_submit_button("Submit Decision", type="primary", use_container_width=True)

        if submitted:
            if not adjuster_id.strip():
                st.error("Please enter your Adjuster ID.")
            elif not adjuster_remarks.strip():
                st.error("Please enter your remarks.")
            else:
                try:
                    override_reason_escaped = override_reason.replace("'", "''")
                    adjuster_remarks_escaped = adjuster_remarks.replace("'", "''")

                    update_sql = f"""
                        UPDATE {TABLE}
                        SET ADJUSTER_ID = '{adjuster_id.strip()}',
                            ADJUSTER_FINAL_DECISION = '{adjuster_decision}',
                            ADJUSTER_REMARKS = '{adjuster_remarks_escaped}',
                            ADJUSTER_OVERRODE_AI = '{override_ai}',
                            ADJUSTER_OVERRIDE_REASON = '{override_reason_escaped}',
                            ADJUSTER_REVIEWED_AT = CURRENT_TIMESTAMP()
                        WHERE CLAIM_ID = '{claim_id}'
                    """
                    execute_sql(update_sql)

                    if override_ai == "Yes":
                        st.success(f"Decision submitted successfully for {claim_id}. Adjuster override logged.")
                    else:
                        st.success(f"Decision submitted successfully for {claim_id}.")
                except Exception as e:
                    st.error(f"Error submitting decision: {e}")

    if st.button("← Back to Claims Queue"):
        navigate("queue")
        st.rerun()


if st.session_state.screen == "queue":
    screen_claims_queue()
elif st.session_state.screen == "detail":
    screen_claim_detail()
elif st.session_state.screen == "decision":
    screen_decision_console()
