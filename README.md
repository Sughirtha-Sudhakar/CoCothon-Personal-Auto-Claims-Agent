# CoCothon-Personal-Auto-Claims-Agent
Snowflake Cortex-Powered Personal Auto Claims Processing Agent  > Orchestrated AI - Empowering Adjusters, Accelerating Claims

Problem

Personal Auto insurance claims processing is slow, manual and 
cumbersome. Adjusters spend hours reading documents, extracting 
data and triaging claims — leaving very little time for complex, 
high value decision making.

---

Solution

A Snowflake Cortex-powered Orchestrated AI pipeline that automates 
the manual work — so adjusters start from a strong, informed position 
instead of a blank page. Every final decision stays firmly in 
human hands.

---

Architecture


1. Claims Submission
2.  Cortex COMPLETE reads Claim Narrative + Police Report
3. 6 Rule-Based Fraud Scoring (Strict guardrails - no AI guesswork)
4. Deterministic Triage Classification (Low / Medium / High / Critical)
5. AI Initial Notes + Recommended Action Generated
6. Full Decision Path JSON Logged
7. Adjuster Reviews via Streamlit Portal
8. Adjuster Makes Final Decision


---

Fraud Scoring Rules

| Rule | Weight |
|---|---|
| Police report not available | 0.25 |
| Zero witnesses | 0.15 |
| Claim amount above $70,000 | 0.20 |
| No injuries but ambulance called | 0.15 |
| Incident between midnight and 5am | 0.10 |
| Narrative inconsistent with incident type | 0.15 |

---

Triage Categories

| Category | Rule |
|---|---|
| Low_Standard_Review | Fraud score < 0.30 AND amount < $25,000 |
| Medium_Needs_Review | Fraud score 0.30–0.59 OR amount $25,000–$70,000 |
| High_Escalate | Fraud score 0.60–0.79 OR amount > $70,000 |
| Critical_Fraud_Suspected | Fraud score >= 0.80 |

---

Four Pillars

- **Explainability** — Full decision path JSON logged per claim
- **Auditability** — Model, prompt version, timestamp per claim
- **Defensibility** — Rule based scoring — no black box
- **Observability** — Every rule fired or not fired tracked

---

Human in the Loop

The AI agent suggests. The adjuster decides. Always.

- AI generates fraud score, triage category, initial notes 
  and recommended action
- Adjuster reviews everything via Streamlit Claims Portal
- Adjuster makes final decision — Approve, Reject or Escalate
- Escalation is always adjuster driven — never automatic
- Every override is logged with reason for full auditability

---

Tech Stack

- **Snowflake Cortex** — AI pipeline
- **Cortex Code** — Generated the entire SQL pipeline
- **CORTEX.COMPLETE** — claude-sonnet-4-6
- **Streamlit in Snowflake** — Adjuster portal
- **Python** — Data enrichment
- **SQL** — 8-CTE transactional pipeline

---

Built with on Snowflake Cortex for Cocothon 2026
