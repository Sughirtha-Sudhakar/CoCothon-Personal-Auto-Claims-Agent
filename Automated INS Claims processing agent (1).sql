-- 1. DATABASE & SCHEMA
-- ------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS CLAIMS_AI_DB;
CREATE SCHEMA IF NOT EXISTS CLAIMS_AI_DB.CLAIMS;
USE DATABASE CLAIMS_AI_DB;
USE SCHEMA CLAIMS;

-- 2. WAREHOUSE
-- ------------------------------------------------------------
CREATE WAREHOUSE IF NOT EXISTS CLAIMS_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND   = 60
    AUTO_RESUME    = TRUE;

USE WAREHOUSE CLAIMS_WH;

-- 3. MAIN CLAIMS TABLE
-- Matches enriched CSV columns exactly
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE INSURANCE_CLAIMS (

    -- Core claim identity
    CLAIM_ID                    VARCHAR(30),
    CLAIMANT_NAME               VARCHAR(100),

    -- Customer info
    MONTHS_AS_CUSTOMER          INT,
    AGE                         INT,

    -- Policy info
    POLICY_NUMBER               VARCHAR(20),
    POLICY_BIND_DATE            DATE,
    POLICY_STATE                VARCHAR(5),
    POLICY_CSL                  VARCHAR(20),
    POLICY_DEDUCTABLE           INT,
    POLICY_ANNUAL_PREMIUM       FLOAT,
    UMBRELLA_LIMIT              INT,

    -- Insured person details
    INSURED_ZIP                 VARCHAR(10),
    INSURED_SEX                 VARCHAR(10),
    INSURED_EDUCATION_LEVEL     VARCHAR(50),
    INSURED_OCCUPATION          VARCHAR(100),
    INSURED_HOBBIES             VARCHAR(100),
    INSURED_RELATIONSHIP        VARCHAR(50),
    CAPITAL_GAINS               INT,
    CAPITAL_LOSS                INT,

    -- Incident details
    INCIDENT_DATE               DATE,
    INCIDENT_TYPE               VARCHAR(50),
    COLLISION_TYPE              VARCHAR(50),
    INCIDENT_SEVERITY           VARCHAR(50),
    AUTHORITIES_CONTACTED       VARCHAR(50),
    INCIDENT_STATE              VARCHAR(5),
    INCIDENT_CITY               VARCHAR(100),
    INCIDENT_LOCATION           VARCHAR(200),
    INCIDENT_HOUR_OF_THE_DAY    INT,
    NUMBER_OF_VEHICLES_INVOLVED INT,
    PROPERTY_DAMAGE             VARCHAR(5),
    BODILY_INJURIES             INT,
    WITNESSES                   INT,
    POLICE_REPORT_AVAILABLE     VARCHAR(5),

    -- Claim amounts (USD)
    TOTAL_CLAIM_AMOUNT_USD      FLOAT,
    INJURY_CLAIM_USD            FLOAT,
    PROPERTY_CLAIM_USD          FLOAT,
    VEHICLE_CLAIM_USD           FLOAT,

    -- Vehicle info
    AUTO_MAKE                   VARCHAR(50),
    AUTO_MODEL                  VARCHAR(50),
    AUTO_YEAR                   INT,

    -- Fraud info
    FRAUD_REPORTED              VARCHAR(5),

    -- Processing status
    PROCESSING_STATUS           VARCHAR(30),

    -- Submitted documents
    SUBMITTED_DOCS              VARIANT,
    NUM_DOCS_SUBMITTED          INT,

    -- Unstructured text fields
    CLAIM_NARRATIVE             VARCHAR(2000),
    POLICE_REPORT_TEXT          VARCHAR(2000),

    -- AI workflow columns (populated by Cortex pipeline)
    AI_INITIAL_NOTES            VARCHAR(4000),
    AI_TRIAGE_CATEGORY          VARCHAR(30),
    AI_FRAUD_SCORE              FLOAT,
    AI_RECOMMENDED_ACTION       VARCHAR(1000),
    AI_DECISION_PATH            VARIANT,
    AI_MODEL_USED               VARCHAR(50),
    AI_PROMPT_VERSION           VARCHAR(10),
    AI_PROCESSED_AT             TIMESTAMP,

    -- Adjuster workflow columns (populated after review)
    ADJUSTER_ID                 VARCHAR(20),
    ADJUSTER_FINAL_DECISION     VARCHAR(30),
    ADJUSTER_REMARKS            VARCHAR(2000),
    ADJUSTER_OVERRODE_AI        VARCHAR(5),
    ADJUSTER_OVERRIDE_REASON    VARCHAR(1000),
    ADJUSTER_REVIEWED_AT        TIMESTAMP
);

-- 4. STAGE FOR CSV UPLOAD
-- ------------------------------------------------------------
CREATE OR REPLACE STAGE CLAIMS_STAGE
    FILE_FORMAT = (
        TYPE                        = 'CSV'
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        SKIP_HEADER                 = 1
        NULL_IF                     = ('NULL', 'null', 'None', '')
        EMPTY_FIELD_AS_NULL         = TRUE
        DATE_FORMAT                 = 'YYYY-MM-DD'
    );

-- 5. USEFUL VIEWS
-- ------------------------------------------------------------

-- Dashboard summary view
CREATE OR REPLACE VIEW CLAIMS_DASHBOARD_V AS
SELECT
    INCIDENT_TYPE,
    INCIDENT_SEVERITY,
    COUNT(*)                                            AS TOTAL_CLAIMS,
    SUM(TOTAL_CLAIM_AMOUNT_USD)                         AS TOTAL_VALUE_USD,
    AVG(TOTAL_CLAIM_AMOUNT_USD)                         AS AVG_VALUE_USD,
    SUM(CASE WHEN FRAUD_REPORTED = 'Y' THEN 1 ELSE 0 END) AS FRAUD_COUNT,
    ROUND(SUM(CASE WHEN FRAUD_REPORTED = 'Y' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS FRAUD_PCT,
    SUM(CASE WHEN PROCESSING_STATUS = 'Pending' THEN 1 ELSE 0 END) AS PENDING_COUNT
FROM INSURANCE_CLAIMS
GROUP BY INCIDENT_TYPE, INCIDENT_SEVERITY
ORDER BY TOTAL_VALUE_USD DESC;

-- High value pending claims view
CREATE OR REPLACE VIEW HIGH_VALUE_PENDING_V AS
SELECT
    CLAIM_ID,
    CLAIMANT_NAME,
    INCIDENT_TYPE,
    INCIDENT_SEVERITY,
    TOTAL_CLAIM_AMOUNT_USD,
    INCIDENT_STATE,
    INCIDENT_CITY,
    FRAUD_REPORTED,
    DATEDIFF('day', INCIDENT_DATE, CURRENT_DATE) AS DAYS_SINCE_INCIDENT
FROM INSURANCE_CLAIMS
WHERE PROCESSING_STATUS = 'Pending'
  AND TOTAL_CLAIM_AMOUNT_USD > 50000
ORDER BY TOTAL_CLAIM_AMOUNT_USD DESC;

-- Fraud risk view
CREATE OR REPLACE VIEW FRAUD_RISK_V AS
SELECT
    CLAIM_ID,
    CLAIMANT_NAME,
    INCIDENT_TYPE,
    TOTAL_CLAIM_AMOUNT_USD,
    INCIDENT_STATE,
    MONTHS_AS_CUSTOMER,
    WITNESSES,
    POLICE_REPORT_AVAILABLE,
    BODILY_INJURIES
FROM INSURANCE_CLAIMS
WHERE FRAUD_REPORTED = 'Y'
ORDER BY TOTAL_CLAIM_AMOUNT_USD DESC;

-- 6. VERIFY SETUP
-- ------------------------------------------------------------
SELECT 'Database created'   AS STATUS UNION ALL
SELECT 'Schema created'     AS STATUS UNION ALL
SELECT 'Table created'      AS STATUS UNION ALL
SELECT 'Stage created'      AS STATUS UNION ALL
SELECT 'Views created'      AS STATUS;

-- Check table structure
DESC TABLE INSURANCE_CLAIMS;

select * from INSURANCE_CLAIMS;

-- ============================================================================
-- 7. AUTOMATED CLAIMS PROCESSING PIPELINE
-- Model: claude-sonnet-4-6 | Prompt Version: v1.0
-- Batch size: 10 unprocessed claims per execution
-- Transaction-safe: rolls back on any error — no partial updates
-- NOTE: Each batch may take 1-5 minutes due to LLM inference per claim
-- ============================================================================

EXECUTE IMMEDIATE $$
DECLARE
    rows_processed INTEGER DEFAULT 0;
BEGIN
    BEGIN TRANSACTION;

    MERGE INTO CLAIMS_AI_DB.CLAIMS.INSURANCE_CLAIMS AS target
    USING (

        -- ----------------------------------------------------------------
        -- CTE 1: Select batch of 10 unprocessed claims
        -- ----------------------------------------------------------------
        WITH unprocessed_claims AS (
            SELECT
                CLAIM_ID, CLAIMANT_NAME, POLICY_NUMBER,
                INCIDENT_TYPE, COLLISION_TYPE, INCIDENT_SEVERITY,
                INCIDENT_DATE, INCIDENT_HOUR_OF_THE_DAY,
                AUTHORITIES_CONTACTED, NUMBER_OF_VEHICLES_INVOLVED,
                BODILY_INJURIES, WITNESSES, POLICE_REPORT_AVAILABLE,
                TOTAL_CLAIM_AMOUNT_USD, INJURY_CLAIM_USD,
                PROPERTY_CLAIM_USD, VEHICLE_CLAIM_USD,
                CLAIM_NARRATIVE, POLICE_REPORT_TEXT, SUBMITTED_DOCS,
                AUTO_MAKE, AUTO_MODEL, AUTO_YEAR
            FROM CLAIMS_AI_DB.CLAIMS.INSURANCE_CLAIMS
            WHERE AI_FRAUD_SCORE IS NULL
            LIMIT 10
        ),

        -- ----------------------------------------------------------------
        -- CTE 2: Compute deterministic fraud rules 1-5 from structured data
        -- ----------------------------------------------------------------
        rule_based AS (
            SELECT
                uc.*,
                -- Rule 1: Police report not available (weight 0.25)
                IFF(POLICE_REPORT_AVAILABLE IN ('NO', '?'), TRUE, FALSE) AS R1_POLICE_MISSING,
                -- Rule 2: Zero witnesses (weight 0.15)
                IFF(WITNESSES = 0, TRUE, FALSE) AS R2_ZERO_WITNESSES,
                -- Rule 3: Claim amount above $70,000 (weight 0.20)
                IFF(TOTAL_CLAIM_AMOUNT_USD > 70000, TRUE, FALSE) AS R3_HIGH_AMOUNT,
                -- Rule 4: No bodily injuries but ambulance called (weight 0.15)
                IFF(BODILY_INJURIES = 0 AND AUTHORITIES_CONTACTED = 'Ambulance', TRUE, FALSE) AS R4_NO_INJURY_AMBULANCE,
                -- Rule 5: Incident between midnight and 5am (weight 0.10)
                IFF(INCIDENT_HOUR_OF_THE_DAY BETWEEN 0 AND 5, TRUE, FALSE) AS R5_ODD_HOUR,
                -- Partial fraud score from rules 1-5
                (IFF(POLICE_REPORT_AVAILABLE IN ('NO', '?'), 0.25, 0.00)
                 + IFF(WITNESSES = 0, 0.15, 0.00)
                 + IFF(TOTAL_CLAIM_AMOUNT_USD > 70000, 0.20, 0.00)
                 + IFF(BODILY_INJURIES = 0 AND AUTHORITIES_CONTACTED = 'Ambulance', 0.15, 0.00)
                 + IFF(INCIDENT_HOUR_OF_THE_DAY BETWEEN 0 AND 5, 0.10, 0.00)
                ) AS PARTIAL_SCORE,
                -- Submitted documents as comma-separated string
                COALESCE(NULLIF(ARRAY_TO_STRING(SUBMITTED_DOCS::ARRAY, ', '), ''), 'None') AS DOCS_LIST
            FROM unprocessed_claims uc
        ),

        -- ----------------------------------------------------------------
        -- CTE 3: Single CORTEX.COMPLETE call per claim
        -- LLM evaluates Rule 6 (narrative consistency) and generates
        -- summary + recommended action text
        -- ----------------------------------------------------------------
        ai_response AS (
            SELECT
                rb.*,
                TRIM(SNOWFLAKE.CORTEX.COMPLETE(
                    'claude-sonnet-4-6',
                    CONCAT(
                        'You are an insurance claims processing assistant. Follow ALL rules strictly. ',
                        'Return ONLY a valid JSON object. No markdown, no code fences, no backticks, no explanation.\n\n',

                        '=== CLAIM DATA ===\n',
                        'Claim ID: ', rb.CLAIM_ID, '\n',
                        'Claimant: ', COALESCE(rb.CLAIMANT_NAME, 'N/A'), '\n',
                        'Incident Type: ', COALESCE(rb.INCIDENT_TYPE, 'N/A'), '\n',
                        'Collision Type: ', COALESCE(rb.COLLISION_TYPE, 'N/A'), '\n',
                        'Incident Severity: ', COALESCE(rb.INCIDENT_SEVERITY, 'N/A'), '\n',
                        'Incident Date: ', COALESCE(TO_VARCHAR(rb.INCIDENT_DATE, 'YYYY-MM-DD'), 'N/A'), '\n',
                        'Incident Hour: ', COALESCE(TO_VARCHAR(rb.INCIDENT_HOUR_OF_THE_DAY), 'N/A'), '\n',
                        'Total Claim Amount: $', COALESCE(TO_VARCHAR(rb.TOTAL_CLAIM_AMOUNT_USD), 'N/A'), '\n',
                        'Bodily Injuries: ', COALESCE(TO_VARCHAR(rb.BODILY_INJURIES), 'N/A'), '\n',
                        'Witnesses: ', COALESCE(TO_VARCHAR(rb.WITNESSES), 'N/A'), '\n',
                        'Police Report Available: ', COALESCE(rb.POLICE_REPORT_AVAILABLE, 'N/A'), '\n',
                        'Authorities Contacted: ', COALESCE(rb.AUTHORITIES_CONTACTED, 'N/A'), '\n',
                        'Vehicles Involved: ', COALESCE(TO_VARCHAR(rb.NUMBER_OF_VEHICLES_INVOLVED), 'N/A'), '\n',
                        'Vehicle: ', COALESCE(TO_VARCHAR(rb.AUTO_YEAR), ''), ' ', COALESCE(rb.AUTO_MAKE, ''), ' ', COALESCE(rb.AUTO_MODEL, ''), '\n',
                        'Documents Submitted: ', rb.DOCS_LIST, '\n\n',

                        '=== CLAIM NARRATIVE ===\n',
                        COALESCE(rb.CLAIM_NARRATIVE, 'No narrative provided.'), '\n\n',

                        '=== POLICE REPORT ===\n',
                        COALESCE(rb.POLICE_REPORT_TEXT, 'No police report text available.'), '\n\n',

                        '=== PRE-COMPUTED RULES (1-5, already calculated) ===\n',
                        'Rule 1 - Police report missing (wt 0.25): ', IFF(rb.R1_POLICE_MISSING, 'FIRED', 'NOT FIRED'), '\n',
                        'Rule 2 - Zero witnesses (wt 0.15): ', IFF(rb.R2_ZERO_WITNESSES, 'FIRED', 'NOT FIRED'), '\n',
                        'Rule 3 - Amount >$70k (wt 0.20): ', IFF(rb.R3_HIGH_AMOUNT, 'FIRED', 'NOT FIRED'), '\n',
                        'Rule 4 - No injury + ambulance (wt 0.15): ', IFF(rb.R4_NO_INJURY_AMBULANCE, 'FIRED', 'NOT FIRED'), '\n',
                        'Rule 5 - Odd hour 0-5am (wt 0.10): ', IFF(rb.R5_ODD_HOUR, 'FIRED', 'NOT FIRED'), '\n',
                        'Partial score (rules 1-5): ', TO_VARCHAR(rb.PARTIAL_SCORE), '\n\n',

                        '=== YOUR TASKS ===\n',
                        '1. NARRATIVE CONSISTENCY (Rule 6, weight 0.15): Read ONLY the claim narrative and police report above. ',
                        'Is the narrative inconsistent with the stated incident type "', COALESCE(rb.INCIDENT_TYPE, 'N/A'), '"? ',
                        'Set narrative_inconsistent to true ONLY if there are clear contradictions between the texts and the incident type. If uncertain, set false.\n',
                        '2. FRAUD SCORE: Calculate as ', TO_VARCHAR(rb.PARTIAL_SCORE), ' + (0.15 if narrative_inconsistent is true, else 0.00). Round to 2 decimal places. Cap at 1.00.\n',
                        '3. TRIAGE CATEGORY (apply highest matching): ',
                        'Critical_Fraud_Suspected (score>=0.80) > High_Escalate (score 0.60-0.79 OR amount>70000) > Medium_Needs_Review (score 0.30-0.59 OR amount 25000-70000) > Low_Standard_Review (score<0.30 AND amount<25000)\n',
                        '4. SUMMARY (ai_initial_notes): Write exactly 3-4 sentences using ONLY information from the claim narrative and police report. Include incident type, severity, claim amount, and notable observations. Do NOT add assumptions or information not in the texts.\n',
                        '5. RECOMMENDED ACTION (ai_recommended_action) based on triage category:\n',
                        '   Low_Standard_Review: "Claim looks good for processing. Documents submitted: [actual list from data]. [If all key docs present: All key documents are present. Recommend approval subject to adjuster review.] [If docs missing: However the following documents are missing: [missing list]. Request from claimant before approving.]"\n',
                        '   Medium_Needs_Review: "Additional review needed. Documents submitted: [actual list]. [Missing docs if any.] Schedule adjuster review call with claimant."\n',
                        '   High_Escalate: "Escalation required. Documents submitted: [actual list]. [Missing docs if any.] Escalate to senior adjuster. Do not process until fully reviewed."\n',
                        '   Critical_Fraud_Suspected: "Fraud suspected. Documents submitted: [actual list]. Flag for Special Investigations Unit immediately. Suspend all claim processing."\n',
                        '   Key documents to check for: Police_Report.pdf, Repair_Estimate.pdf, Medical_Bills.pdf, Witness_Statement.pdf, Damage_Photos.jpg, Vehicle_Registration.pdf, Drivers_License.pdf, Third_Party_Statement.pdf\n\n',

                        '=== RESPONSE FORMAT (return ONLY this JSON, nothing else) ===\n',
                        '{"narrative_inconsistent":true_or_false,"fraud_score":0.00,"triage_category":"exact_category_name","ai_initial_notes":"3-4 sentence summary","ai_recommended_action":"action from template"}'
                    )
                )) AS AI_RAW
            FROM rule_based rb
        ),

        -- ----------------------------------------------------------------
        -- CTE 4: Safely parse AI JSON response with fallback extraction
        -- ----------------------------------------------------------------
        parsed AS (
            SELECT
                ar.CLAIM_ID,
                ar.TOTAL_CLAIM_AMOUNT_USD,
                ar.R1_POLICE_MISSING,
                ar.R2_ZERO_WITNESSES,
                ar.R3_HIGH_AMOUNT,
                ar.R4_NO_INJURY_AMBULANCE,
                ar.R5_ODD_HOUR,
                ar.PARTIAL_SCORE,
                COALESCE(
                    TRY_PARSE_JSON(TRIM(ar.AI_RAW)),
                    TRY_PARSE_JSON(REGEXP_SUBSTR(TRIM(ar.AI_RAW), '\\{.*\\}', 1, 1, 's'))
                ) AS AI_JSON
            FROM ai_response ar
        ),

        -- ----------------------------------------------------------------
        -- CTE 5: Extract fields from parsed JSON
        -- Skips claims where JSON parsing failed (AI_JSON IS NULL)
        -- ----------------------------------------------------------------
        extracted AS (
            SELECT
                p.*,
                COALESCE(p.AI_JSON:narrative_inconsistent::BOOLEAN, FALSE) AS R6_NARRATIVE_INCONSISTENT,
                p.AI_JSON:ai_initial_notes::VARCHAR AS AI_INITIAL_NOTES,
                p.AI_JSON:ai_recommended_action::VARCHAR AS AI_RECOMMENDED_ACTION
            FROM parsed p
            WHERE p.AI_JSON IS NOT NULL
        ),

        -- ----------------------------------------------------------------
        -- CTE 6: Compute final fraud score deterministically in SQL
        -- Score = partial_score (rules 1-5) + rule 6 weight if fired
        -- ----------------------------------------------------------------
        scored AS (
            SELECT
                e.*,
                LEAST(
                    ROUND(e.PARTIAL_SCORE + IFF(e.R6_NARRATIVE_INCONSISTENT, 0.15, 0.00), 2),
                    1.00
                ) AS FINAL_FRAUD_SCORE
            FROM extracted e
        ),

        -- ----------------------------------------------------------------
        -- CTE 7: Assign triage category deterministically in SQL
        -- Highest matching category wins (ordered Critical > High > Medium > Low)
        -- ----------------------------------------------------------------
        triaged AS (
            SELECT
                s.*,
                CASE
                    WHEN s.FINAL_FRAUD_SCORE >= 0.80
                        THEN 'Critical_Fraud_Suspected'
                    WHEN s.FINAL_FRAUD_SCORE >= 0.60 OR s.TOTAL_CLAIM_AMOUNT_USD > 70000
                        THEN 'High_Escalate'
                    WHEN s.FINAL_FRAUD_SCORE >= 0.30 OR s.TOTAL_CLAIM_AMOUNT_USD BETWEEN 25000 AND 70000
                        THEN 'Medium_Needs_Review'
                    ELSE 'Low_Standard_Review'
                END AS FINAL_TRIAGE
            FROM scored s
        ),

        -- ----------------------------------------------------------------
        -- CTE 8: Assemble all output columns including decision path JSON
        -- ----------------------------------------------------------------
        final_results AS (
            SELECT
                t.CLAIM_ID,
                t.FINAL_FRAUD_SCORE,
                t.FINAL_TRIAGE,
                t.AI_INITIAL_NOTES,
                t.AI_RECOMMENDED_ACTION,
                -- Decision path JSON with all 6 rules logged
                OBJECT_CONSTRUCT(
                    'fraud_score', t.FINAL_FRAUD_SCORE,
                    'triage_category', t.FINAL_TRIAGE,
                    'rules_fired', ARRAY_CONSTRUCT(
                        OBJECT_CONSTRUCT('rule', 'Police report not available', 'weight', 0.25, 'fired', t.R1_POLICE_MISSING),
                        OBJECT_CONSTRUCT('rule', 'Zero witnesses', 'weight', 0.15, 'fired', t.R2_ZERO_WITNESSES),
                        OBJECT_CONSTRUCT('rule', 'High claim amount', 'weight', 0.20, 'fired', t.R3_HIGH_AMOUNT),
                        OBJECT_CONSTRUCT('rule', 'No injuries but ambulance called', 'weight', 0.15, 'fired', t.R4_NO_INJURY_AMBULANCE),
                        OBJECT_CONSTRUCT('rule', 'Odd hour incident', 'weight', 0.10, 'fired', t.R5_ODD_HOUR),
                        OBJECT_CONSTRUCT('rule', 'Narrative inconsistent', 'weight', 0.15, 'fired', t.R6_NARRATIVE_INCONSISTENT)
                    ),
                    'model_used', 'claude-sonnet-4-6',
                    'prompt_version', 'v1.0'
                ) AS AI_DECISION_PATH,
                -- Processing status mapped from triage category
                CASE t.FINAL_TRIAGE
                    WHEN 'Low_Standard_Review'       THEN 'Pending_Approval'
                    WHEN 'Medium_Needs_Review'       THEN 'Under_Review'
                    WHEN 'High_Escalate'             THEN 'Escalated'
                    WHEN 'Critical_Fraud_Suspected'  THEN 'Fraud_Investigation'
                END AS FINAL_PROCESSING_STATUS
            FROM triaged t
        )

        SELECT * FROM final_results

    ) AS src
    ON target.CLAIM_ID = src.CLAIM_ID

    WHEN MATCHED THEN UPDATE SET
        target.AI_FRAUD_SCORE        = src.FINAL_FRAUD_SCORE,
        target.AI_TRIAGE_CATEGORY    = src.FINAL_TRIAGE,
        target.AI_INITIAL_NOTES      = src.AI_INITIAL_NOTES,
        target.AI_RECOMMENDED_ACTION = src.AI_RECOMMENDED_ACTION,
        target.AI_DECISION_PATH      = src.AI_DECISION_PATH,
        target.AI_MODEL_USED         = 'claude-sonnet-4-6',
        target.AI_PROMPT_VERSION     = 'v1.0',
        target.AI_PROCESSED_AT       = CURRENT_TIMESTAMP(),
        target.PROCESSING_STATUS     = src.FINAL_PROCESSING_STATUS;

    rows_processed := SQLROWCOUNT;
    COMMIT;
    RETURN rows_processed || ' claims processed successfully';

EXCEPTION
    WHEN OTHER THEN
        ROLLBACK;
        RETURN 'ERROR: ' || SQLERRM || ' — transaction rolled back, no claims updated';
END;
$$;

-- ============================================================================
-- 8. VERIFICATION: Check processed claims
-- ============================================================================
SELECT
    CLAIM_ID,
    AI_FRAUD_SCORE,
    AI_TRIAGE_CATEGORY,
    PROCESSING_STATUS,
    AI_MODEL_USED,
    AI_PROMPT_VERSION,
    AI_PROCESSED_AT,
    LEFT(AI_INITIAL_NOTES, 120)      AS NOTES_PREVIEW,
    LEFT(AI_RECOMMENDED_ACTION, 120) AS ACTION_PREVIEW,
    AI_DECISION_PATH
FROM CLAIMS_AI_DB.CLAIMS.INSURANCE_CLAIMS
WHERE AI_PROCESSED_AT IS NOT NULL
ORDER BY AI_PROCESSED_AT DESC
LIMIT 10;

select count(*) from insurance_claims
where ai_fraud_score is null;

-- ============================================================================
-- 9. STORED PROCEDURE: Process ALL claims in a loop (batch of 10)
-- Keeps running until no unprocessed claims remain.
-- Returns JSON summary with total processed, batches run, and any errors.
-- ============================================================================
CREATE OR REPLACE PROCEDURE CLAIMS_AI_DB.CLAIMS.PROCESS_ALL_CLAIMS(BATCH_SIZE INT DEFAULT 10)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    total_processed INT DEFAULT 0;
    batch_count     INT DEFAULT 0;
    batch_result    INT DEFAULT 0;
    remaining       INT DEFAULT 0;
    err_msg         VARCHAR DEFAULT '';
BEGIN

    SELECT COUNT(*) INTO remaining
    FROM CLAIMS_AI_DB.CLAIMS.INSURANCE_CLAIMS
    WHERE AI_FRAUD_SCORE IS NULL;

    IF (remaining = 0) THEN
        RETURN OBJECT_CONSTRUCT(
            'status', 'NO_WORK',
            'message', 'All claims already processed',
            'total_processed', 0,
            'batches_run', 0
        );
    END IF;

    WHILE (remaining > 0) DO

        batch_count := batch_count + 1;
        batch_result := 0;

        BEGIN
            BEGIN TRANSACTION;

            MERGE INTO CLAIMS_AI_DB.CLAIMS.INSURANCE_CLAIMS AS target
            USING (
                WITH unprocessed_claims AS (
                    SELECT
                        CLAIM_ID, CLAIMANT_NAME, POLICY_NUMBER,
                        INCIDENT_TYPE, COLLISION_TYPE, INCIDENT_SEVERITY,
                        INCIDENT_DATE, INCIDENT_HOUR_OF_THE_DAY,
                        AUTHORITIES_CONTACTED, NUMBER_OF_VEHICLES_INVOLVED,
                        BODILY_INJURIES, WITNESSES, POLICE_REPORT_AVAILABLE,
                        TOTAL_CLAIM_AMOUNT_USD, INJURY_CLAIM_USD,
                        PROPERTY_CLAIM_USD, VEHICLE_CLAIM_USD,
                        CLAIM_NARRATIVE, POLICE_REPORT_TEXT, SUBMITTED_DOCS,
                        AUTO_MAKE, AUTO_MODEL, AUTO_YEAR
                    FROM CLAIMS_AI_DB.CLAIMS.INSURANCE_CLAIMS
                    WHERE AI_FRAUD_SCORE IS NULL
                    LIMIT :BATCH_SIZE
                ),
                rule_based AS (
                    SELECT
                        uc.*,
                        IFF(POLICE_REPORT_AVAILABLE IN ('NO', '?'), TRUE, FALSE) AS R1_POLICE_MISSING,
                        IFF(WITNESSES = 0, TRUE, FALSE) AS R2_ZERO_WITNESSES,
                        IFF(TOTAL_CLAIM_AMOUNT_USD > 70000, TRUE, FALSE) AS R3_HIGH_AMOUNT,
                        IFF(BODILY_INJURIES = 0 AND AUTHORITIES_CONTACTED = 'Ambulance', TRUE, FALSE) AS R4_NO_INJURY_AMBULANCE,
                        IFF(INCIDENT_HOUR_OF_THE_DAY BETWEEN 0 AND 5, TRUE, FALSE) AS R5_ODD_HOUR,
                        (IFF(POLICE_REPORT_AVAILABLE IN ('NO', '?'), 0.25, 0.00)
                         + IFF(WITNESSES = 0, 0.15, 0.00)
                         + IFF(TOTAL_CLAIM_AMOUNT_USD > 70000, 0.20, 0.00)
                         + IFF(BODILY_INJURIES = 0 AND AUTHORITIES_CONTACTED = 'Ambulance', 0.15, 0.00)
                         + IFF(INCIDENT_HOUR_OF_THE_DAY BETWEEN 0 AND 5, 0.10, 0.00)
                        ) AS PARTIAL_SCORE,
                        COALESCE(NULLIF(ARRAY_TO_STRING(SUBMITTED_DOCS::ARRAY, ', '), ''), 'None') AS DOCS_LIST
                    FROM unprocessed_claims uc
                ),
                ai_response AS (
                    SELECT
                        rb.*,
                        TRIM(SNOWFLAKE.CORTEX.COMPLETE(
                            'claude-sonnet-4-6',
                            CONCAT(
                                'You are an insurance claims processing assistant. Follow ALL rules strictly. ',
                                'Return ONLY a valid JSON object. No markdown, no code fences, no backticks, no explanation.\n\n',
                                '=== CLAIM DATA ===\n',
                                'Claim ID: ', rb.CLAIM_ID, '\n',
                                'Claimant: ', COALESCE(rb.CLAIMANT_NAME, 'N/A'), '\n',
                                'Incident Type: ', COALESCE(rb.INCIDENT_TYPE, 'N/A'), '\n',
                                'Collision Type: ', COALESCE(rb.COLLISION_TYPE, 'N/A'), '\n',
                                'Incident Severity: ', COALESCE(rb.INCIDENT_SEVERITY, 'N/A'), '\n',
                                'Incident Date: ', COALESCE(TO_VARCHAR(rb.INCIDENT_DATE, 'YYYY-MM-DD'), 'N/A'), '\n',
                                'Incident Hour: ', COALESCE(TO_VARCHAR(rb.INCIDENT_HOUR_OF_THE_DAY), 'N/A'), '\n',
                                'Total Claim Amount: $', COALESCE(TO_VARCHAR(rb.TOTAL_CLAIM_AMOUNT_USD), 'N/A'), '\n',
                                'Bodily Injuries: ', COALESCE(TO_VARCHAR(rb.BODILY_INJURIES), 'N/A'), '\n',
                                'Witnesses: ', COALESCE(TO_VARCHAR(rb.WITNESSES), 'N/A'), '\n',
                                'Police Report Available: ', COALESCE(rb.POLICE_REPORT_AVAILABLE, 'N/A'), '\n',
                                'Authorities Contacted: ', COALESCE(rb.AUTHORITIES_CONTACTED, 'N/A'), '\n',
                                'Vehicles Involved: ', COALESCE(TO_VARCHAR(rb.NUMBER_OF_VEHICLES_INVOLVED), 'N/A'), '\n',
                                'Vehicle: ', COALESCE(TO_VARCHAR(rb.AUTO_YEAR), ''), ' ', COALESCE(rb.AUTO_MAKE, ''), ' ', COALESCE(rb.AUTO_MODEL, ''), '\n',
                                'Documents Submitted: ', rb.DOCS_LIST, '\n\n',
                                '=== CLAIM NARRATIVE ===\n',
                                COALESCE(rb.CLAIM_NARRATIVE, 'No narrative provided.'), '\n\n',
                                '=== POLICE REPORT ===\n',
                                COALESCE(rb.POLICE_REPORT_TEXT, 'No police report text available.'), '\n\n',
                                '=== PRE-COMPUTED RULES (1-5, already calculated) ===\n',
                                'Rule 1 - Police report missing (wt 0.25): ', IFF(rb.R1_POLICE_MISSING, 'FIRED', 'NOT FIRED'), '\n',
                                'Rule 2 - Zero witnesses (wt 0.15): ', IFF(rb.R2_ZERO_WITNESSES, 'FIRED', 'NOT FIRED'), '\n',
                                'Rule 3 - Amount >$70k (wt 0.20): ', IFF(rb.R3_HIGH_AMOUNT, 'FIRED', 'NOT FIRED'), '\n',
                                'Rule 4 - No injury + ambulance (wt 0.15): ', IFF(rb.R4_NO_INJURY_AMBULANCE, 'FIRED', 'NOT FIRED'), '\n',
                                'Rule 5 - Odd hour 0-5am (wt 0.10): ', IFF(rb.R5_ODD_HOUR, 'FIRED', 'NOT FIRED'), '\n',
                                'Partial score (rules 1-5): ', TO_VARCHAR(rb.PARTIAL_SCORE), '\n\n',
                                '=== YOUR TASKS ===\n',
                                '1. NARRATIVE CONSISTENCY (Rule 6, weight 0.15): Read ONLY the claim narrative and police report above. ',
                                'Is the narrative inconsistent with the stated incident type "', COALESCE(rb.INCIDENT_TYPE, 'N/A'), '"? ',
                                'Set narrative_inconsistent to true ONLY if there are clear contradictions between the texts and the incident type. If uncertain, set false.\n',
                                '2. FRAUD SCORE: Calculate as ', TO_VARCHAR(rb.PARTIAL_SCORE), ' + (0.15 if narrative_inconsistent is true, else 0.00). Round to 2 decimal places. Cap at 1.00.\n',
                                '3. TRIAGE CATEGORY (apply highest matching): ',
                                'Critical_Fraud_Suspected (score>=0.80) > High_Escalate (score 0.60-0.79 OR amount>70000) > Medium_Needs_Review (score 0.30-0.59 OR amount 25000-70000) > Low_Standard_Review (score<0.30 AND amount<25000)\n',
                                '4. SUMMARY (ai_initial_notes): Write exactly 3-4 sentences using ONLY information from the claim narrative and police report. Include incident type, severity, claim amount, and notable observations. Do NOT add assumptions or information not in the texts.\n',
                                '5. RECOMMENDED ACTION (ai_recommended_action) based on triage category:\n',
                                '   Low_Standard_Review: "Claim looks good for processing. Documents submitted: [actual list from data]. [If all key docs present: All key documents are present. Recommend approval subject to adjuster review.] [If docs missing: However the following documents are missing: [missing list]. Request from claimant before approving.]"\n',
                                '   Medium_Needs_Review: "Additional review needed. Documents submitted: [actual list]. [Missing docs if any.] Schedule adjuster review call with claimant."\n',
                                '   High_Escalate: "Escalation required. Documents submitted: [actual list]. [Missing docs if any.] Escalate to senior adjuster. Do not process until fully reviewed."\n',
                                '   Critical_Fraud_Suspected: "Fraud suspected. Documents submitted: [actual list]. Flag for Special Investigations Unit immediately. Suspend all claim processing."\n',
                                '   Key documents to check for: Police_Report.pdf, Repair_Estimate.pdf, Medical_Bills.pdf, Witness_Statement.pdf, Damage_Photos.jpg, Vehicle_Registration.pdf, Drivers_License.pdf, Third_Party_Statement.pdf\n\n',
                                '=== RESPONSE FORMAT (return ONLY this JSON, nothing else) ===\n',
                                '{"narrative_inconsistent":true_or_false,"fraud_score":0.00,"triage_category":"exact_category_name","ai_initial_notes":"3-4 sentence summary","ai_recommended_action":"action from template"}'
                            )
                        )) AS AI_RAW
                    FROM rule_based rb
                ),
                parsed AS (
                    SELECT
                        ar.CLAIM_ID,
                        ar.TOTAL_CLAIM_AMOUNT_USD,
                        ar.R1_POLICE_MISSING,
                        ar.R2_ZERO_WITNESSES,
                        ar.R3_HIGH_AMOUNT,
                        ar.R4_NO_INJURY_AMBULANCE,
                        ar.R5_ODD_HOUR,
                        ar.PARTIAL_SCORE,
                        COALESCE(
                            TRY_PARSE_JSON(TRIM(ar.AI_RAW)),
                            TRY_PARSE_JSON(REGEXP_SUBSTR(TRIM(ar.AI_RAW), '\\{.*\\}', 1, 1, 's'))
                        ) AS AI_JSON
                    FROM ai_response ar
                ),
                extracted AS (
                    SELECT
                        p.*,
                        COALESCE(p.AI_JSON:narrative_inconsistent::BOOLEAN, FALSE) AS R6_NARRATIVE_INCONSISTENT,
                        p.AI_JSON:ai_initial_notes::VARCHAR AS AI_INITIAL_NOTES,
                        p.AI_JSON:ai_recommended_action::VARCHAR AS AI_RECOMMENDED_ACTION
                    FROM parsed p
                    WHERE p.AI_JSON IS NOT NULL
                ),
                scored AS (
                    SELECT
                        e.*,
                        LEAST(
                            ROUND(e.PARTIAL_SCORE + IFF(e.R6_NARRATIVE_INCONSISTENT, 0.15, 0.00), 2),
                            1.00
                        ) AS FINAL_FRAUD_SCORE
                    FROM extracted e
                ),
                triaged AS (
                    SELECT
                        s.*,
                        CASE
                            WHEN s.FINAL_FRAUD_SCORE >= 0.80
                                THEN 'Critical_Fraud_Suspected'
                            WHEN s.FINAL_FRAUD_SCORE >= 0.60 OR s.TOTAL_CLAIM_AMOUNT_USD > 70000
                                THEN 'High_Escalate'
                            WHEN s.FINAL_FRAUD_SCORE >= 0.30 OR s.TOTAL_CLAIM_AMOUNT_USD BETWEEN 25000 AND 70000
                                THEN 'Medium_Needs_Review'
                            ELSE 'Low_Standard_Review'
                        END AS FINAL_TRIAGE
                    FROM scored s
                ),
                final_results AS (
                    SELECT
                        t.CLAIM_ID,
                        t.FINAL_FRAUD_SCORE,
                        t.FINAL_TRIAGE,
                        t.AI_INITIAL_NOTES,
                        t.AI_RECOMMENDED_ACTION,
                        OBJECT_CONSTRUCT(
                            'fraud_score', t.FINAL_FRAUD_SCORE,
                            'triage_category', t.FINAL_TRIAGE,
                            'rules_fired', ARRAY_CONSTRUCT(
                                OBJECT_CONSTRUCT('rule', 'Police report not available', 'weight', 0.25, 'fired', t.R1_POLICE_MISSING),
                                OBJECT_CONSTRUCT('rule', 'Zero witnesses', 'weight', 0.15, 'fired', t.R2_ZERO_WITNESSES),
                                OBJECT_CONSTRUCT('rule', 'High claim amount', 'weight', 0.20, 'fired', t.R3_HIGH_AMOUNT),
                                OBJECT_CONSTRUCT('rule', 'No injuries but ambulance called', 'weight', 0.15, 'fired', t.R4_NO_INJURY_AMBULANCE),
                                OBJECT_CONSTRUCT('rule', 'Odd hour incident', 'weight', 0.10, 'fired', t.R5_ODD_HOUR),
                                OBJECT_CONSTRUCT('rule', 'Narrative inconsistent', 'weight', 0.15, 'fired', t.R6_NARRATIVE_INCONSISTENT)
                            ),
                            'model_used', 'claude-sonnet-4-6',
                            'prompt_version', 'v1.0'
                        ) AS AI_DECISION_PATH,
                        CASE t.FINAL_TRIAGE
                            WHEN 'Low_Standard_Review'       THEN 'Pending_Approval'
                            WHEN 'Medium_Needs_Review'       THEN 'Under_Review'
                            WHEN 'High_Escalate'             THEN 'Escalated'
                            WHEN 'Critical_Fraud_Suspected'  THEN 'Fraud_Investigation'
                        END AS FINAL_PROCESSING_STATUS
                    FROM triaged t
                )
                SELECT * FROM final_results
            ) AS src
            ON target.CLAIM_ID = src.CLAIM_ID
            WHEN MATCHED THEN UPDATE SET
                target.AI_FRAUD_SCORE        = src.FINAL_FRAUD_SCORE,
                target.AI_TRIAGE_CATEGORY    = src.FINAL_TRIAGE,
                target.AI_INITIAL_NOTES      = src.AI_INITIAL_NOTES,
                target.AI_RECOMMENDED_ACTION = src.AI_RECOMMENDED_ACTION,
                target.AI_DECISION_PATH      = src.AI_DECISION_PATH,
                target.AI_MODEL_USED         = 'claude-sonnet-4-6',
                target.AI_PROMPT_VERSION     = 'v1.0',
                target.AI_PROCESSED_AT       = CURRENT_TIMESTAMP(),
                target.PROCESSING_STATUS     = src.FINAL_PROCESSING_STATUS;

            batch_result := SQLROWCOUNT;
            COMMIT;

            total_processed := total_processed + batch_result;

        EXCEPTION
            WHEN OTHER THEN
                ROLLBACK;
                err_msg := SQLERRM;
                RETURN OBJECT_CONSTRUCT(
                    'status', 'ERROR',
                    'message', err_msg,
                    'total_processed_before_error', total_processed,
                    'batches_completed', batch_count - 1,
                    'failed_batch', batch_count
                );
        END;

        IF (batch_result = 0) THEN
            remaining := 0;
        ELSE
            SELECT COUNT(*) INTO remaining
            FROM CLAIMS_AI_DB.CLAIMS.INSURANCE_CLAIMS
            WHERE AI_FRAUD_SCORE IS NULL;
        END IF;

    END WHILE;

    RETURN OBJECT_CONSTRUCT(
        'status', 'SUCCESS',
        'total_processed', total_processed,
        'batches_run', batch_count
    );

END;
$$;

--Run it (default batch size = 10):
CALL CLAIMS_AI_DB.CLAIMS.PROCESS_ALL_CLAIMS();
--
----CREATE A DEMO TABLE FOR STREAMLIT APP----
CREATE OR REPLACE TABLE CLAIMS_AI_DB.CLAIMS.INSURANCE_CLAIMS_DEMO AS

-- 10 Critical Fraud Suspected - highest fraud scores
SELECT * FROM (
    SELECT * FROM CLAIMS_AI_DB.CLAIMS.INSURANCE_CLAIMS
    WHERE AI_TRIAGE_CATEGORY = 'Critical_Fraud_Suspected'
    AND AI_FRAUD_SCORE IS NOT NULL
    AND AI_INITIAL_NOTES IS NOT NULL
    AND AI_RECOMMENDED_ACTION IS NOT NULL
    ORDER BY AI_FRAUD_SCORE DESC
    LIMIT 10
)

UNION ALL

-- 15 High Escalate - mix of high amounts and high fraud scores
SELECT * FROM (
    SELECT * FROM CLAIMS_AI_DB.CLAIMS.INSURANCE_CLAIMS
    WHERE AI_TRIAGE_CATEGORY = 'High_Escalate'
    AND AI_FRAUD_SCORE IS NOT NULL
    AND AI_INITIAL_NOTES IS NOT NULL
    AND AI_RECOMMENDED_ACTION IS NOT NULL
    ORDER BY TOTAL_CLAIM_AMOUNT_USD DESC
    LIMIT 15
)

UNION ALL

-- 15 Medium Needs Review - variety of incident types
SELECT * FROM (
    SELECT * FROM CLAIMS_AI_DB.CLAIMS.INSURANCE_CLAIMS
    WHERE AI_TRIAGE_CATEGORY = 'Medium_Needs_Review'
    AND AI_FRAUD_SCORE IS NOT NULL
    AND AI_INITIAL_NOTES IS NOT NULL
    AND AI_RECOMMENDED_ACTION IS NOT NULL
    ORDER BY AI_FRAUD_SCORE DESC
    LIMIT 15
)

UNION ALL

-- 10 Low Standard Review - clean low risk claims
SELECT * FROM (
    SELECT * FROM CLAIMS_AI_DB.CLAIMS.INSURANCE_CLAIMS
    WHERE AI_TRIAGE_CATEGORY = 'Low_Standard_Review'
    AND AI_FRAUD_SCORE IS NOT NULL
    AND AI_INITIAL_NOTES IS NOT NULL
    AND AI_RECOMMENDED_ACTION IS NOT NULL
    ORDER BY AI_FRAUD_SCORE ASC
    LIMIT 10
);

-- Verify the distribution
SELECT
    AI_TRIAGE_CATEGORY,
    COUNT(*) AS CLAIM_COUNT,
    ROUND(AVG(AI_FRAUD_SCORE), 2) AS AVG_FRAUD_SCORE,
    MIN(TOTAL_CLAIM_AMOUNT_USD) AS MIN_AMOUNT,
    MAX(TOTAL_CLAIM_AMOUNT_USD) AS MAX_AMOUNT
FROM CLAIMS_AI_DB.CLAIMS.INSURANCE_CLAIMS_DEMO
GROUP BY AI_TRIAGE_CATEGORY
ORDER BY AVG_FRAUD_SCORE DESC;
