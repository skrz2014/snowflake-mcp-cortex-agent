-- ============================================================
-- Snowflake MCP Banking Agent — Monitoring Queries
-- Author: Satish Kumar
-- LinkedIn: https://www.linkedin.com/in/satishkumar-snowflake/
-- Medium:   https://medium.com/@snowflakechronicles
-- ============================================================
-- NOTE: CORTEX_AGENT_USAGE_HISTORY has ~2h latency.
--       Use 15j/15k for immediate post-test verification.
-- ============================================================

-- ── 15a. All Agent Invocations ──────────────────────────────
SELECT
    START_TIME,
    END_TIME,
    TIMESTAMPDIFF('millisecond', START_TIME, END_TIME) AS response_time_ms,
    USER_NAME,
    AGENT_NAME,
    REQUEST_ID,
    TOKENS,
    TOKEN_CREDITS,
    METADATA:role_name::STRING AS role_used
FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_AGENT_USAGE_HISTORY
WHERE AGENT_NAME = 'BANKING_ASSISTANT'
  AND AGENT_DATABASE_NAME = 'MCP_DEMO_DB'
ORDER BY START_TIME DESC
LIMIT 50;

-- ── 15b. Response Time Percentiles by Day ──────────────────
SELECT
    DATE_TRUNC('day', START_TIME)::DATE AS day,
    COUNT(*) AS total_requests,
    ROUND(AVG(TIMESTAMPDIFF('millisecond', START_TIME, END_TIME)), 0) AS avg_response_ms,
    ROUND(MEDIAN(TIMESTAMPDIFF('millisecond', START_TIME, END_TIME)), 0) AS p50_response_ms,
    ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (
        ORDER BY TIMESTAMPDIFF('millisecond', START_TIME, END_TIME)), 0) AS p95_response_ms,
    MAX(TIMESTAMPDIFF('millisecond', START_TIME, END_TIME)) AS max_response_ms
FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_AGENT_USAGE_HISTORY
WHERE AGENT_NAME = 'BANKING_ASSISTANT'
  AND AGENT_DATABASE_NAME = 'MCP_DEMO_DB'
  AND START_TIME >= DATEADD('day', -30, CURRENT_TIMESTAMP())
GROUP BY day
ORDER BY day DESC;

-- ── 15c. Token & Credit Consumption ────────────────────────
SELECT
    DATE_TRUNC('day', START_TIME)::DATE AS day,
    COUNT(*) AS total_requests,
    SUM(TOKENS) AS total_tokens,
    ROUND(SUM(TOKEN_CREDITS), 4) AS total_credits,
    ROUND(AVG(TOKENS), 0) AS avg_tokens_per_request,
    ROUND(AVG(TOKEN_CREDITS), 6) AS avg_credits_per_request
FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_AGENT_USAGE_HISTORY
WHERE AGENT_NAME = 'BANKING_ASSISTANT'
  AND AGENT_DATABASE_NAME = 'MCP_DEMO_DB'
  AND START_TIME >= DATEADD('day', -30, CURRENT_TIMESTAMP())
GROUP BY day
ORDER BY day DESC;

-- ── 15d. Tool-Level Breakdown ───────────────────────────────
SELECT
    DATE_TRUNC('day', START_TIME)::DATE AS day,
    g.value AS granular_entry,
    COUNT(*) AS invocations
FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_AGENT_USAGE_HISTORY,
    LATERAL FLATTEN(input => TOKENS_GRANULAR) g
WHERE AGENT_NAME = 'BANKING_ASSISTANT'
  AND AGENT_DATABASE_NAME = 'MCP_DEMO_DB'
  AND START_TIME >= DATEADD('day', -30, CURRENT_TIMESTAMP())
GROUP BY day, granular_entry
ORDER BY day DESC;

-- ── 15e. User Activity Leaderboard ─────────────────────────
SELECT
    USER_NAME,
    COUNT(*) AS total_requests,
    SUM(TOKENS) AS total_tokens,
    ROUND(SUM(TOKEN_CREDITS), 4) AS total_credits,
    MIN(START_TIME) AS first_request,
    MAX(START_TIME) AS last_request,
    ROUND(AVG(TIMESTAMPDIFF('millisecond', START_TIME, END_TIME)), 0) AS avg_response_ms
FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_AGENT_USAGE_HISTORY
WHERE AGENT_NAME = 'BANKING_ASSISTANT'
  AND AGENT_DATABASE_NAME = 'MCP_DEMO_DB'
  AND START_TIME >= DATEADD('day', -30, CURRENT_TIMESTAMP())
GROUP BY USER_NAME
ORDER BY total_requests DESC;

-- ── 15f. Slow Requests (>10 seconds) ───────────────────────
SELECT
    START_TIME,
    END_TIME,
    TIMESTAMPDIFF('millisecond', START_TIME, END_TIME) AS response_time_ms,
    USER_NAME,
    REQUEST_ID,
    TOKENS,
    TOKEN_CREDITS,
    METADATA:role_name::STRING AS role_used
FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_AGENT_USAGE_HISTORY
WHERE AGENT_NAME = 'BANKING_ASSISTANT'
  AND AGENT_DATABASE_NAME = 'MCP_DEMO_DB'
  AND TIMESTAMPDIFF('millisecond', START_TIME, END_TIME) > 10000
  AND START_TIME >= DATEADD('day', -30, CURRENT_TIMESTAMP())
ORDER BY response_time_ms DESC;

-- ── 15g. Hourly Usage Heatmap ───────────────────────────────
SELECT
    HOUR(START_TIME) AS hour_of_day,
    DAYNAME(START_TIME) AS day_of_week,
    COUNT(*) AS request_count,
    ROUND(AVG(TIMESTAMPDIFF('millisecond', START_TIME, END_TIME)), 0) AS avg_response_ms
FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_AGENT_USAGE_HISTORY
WHERE AGENT_NAME = 'BANKING_ASSISTANT'
  AND AGENT_DATABASE_NAME = 'MCP_DEMO_DB'
  AND START_TIME >= DATEADD('day', -30, CURRENT_TIMESTAMP())
GROUP BY hour_of_day, day_of_week
ORDER BY hour_of_day, day_of_week;

-- ── 15h. Credit Breakdown by Model / Service ────────────────
SELECT
    DATE_TRUNC('day', START_TIME)::DATE AS day,
    f.key AS request_id,
    svc.key AS service_type,
    model.key AS model_name,
    model.value:input::FLOAT AS input_credits,
    model.value:output::FLOAT AS output_credits,
    COALESCE(model.value:cache_read_input::FLOAT, 0) AS cache_read_credits,
    (model.value:input::FLOAT + model.value:output::FLOAT
        + COALESCE(model.value:cache_read_input::FLOAT, 0)) AS total_credits
FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_AGENT_USAGE_HISTORY,
    LATERAL FLATTEN(input => CREDITS_GRANULAR) g,
    LATERAL FLATTEN(input => g.value) f,
    LATERAL FLATTEN(input => f.value) svc,
    LATERAL FLATTEN(input => svc.value) model
WHERE AGENT_NAME = 'BANKING_ASSISTANT'
  AND AGENT_DATABASE_NAME = 'MCP_DEMO_DB'
  AND START_TIME >= DATEADD('day', -7, CURRENT_TIMESTAMP())
  AND model.key NOT IN ('start_time')
ORDER BY day DESC, total_credits DESC;

-- ── 15i. Role-Based Access Audit ────────────────────────────
SELECT
    METADATA:role_name::STRING AS role_used,
    USER_NAME,
    COUNT(*) AS request_count,
    ROUND(SUM(TOKEN_CREDITS), 4) AS total_credits
FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_AGENT_USAGE_HISTORY
WHERE AGENT_NAME = 'BANKING_ASSISTANT'
  AND AGENT_DATABASE_NAME = 'MCP_DEMO_DB'
  AND START_TIME >= DATEADD('day', -30, CURRENT_TIMESTAMP())
GROUP BY role_used, USER_NAME
ORDER BY total_credits DESC;

-- ── 15j. Real-Time Fallback via QUERY_HISTORY ───────────────
SELECT
    QUERY_ID,
    START_TIME,
    END_TIME,
    TIMESTAMPDIFF('millisecond', START_TIME, END_TIME) AS duration_ms,
    USER_NAME,
    ROLE_NAME,
    EXECUTION_STATUS,
    REGEXP_SUBSTR(QUERY_TEXT, '"text": "([^"]+)"', 1, 1, 'e') AS user_question
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE QUERY_TEXT ILIKE '%DATA_AGENT_RUN%BANKING_ASSISTANT%'
  AND START_TIME >= DATEADD('day', -7, CURRENT_TIMESTAMP())
ORDER BY START_TIME DESC;

-- ── 15k. Cortex Search Serving Credits (zero latency) ───────
SELECT
    START_TIME,
    END_TIME,
    DATABASE_NAME,
    SCHEMA_NAME,
    SERVICE_NAME,
    CREDITS
FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_SEARCH_SERVING_USAGE_HISTORY
WHERE SERVICE_NAME = 'PRODUCT_SEARCH_SERVICE'
  AND DATABASE_NAME = 'MCP_DEMO_DB'
  AND START_TIME >= DATEADD('day', -7, CURRENT_TIMESTAMP())
ORDER BY START_TIME DESC;

-- ── 15l. Executive Daily Dashboard ─────────────────────────
SELECT
    DATE_TRUNC('day', START_TIME)::DATE AS day,
    COUNT(*) AS requests,
    COUNT(DISTINCT USER_NAME) AS unique_users,
    SUM(TOKENS) AS total_tokens,
    ROUND(SUM(TOKEN_CREDITS), 4) AS total_credits,
    ROUND(AVG(TIMESTAMPDIFF('millisecond', START_TIME, END_TIME)), 0) AS avg_ms,
    ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (
        ORDER BY TIMESTAMPDIFF('millisecond', START_TIME, END_TIME)), 0) AS p95_ms,
    SUM(CASE WHEN TIMESTAMPDIFF('millisecond', START_TIME, END_TIME) > 10000
        THEN 1 ELSE 0 END) AS slow_requests
FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_AGENT_USAGE_HISTORY
WHERE AGENT_NAME = 'BANKING_ASSISTANT'
  AND AGENT_DATABASE_NAME = 'MCP_DEMO_DB'
  AND START_TIME >= DATEADD('day', -30, CURRENT_TIMESTAMP())
GROUP BY day
ORDER BY day DESC;
