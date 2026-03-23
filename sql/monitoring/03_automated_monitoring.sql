-- ============================================================
-- Snowflake MCP Banking Agent — Automated Monitoring
-- Author: Satish Kumar
-- LinkedIn: https://www.linkedin.com/in/satishkumar-snowflake/
-- Medium:   https://medium.com/@snowflakechronicles
-- ============================================================

-- ── 17a. Snapshot Table ─────────────────────────────────────
CREATE OR REPLACE TABLE MCP_DEMO_DB.MCP_SCHEMA.agent_monitoring_snapshots (
    snapshot_id    NUMBER AUTOINCREMENT,
    snapshot_time  TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    report_name    VARCHAR,
    lookback_days  NUMBER,
    report_data    VARIANT
);

-- ── 17b. Stored Procedure ───────────────────────────────────
CREATE OR REPLACE PROCEDURE MCP_DEMO_DB.MCP_SCHEMA.sp_agent_monitoring_report(LOOKBACK_DAYS FLOAT)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    INSERT INTO MCP_DEMO_DB.MCP_SCHEMA.agent_monitoring_snapshots
        (report_name, lookback_days, report_data)
    SELECT 'DAILY_SUMMARY', :LOOKBACK_DAYS,
        OBJECT_CONSTRUCT(
            'day', day, 'requests', requests,
            'unique_users', unique_users,
            'total_tokens', total_tokens,
            'total_credits', total_credits,
            'avg_ms', avg_ms, 'p95_ms', p95_ms,
            'slow_requests', slow_requests
        )
    FROM (
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
          AND START_TIME >= DATEADD('day', -:LOOKBACK_DAYS, CURRENT_TIMESTAMP())
        GROUP BY day
    );

    INSERT INTO MCP_DEMO_DB.MCP_SCHEMA.agent_monitoring_snapshots
        (report_name, lookback_days, report_data)
    SELECT 'RESPONSE_TIME_ANALYTICS', :LOOKBACK_DAYS,
        OBJECT_CONSTRUCT(
            'day', day, 'total_requests', total_requests,
            'avg_response_ms', avg_response_ms,
            'p50_response_ms', p50_response_ms,
            'p95_response_ms', p95_response_ms,
            'max_response_ms', max_response_ms
        )
    FROM (
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
          AND START_TIME >= DATEADD('day', -:LOOKBACK_DAYS, CURRENT_TIMESTAMP())
        GROUP BY day
    );

    INSERT INTO MCP_DEMO_DB.MCP_SCHEMA.agent_monitoring_snapshots
        (report_name, lookback_days, report_data)
    SELECT 'USER_ACTIVITY', :LOOKBACK_DAYS,
        OBJECT_CONSTRUCT(
            'user_name', USER_NAME,
            'total_requests', COUNT(*),
            'total_tokens', SUM(TOKENS),
            'total_credits', ROUND(SUM(TOKEN_CREDITS), 4),
            'first_request', MIN(START_TIME)::VARCHAR,
            'last_request', MAX(START_TIME)::VARCHAR,
            'avg_response_ms', ROUND(AVG(TIMESTAMPDIFF('millisecond', START_TIME, END_TIME)), 0)
        )
    FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_AGENT_USAGE_HISTORY
    WHERE AGENT_NAME = 'BANKING_ASSISTANT'
      AND AGENT_DATABASE_NAME = 'MCP_DEMO_DB'
      AND START_TIME >= DATEADD('day', -:LOOKBACK_DAYS, CURRENT_TIMESTAMP())
    GROUP BY USER_NAME;

    INSERT INTO MCP_DEMO_DB.MCP_SCHEMA.agent_monitoring_snapshots
        (report_name, lookback_days, report_data)
    SELECT 'REALTIME_INVOCATIONS', :LOOKBACK_DAYS,
        OBJECT_CONSTRUCT(
            'query_id', QUERY_ID,
            'start_time', START_TIME::VARCHAR,
            'duration_ms', TIMESTAMPDIFF('millisecond', START_TIME, END_TIME),
            'user_name', USER_NAME,
            'role_name', ROLE_NAME,
            'execution_status', EXECUTION_STATUS,
            'user_question', REGEXP_SUBSTR(QUERY_TEXT, '"text": "([^"]+)"', 1, 1, 'e')
        )
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE QUERY_TEXT ILIKE '%DATA_AGENT_RUN%BANKING_ASSISTANT%'
      AND START_TIME >= DATEADD('day', -:LOOKBACK_DAYS, CURRENT_TIMESTAMP());

    INSERT INTO MCP_DEMO_DB.MCP_SCHEMA.agent_monitoring_snapshots
        (report_name, lookback_days, report_data)
    SELECT 'CORTEX_SEARCH_CREDITS', :LOOKBACK_DAYS,
        OBJECT_CONSTRUCT(
            'start_time', START_TIME::VARCHAR,
            'end_time', END_TIME::VARCHAR,
            'schema_name', SCHEMA_NAME,
            'service_name', SERVICE_NAME,
            'credits', CREDITS
        )
    FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_SEARCH_SERVING_USAGE_HISTORY
    WHERE SERVICE_NAME = 'PRODUCT_SEARCH_SERVICE'
      AND DATABASE_NAME = 'MCP_DEMO_DB'
      AND START_TIME >= DATEADD('day', -:LOOKBACK_DAYS, CURRENT_TIMESTAMP());

    INSERT INTO MCP_DEMO_DB.MCP_SCHEMA.agent_monitoring_snapshots
        (report_name, lookback_days, report_data)
    SELECT 'ROLE_AUDIT', :LOOKBACK_DAYS,
        OBJECT_CONSTRUCT(
            'role_used', METADATA:role_name::STRING,
            'user_name', USER_NAME,
            'request_count', COUNT(*),
            'total_credits', ROUND(SUM(TOKEN_CREDITS), 4)
        )
    FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_AGENT_USAGE_HISTORY
    WHERE AGENT_NAME = 'BANKING_ASSISTANT'
      AND AGENT_DATABASE_NAME = 'MCP_DEMO_DB'
      AND START_TIME >= DATEADD('day', -:LOOKBACK_DAYS, CURRENT_TIMESTAMP())
    GROUP BY METADATA:role_name::STRING, USER_NAME;

    RETURN 'Monitoring report completed. ' ||
           (SELECT COUNT(*) FROM MCP_DEMO_DB.MCP_SCHEMA.agent_monitoring_snapshots
            WHERE snapshot_time >= DATEADD('second', -10, CURRENT_TIMESTAMP()))::VARCHAR ||
           ' rows inserted into AGENT_MONITORING_SNAPSHOTS.';
END;
$$;

-- ── 17c. Manual Execution ───────────────────────────────────
-- CALL MCP_DEMO_DB.MCP_SCHEMA.sp_agent_monitoring_report(1);   -- last 24 hours
-- CALL MCP_DEMO_DB.MCP_SCHEMA.sp_agent_monitoring_report(7);   -- last 7 days
-- CALL MCP_DEMO_DB.MCP_SCHEMA.sp_agent_monitoring_report(30);  -- last 30 days

-- ── 17d. Scheduled Tasks ────────────────────────────────────
CREATE OR REPLACE TASK MCP_DEMO_DB.MCP_SCHEMA.task_daily_monitoring_report
    WAREHOUSE = SHARED_WH
    SCHEDULE = 'USING CRON 0 8 * * * UTC'
    COMMENT = 'Daily at 8AM UTC — 1-day lookback'
AS
    CALL MCP_DEMO_DB.MCP_SCHEMA.sp_agent_monitoring_report(1);

CREATE OR REPLACE TASK MCP_DEMO_DB.MCP_SCHEMA.task_weekly_monitoring_report
    WAREHOUSE = SHARED_WH
    SCHEDULE = 'USING CRON 0 9 * * 1 UTC'
    COMMENT = 'Every Monday 9AM UTC - 30-day lookback'
AS
    CALL MCP_DEMO_DB.MCP_SCHEMA.sp_agent_monitoring_report(30);

ALTER TASK MCP_DEMO_DB.MCP_SCHEMA.TASK_DAILY_MONITORING_REPORT RESUME;
ALTER TASK MCP_DEMO_DB.MCP_SCHEMA.TASK_WEEKLY_MONITORING_REPORT RESUME;

-- ── 17e-f. Query & Flatten Snapshots ───────────────────────
SELECT report_name, lookback_days, report_data, snapshot_time
FROM MCP_DEMO_DB.MCP_SCHEMA.agent_monitoring_snapshots
ORDER BY snapshot_time DESC
LIMIT 50;

SELECT
    report_data:day::DATE          AS day,
    report_data:requests::NUMBER   AS requests,
    report_data:unique_users::NUMBER AS unique_users,
    report_data:total_tokens::NUMBER AS total_tokens,
    report_data:total_credits::FLOAT AS total_credits,
    report_data:avg_ms::NUMBER     AS avg_ms,
    report_data:p95_ms::NUMBER     AS p95_ms,
    report_data:slow_requests::NUMBER AS slow_requests
FROM MCP_DEMO_DB.MCP_SCHEMA.agent_monitoring_snapshots
WHERE report_name = 'DAILY_SUMMARY'
ORDER BY snapshot_time DESC, day DESC;

-- ── 17g. Retention Policy ───────────────────────────────────
-- DELETE FROM MCP_DEMO_DB.MCP_SCHEMA.agent_monitoring_snapshots
-- WHERE snapshot_time < DATEADD('day', -90, CURRENT_TIMESTAMP());
