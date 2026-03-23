-- ============================================================
-- Snowflake MCP Banking Agent — Full Setup Script
-- Author: Satish Kumar
-- LinkedIn: https://www.linkedin.com/in/satishkumar-snowflake/
-- Medium:   https://medium.com/@snowflakechronicles
-- ============================================================
-- Run each phase in order. Replace SATISH with your username.
-- ============================================================

-- ── Phase 1: Environment ────────────────────────────────────
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE SHARED_WH;

CREATE DATABASE IF NOT EXISTS MCP_DEMO_DB;
CREATE SCHEMA IF NOT EXISTS MCP_DEMO_DB.MCP_SCHEMA;
USE DATABASE MCP_DEMO_DB;
USE SCHEMA MCP_DEMO_DB.MCP_SCHEMA;
ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION';

-- ── Phase 2: Source Data ────────────────────────────────────
CREATE OR REPLACE TABLE products (
    product_id    NUMBER AUTOINCREMENT,
    product_name  VARCHAR,
    category      VARCHAR,
    description   VARCHAR,
    price         NUMBER(10,2),
    region        VARCHAR
);

INSERT INTO products (product_name, category, description, price, region) VALUES
    ('Premium Savings Account', 'Banking',
     'High-yield savings account with 7.5% interest rate for balances above 1 lakh',
     0.00, 'India'),
    ('Gold Credit Card', 'Cards',
     'Reward credit card with 2x points on dining and travel, no annual fee first year',
     500.00, 'India'),
    ('Home Loan Fixed Rate', 'Loans',
     'Fixed rate home loan at 8.5% for up to 30 years with zero processing fee',
     0.00, 'India'),
    ('Term Deposit 1Y', 'Deposits',
     '1-year fixed deposit with 7.1% guaranteed returns and premature withdrawal facility',
     10000.00, 'India'),
    ('Mutual Fund SIP', 'Investments',
     'Systematic investment plan starting at Rs 500/month across equity and debt funds',
     500.00, 'India');

CREATE OR REPLACE TABLE revenue (
    transaction_date  DATE,
    product_category  VARCHAR,
    region            VARCHAR,
    revenue_amount    NUMBER(12,2),
    transactions      NUMBER
);

INSERT INTO revenue VALUES
    ('2025-01-15', 'Banking',     'Mumbai',    1250000.00, 3200),
    ('2025-01-15', 'Cards',       'Mumbai',     890000.00, 5600),
    ('2025-01-15', 'Loans',       'Delhi',     3400000.00, 1200),
    ('2025-02-15', 'Banking',     'Bangalore',  980000.00, 2800),
    ('2025-02-15', 'Investments', 'Hyderabad',  670000.00, 4100),
    ('2025-03-15', 'Deposits',    'Chennai',   2100000.00, 1900),
    ('2025-03-15', 'Cards',       'Pune',       540000.00, 3300);

-- ── Phase 3: Cortex Search Service ─────────────────────────
CREATE OR REPLACE CORTEX SEARCH SERVICE
    MCP_DEMO_DB.MCP_SCHEMA.product_search_service
    ON description
    ATTRIBUTES category, region
    WAREHOUSE = SHARED_WH
    TARGET_LAG = '1 hour'
    AS (
        SELECT product_name, category, description, price, region
        FROM products
    );

-- ── Phase 4: Semantic View ──────────────────────────────────
CREATE OR REPLACE SEMANTIC VIEW MCP_DEMO_DB.MCP_SCHEMA.REVENUE_SEMANTIC_VIEW
TABLES (
    revenue AS MCP_DEMO_DB.MCP_SCHEMA.REVENUE
      PRIMARY KEY (transaction_date, product_category, region)
      COMMENT = 'Banking revenue data by product category and region'
  )
  DIMENSIONS (
    revenue.transaction_date_dim AS transaction_date
      COMMENT = 'Date of the transaction',
    revenue.product_category_dim AS product_category
      WITH SYNONYMS = ('product', 'category', 'product type')
      COMMENT = 'Banking product category',
    revenue.region_dim AS region
      WITH SYNONYMS = ('city', 'location')
      COMMENT = 'Geographic region'
  )
  METRICS (
    revenue.total_revenue AS SUM(revenue_amount)
      WITH SYNONYMS = ('revenue', 'sales', 'income')
      COMMENT = 'Total revenue amount in INR',
    revenue.total_transactions AS SUM(transactions)
      WITH SYNONYMS = ('transaction count', 'number of transactions')
      COMMENT = 'Total number of transactions',
    revenue.avg_revenue_per_transaction AS DIV0(SUM(revenue_amount), SUM(transactions))
      COMMENT = 'Average revenue per transaction in INR'
  )
  COMMENT = 'Semantic view for banking revenue analysis';

-- ── Phase 5: Python UDFs ────────────────────────────────────
CREATE OR REPLACE FUNCTION MCP_DEMO_DB.MCP_SCHEMA.calculate_compound_interest(
    principal FLOAT, rate FLOAT, years FLOAT
)
RETURNS FLOAT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
HANDLER = 'calc_interest'
AS
$$
def calc_interest(principal: float, rate: float, years: float) -> float:
    return round(principal * ((1 + rate / 100) ** years - 1), 2)
$$;

CREATE OR REPLACE FUNCTION MCP_DEMO_DB.MCP_SCHEMA.classify_customer_risk(
    age FLOAT, balance FLOAT, tenure_years FLOAT
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
HANDLER = 'classify_risk'
AS
$$
def classify_risk(age: float, balance: float, tenure_years: float) -> str:
    score = 0
    if age > 45: score += 1
    if balance > 500000: score += 2
    if tenure_years > 5: score += 1
    if score >= 3: return "Low Risk"
    elif score >= 1: return "Medium Risk"
    else: return "High Risk"
$$;

-- Verify UDFs
SELECT MCP_DEMO_DB.MCP_SCHEMA.calculate_compound_interest(100000, 7.5, 5);
-- Expected: 43562.93
SELECT MCP_DEMO_DB.MCP_SCHEMA.classify_customer_risk(50, 750000, 8);
-- Expected: "Low Risk"

-- ── Phase 6: MCP Server ─────────────────────────────────────
CREATE OR REPLACE MCP SERVER MCP_DEMO_DB.MCP_SCHEMA.mcp_banking_server
    FROM SPECIFICATION $$
    tools:
      - name: "product-search"
        type: "CORTEX_SEARCH_SERVICE_QUERY"
        identifier: "MCP_DEMO_DB.MCP_SCHEMA.PRODUCT_SEARCH_SERVICE"
        description: "Search banking products by description, category, or region"
        title: "Product Search"

      - name: "sql-executor"
        title: "SQL Execution Tool"
        type: "SYSTEM_EXECUTE_SQL"
        description: "Execute SQL queries against MCP_DEMO_DB"

      - name: "compound-interest-calculator"
        identifier: "MCP_DEMO_DB.MCP_SCHEMA.CALCULATE_COMPOUND_INTEREST"
        type: "GENERIC"
        description: "Calculate compound interest given principal, rate, and years"
        title: "Compound Interest Calculator"
        config:
          type: "function"
          warehouse: "SHARED_WH"
          input_schema:
            type: "object"
            properties:
              principal: { description: "Initial investment amount", type: "number" }
              rate: { description: "Annual interest rate in percentage", type: "number" }
              years: { description: "Number of years", type: "number" }

      - name: "customer-risk-classifier"
        identifier: "MCP_DEMO_DB.MCP_SCHEMA.CLASSIFY_CUSTOMER_RISK"
        type: "GENERIC"
        description: "Classify customer risk based on age, balance, and tenure"
        title: "Customer Risk Classifier"
        config:
          type: "function"
          warehouse: "SHARED_WH"
          input_schema:
            type: "object"
            properties:
              age: { description: "Customer age in years", type: "number" }
              balance: { description: "Account balance in INR", type: "number" }
              tenure_years: { description: "Years as a customer", type: "number" }
    $$;

SHOW MCP SERVERS IN SCHEMA MCP_DEMO_DB.MCP_SCHEMA;
DESCRIBE MCP SERVER MCP_DEMO_DB.MCP_SCHEMA.MCP_BANKING_SERVER;

-- ── Phase 7: Cortex Agent ───────────────────────────────────
CREATE OR REPLACE AGENT MCP_DEMO_DB.MCP_SCHEMA.banking_assistant
    COMMENT = 'Banking Customer Assistant'
    FROM SPECIFICATION
    $$
    models:
      orchestration: auto
    $$;

-- ── Phase 8: Authentication (PAT) ──────────────────────────
-- Replace SATISH with your Snowflake username
CREATE OR REPLACE AUTHENTICATION POLICY MCP_DEMO_DB.MCP_SCHEMA.mcp_pat_auth_policy
  PAT_POLICY = ( NETWORK_POLICY_EVALUATION = NOT_ENFORCED );
ALTER USER SATISH SET AUTHENTICATION POLICY MCP_DEMO_DB.MCP_SCHEMA.mcp_pat_auth_policy;

ALTER USER SATISH ADD PROGRAMMATIC ACCESS TOKEN mcp_test_token
  ROLE_RESTRICTION = 'MCP_CONSUMER_ROLE'
  DAYS_TO_EXPIRY = 1
  COMMENT = 'MCP Banking Server test token';

-- ── Phase 8b: OAuth (optional, for Claude Desktop) ─────────
CREATE OR REPLACE SECURITY INTEGRATION mcp_oauth_integration
    TYPE = OAUTH
    OAUTH_CLIENT = CUSTOM
    ENABLED = TRUE
    OAUTH_CLIENT_TYPE = 'CONFIDENTIAL'
    OAUTH_REDIRECT_URI = 'http://localhost:8080/callback'
    OAUTH_ISSUE_REFRESH_TOKENS = TRUE
    OAUTH_REFRESH_TOKEN_VALIDITY = 86400
    OAUTH_ALLOW_NON_TLS_REDIRECT_URI = TRUE;

SELECT SYSTEM$SHOW_OAUTH_CLIENT_SECRETS('MCP_OAUTH_INTEGRATION');

-- ── Phase 9: RBAC ───────────────────────────────────────────
CREATE ROLE IF NOT EXISTS MCP_CONSUMER_ROLE;

GRANT USAGE ON DATABASE MCP_DEMO_DB TO ROLE MCP_CONSUMER_ROLE;
GRANT USAGE ON SCHEMA MCP_DEMO_DB.MCP_SCHEMA TO ROLE MCP_CONSUMER_ROLE;
GRANT USAGE ON WAREHOUSE SHARED_WH TO ROLE MCP_CONSUMER_ROLE;
GRANT USAGE ON MCP SERVER MCP_DEMO_DB.MCP_SCHEMA.MCP_BANKING_SERVER
    TO ROLE MCP_CONSUMER_ROLE;
GRANT USAGE ON CORTEX SEARCH SERVICE
    MCP_DEMO_DB.MCP_SCHEMA.PRODUCT_SEARCH_SERVICE TO ROLE MCP_CONSUMER_ROLE;
GRANT USAGE ON FUNCTION
    MCP_DEMO_DB.MCP_SCHEMA.CALCULATE_COMPOUND_INTEREST(FLOAT, FLOAT, FLOAT)
    TO ROLE MCP_CONSUMER_ROLE;
GRANT USAGE ON FUNCTION
    MCP_DEMO_DB.MCP_SCHEMA.CLASSIFY_CUSTOMER_RISK(FLOAT, FLOAT, FLOAT)
    TO ROLE MCP_CONSUMER_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA MCP_DEMO_DB.MCP_SCHEMA
    TO ROLE MCP_CONSUMER_ROLE;
GRANT REFERENCES, SELECT ON SEMANTIC VIEW
    MCP_DEMO_DB.MCP_SCHEMA.REVENUE_SEMANTIC_VIEW TO ROLE MCP_CONSUMER_ROLE;
GRANT ROLE MCP_CONSUMER_ROLE TO USER SATISH;
