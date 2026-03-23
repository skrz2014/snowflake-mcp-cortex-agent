# 🏦 Production-Grade Snowflake AI Agent using MCP Server with Cortex AI

> A complete implementation guide for exposing Snowflake data through the Model Context Protocol — including Cortex Search, Cortex Analyst, Python UDFs, end-to-end authentication, and production monitoring.

**Author:** [Satish Kumar](https://www.linkedin.com/in/satishkumar-snowflake/) | [Medium Blog](https://medium.com/@snowflakechronicles)

---

## 📋 Table of Contents

- [What Is the Snowflake Managed MCP Server?](#what-is-the-snowflake-managed-mcp-server)
- [Why This Matters for Enterprise AI](#why-this-matters-for-enterprise-ai)
- [How MCP Works: Host, Client, Server](#how-mcp-works-host-client-server)
- [Architecture Overview](#architecture-overview)
- [Phase 1: Environment Setup](#phase-1-environment-setup)
- [Phase 2: Source Data](#phase-2-source-data)
- [Phase 3: Cortex Search Service](#phase-3-cortex-search-service)
- [Phase 4: Semantic View for Cortex Analyst](#phase-4-semantic-view-for-cortex-analyst)
- [Phase 5: Python UDFs](#phase-5-python-udfs)
- [Phase 6: Creating the MCP Server](#phase-6-creating-the-mcp-server)
- [Phase 7: Cortex Agent (for Snowsight)](#phase-7-cortex-agent-for-snowsight)
- [Phase 8: Authentication](#phase-8-authentication)
- [Phase 9: RBAC — Least Privilege Grants](#phase-9-rbac--least-privilege-grants)
- [Phase 10: Testing via curl](#phase-10-testing-via-curl)
- [Phase 11: Testing the Cortex Agent](#phase-11-testing-the-cortex-agent)
- [Phase 12: Claude Desktop Integration](#phase-12-claude-desktop-integration)
- [Phase 15: Monitoring](#phase-15-monitoring)
- [Phase 17: Automated Monitoring](#phase-17-automated-monitoring)
- [Teardown / Cleanup](#teardown--cleanup)
- [Objects Created](#objects-created)

---

## What Is the Snowflake Managed MCP Server?

The Snowflake managed MCP server is a native, fully hosted implementation of the Model Context Protocol that lives inside Snowflake instead of on external infrastructure you maintain.

### MCP in one sentence
The Model Context Protocol (MCP) is a standard way for AI agents to discover, call, and get results from tools through a uniform interface, instead of bespoke integrations for every system.

### What "managed MCP server" means in Snowflake
- The MCP server is created and managed as a first-class object inside Snowflake.
- You don't run any extra middleware or services; Snowflake hosts the server, scales it, and wires it to your data and functions.
- Compute, authentication, and governance all reuse Snowflake's existing mechanisms (roles, policies, warehouses).

### Tools it exposes at launch

| Tool | Description |
|------|-------------|
| **Cortex Analyst** | Natural language → SQL over your structured data |
| **Cortex Search** | Semantic search over unstructured or semi-structured content |
| **Python UDFs** | Custom business logic callable as agent tools |
| **SYSTEM_EXECUTE_SQL** | Low-level tool to run arbitrary SQL |

### Cost model
- The MCP server object itself does not add a new billing line item.
- You pay the usual underlying Snowflake costs: compute for warehouses, Cortex usage, and any storage or data processing those tools trigger.

---

## Why This Matters for Enterprise AI

Enterprises are moving from AI pilots to production, but safely connecting AI to proprietary data has historically been a critical barrier. The MCP server solves this by bringing AI access into the governance boundary rather than trying to export governed data out of it.

**Key benefits:**

- **Governance by design**: The same role-based access controls, masking policies, and row-level security that govern your data tables automatically govern your MCP server.
- **Zero integration overhead**: Any MCP-compatible client — Claude Desktop, LangGraph, CrewAI, Cursor — connects to the same endpoint without new development work.
- **Standards-based interoperability**: MCP is an open community standard — clients and servers are plug-and-play.
- **Trusted third-party data**: Cortex Knowledge Extensions from the Snowflake Marketplace (AP, Washington Post, MSCI, NASDAQ) with proper attribution built in.

---

## How MCP Works: Host, Client, Server

```
┌──────────────────────────────────────────────────┐
│  HOST  (e.g. Claude Desktop, LangGraph app)       │
│  ┌────────────────────────────────────────────┐   │
│  │  CLIENT  (maintains server connections)    │   │
│  └────────────┬───────────────────────────────┘   │
└───────────────│──────────────────────────────────┘
                │  JSON-RPC 2.0
                ▼
┌──────────────────────────────────────────────────┐
│  SERVER  (Snowflake Managed MCP Server)           │
│  - Surfaces tools and resources                   │
│  - Handles auth, execution, governance            │
└──────────────────────────────────────────────────┘
```

**Protocol flow:**
1. Client calls `tools/list` to discover available tools
2. Agent reasons about which tool fits the current task
3. Client calls `tools/call` with the chosen tool name and structured arguments
4. Server executes the request using Snowflake's infrastructure and returns results

---

## Architecture Overview

```
┌─────────────────────┐                    ┌──────────────────────────────────┐
│  EXTERNAL CLIENTS   │                    │  SNOWFLAKE MANAGED MCP SERVER    │
│  (Claude Desktop,   │  ──JSON-RPC 2.0──► │                                  │
│   LangGraph,        │    PAT / OAuth     │  1. Cortex Search (products)     │
│   Custom Apps)      │  ◄──────────────── │  2. SQL Executor (revenue)       │
└─────────────────────┘                    │  3. Compound Interest (UDF)      │
                                           │  4. Risk Classifier (UDF)        │
┌─────────────────────┐                    └──────────────────────────────────┘
│  SNOWFLAKE UI       │
│  (Snowsight)        │                    ┌──────────────────────────────────┐
│                     │  ─DATA_AGENT_RUN►  │  CORTEX AGENT                    │
│  Ask Cortex ────────│                    │  1. Cortex Search (products)     │
└─────────────────────┘                    │  2. Cortex Analyst (revenue)     │
                                           │  3. Compound Interest (UDF)      │
                                           │  4. Risk Classifier (UDF)        │
                                           └──────────────────────────────────┘
```

---

## Phase 1: Environment Setup

Start clean with a dedicated database and schema. Enable cross-region Cortex access if your account is in a region without full Cortex support.

```sql
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE SHARED_WH;

CREATE DATABASE IF NOT EXISTS MCP_DEMO_DB;
CREATE SCHEMA IF NOT EXISTS MCP_DEMO_DB.MCP_SCHEMA;
USE DATABASE MCP_DEMO_DB;
USE SCHEMA MCP_DEMO_DB.MCP_SCHEMA;
ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION';
```

---

## Phase 2: Source Data

Banking domain with two tables: `products` (for semantic search) and `revenue` (for analytical queries).

```sql
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
```

---

## Phase 3: Cortex Search Service

Cortex Search enables semantic (vector) search over your product descriptions. It automatically handles embedding generation, indexing, and retrieval.

```sql
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
```

> The `ON description` clause specifies which column gets embedded. `ATTRIBUTES` are filterable metadata fields that can be used to narrow search results at query time.

---

## Phase 4: Semantic View for Cortex Analyst

Cortex Analyst translates natural language into SQL. The semantic view maps business vocabulary to physical columns, defines synonyms, and specifies how metrics are calculated.

```sql
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
```

> **Tip:** Synonyms are what make AI feel truly intuitive. The more synonyms you define, the better your AI understands natural language variations.

---

## Phase 5: Python UDFs

Two domain-specific functions: a compound interest calculator and a customer risk classifier.

```sql
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
```

**Verify they work:**
```sql
SELECT MCP_DEMO_DB.MCP_SCHEMA.calculate_compound_interest(100000, 7.5, 5);
-- Expected: 43562.93

SELECT MCP_DEMO_DB.MCP_SCHEMA.classify_customer_risk(50, 750000, 8);
-- Expected: "Low Risk"
```

---

## Phase 6: Creating the MCP Server

The MCP server specification maps tool names to Snowflake objects and exposes them via the JSON-RPC protocol.

```sql
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
```

**Verify creation:**
```sql
SHOW MCP SERVERS IN SCHEMA MCP_DEMO_DB.MCP_SCHEMA;
DESCRIBE MCP SERVER MCP_DEMO_DB.MCP_SCHEMA.MCP_BANKING_SERVER;
```

---

## Phase 7: Cortex Agent (for Snowsight)

Powers the natural-language interface in Snowflake's UI.

```sql
CREATE OR REPLACE AGENT MCP_DEMO_DB.MCP_SCHEMA.banking_assistant
    COMMENT = 'Banking Customer Assistant'
    FROM SPECIFICATION
    $$
    models:
      orchestration: auto
    $$;
```

---

## Phase 8: Authentication

Snowflake supports two auth methods for MCP: **Programmatic Access Tokens (PAT)** for testing/automation, and **OAuth** for browser-based flows like Claude Desktop.

### Option A: Programmatic Access Token (PAT)

```sql
-- Create auth policy
CREATE OR REPLACE AUTHENTICATION POLICY MCP_DEMO_DB.MCP_SCHEMA.mcp_pat_auth_policy
  PAT_POLICY = ( NETWORK_POLICY_EVALUATION = NOT_ENFORCED );
ALTER USER SATISH SET AUTHENTICATION POLICY MCP_DEMO_DB.MCP_SCHEMA.mcp_pat_auth_policy;

-- Generate PAT (1-day expiry, scoped to MCP_CONSUMER_ROLE)
ALTER USER SATISH ADD PROGRAMMATIC ACCESS TOKEN mcp_test_token
  ROLE_RESTRICTION = 'MCP_CONSUMER_ROLE'
  DAYS_TO_EXPIRY = 1
  COMMENT = 'MCP Banking Server test token';
```

### Option B: OAuth (Claude Desktop / Browser)

```sql
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
```

---

## Phase 9: RBAC — Least Privilege Grants

> ⚠️ **Critical**: Granting USAGE on the MCP server is not enough — each underlying tool requires its own separate grant.

```sql
CREATE ROLE IF NOT EXISTS MCP_CONSUMER_ROLE;

-- Basic access
GRANT USAGE ON DATABASE MCP_DEMO_DB TO ROLE MCP_CONSUMER_ROLE;
GRANT USAGE ON SCHEMA MCP_DEMO_DB.MCP_SCHEMA TO ROLE MCP_CONSUMER_ROLE;
GRANT USAGE ON WAREHOUSE SHARED_WH TO ROLE MCP_CONSUMER_ROLE;

-- MCP server itself
GRANT USAGE ON MCP SERVER MCP_DEMO_DB.MCP_SCHEMA.MCP_BANKING_SERVER
    TO ROLE MCP_CONSUMER_ROLE;

-- Each tool separately - this is the part people forget
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
```

---

## Phase 10: Testing via curl

Test each tool directly with curl before wiring up a client.

> ⚠️ **URL Format Matters:** Use the org-account URL — no region: `https://inbpfyy-gvc29587.snowflakecomputing.com`. Including a region suffix in the hostname causes authentication failures.

```bash
ENDPOINT="https://inbpfyy-gvc29587.snowflakecomputing.com/api/v2/databases/MCP_DEMO_DB/schemas/MCP_SCHEMA/mcp-servers/MCP_BANKING_SERVER"
PAT="<your-pat-token>"

# List all tools (should return 4)
curl -X POST "$ENDPOINT" \
  -H "Authorization: Bearer $PAT" \
  -H "Content-Type: application/json" \
  -H "X-Snowflake-Authorization-Token-Type: PROGRAMMATIC_ACCESS_TOKEN" \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}'

# Product search
curl -X POST "$ENDPOINT" \
  -H "Authorization: Bearer $PAT" \
  -H "Content-Type: application/json" \
  -H "X-Snowflake-Authorization-Token-Type: PROGRAMMATIC_ACCESS_TOKEN" \
  -d '{"jsonrpc":"2.0","id":2,"method":"tools/call",
      "params":{"name":"product-search","arguments":{"query":"savings account","limit":3}}}'
# Expected: Premium Savings Account as top result

# Compound interest
curl -X POST "$ENDPOINT" \
  -H "Authorization: Bearer $PAT" \
  -H "Content-Type: application/json" \
  -H "X-Snowflake-Authorization-Token-Type: PROGRAMMATIC_ACCESS_TOKEN" \
  -d '{"jsonrpc":"2.0","id":3,"method":"tools/call",
      "params":{"name":"compound-interest-calculator",
               "arguments":{"principal":100000,"rate":7.5,"years":5}}}'
# Expected: 43562.93

# SQL executor — note "sql" key, not "statement"
curl -X POST "$ENDPOINT" \
  -H "Authorization: Bearer $PAT" \
  -H "Content-Type: application/json" \
  -H "X-Snowflake-Authorization-Token-Type: PROGRAMMATIC_ACCESS_TOKEN" \
  -d '{"jsonrpc":"2.0","id":4,"method":"tools/call",
      "params":{"name":"sql-executor",
               "arguments":{"sql":"SELECT * FROM revenue ORDER BY revenue_amount DESC LIMIT 3"}}}'
# Expected: Loans/Delhi=3.4M, Deposits/Chennai=2.1M, Banking/Mumbai=1.25M
```

---

## Phase 11: Testing the Cortex Agent

Test all four tools via `DATA_AGENT_RUN()`:

```sql
-- Revenue question → routes to Cortex Analyst tool
SELECT TRY_PARSE_JSON(
  SNOWFLAKE.CORTEX.DATA_AGENT_RUN(
    'MCP_DEMO_DB.MCP_SCHEMA.BANKING_ASSISTANT',
    $${ "messages": [{"role": "user", "content":
        [{"type": "text", "text": "Show me total revenue by region"}]}],
        "stream": false }$$
  )
) AS response;

-- Calculator → routes to compound_interest UDF
SELECT TRY_PARSE_JSON(
  SNOWFLAKE.CORTEX.DATA_AGENT_RUN(
    'MCP_DEMO_DB.MCP_SCHEMA.BANKING_ASSISTANT',
    $${ "messages": [{"role": "user", "content":
        [{"type": "text", "text": "Calculate compound interest on 100000 at 7.5% for 5 years"}]}],
        "stream": false }$$
  )
) AS response;
-- Expected: 43562.93

-- Risk classifier
SELECT TRY_PARSE_JSON(
  SNOWFLAKE.CORTEX.DATA_AGENT_RUN(
    'MCP_DEMO_DB.MCP_SCHEMA.BANKING_ASSISTANT',
    $${ "messages": [{"role": "user", "content":
        [{"type": "text", "text": "What is the risk for a 50 year old with 750000 balance and 8 years tenure?"}]}],
        "stream": false }$$
  )
) AS response;
-- Expected: "Low Risk"
```

---

## Phase 12: Claude Desktop Integration

Add the MCP server to your Claude Desktop config. The desktop app handles the OAuth flow automatically via a browser popup.

```json
// ~/Library/Application Support/Claude/claude_desktop_config.json
{
  "mcpServers": {
    "snowflake-banking": {
      "url": "https://inbpfyy-gvc29587.snowflakecomputing.com/api/v2/databases/MCP_DEMO_DB/schemas/MCP_SCHEMA/mcp-servers/MCP_BANKING_SERVER"
    }
  }
}
```

After restarting Claude Desktop, all four Snowflake tools appear in the tools panel. You can then ask Claude things like:
- *"search for savings products"*
- *"what's the compound interest on 500,000 at 8% for 10 years"*
- *"what's the risk profile for a 45-year-old with 600,000 balance?"*

---

## Phase 15: Monitoring

Once your agent is live, you need visibility into how it's being used. Snowflake provides `SNOWFLAKE.ACCOUNT_USAGE.CORTEX_AGENT_USAGE_HISTORY` as the primary monitoring view.

> **Data Latency:** `CORTEX_AGENT_USAGE_HISTORY` may take up to 2 hours to populate after agent invocations.

### 15a. Overview: All Agent Invocations

```sql
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
```

### 15b. Response Time Analytics: avg / p50 / p95 / max by Day

```sql
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
```

### 15c. Token & Credit Consumption: Daily Trend

```sql
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
```

### 15d. Tool-Level Breakdown

```sql
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
```

### 15e. User Activity: Requests Per User

```sql
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
```

### 15f. Slow Requests: Queries Exceeding 10 Seconds

```sql
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
```

### 15g. Hourly Heatmap: Request Volume by Hour

```sql
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
```

### 15h. Credit Cost Breakdown by Model and Service Type

```sql
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
```

### 15i. Role-Based Access Audit

```sql
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
```

### 15j. Real-Time Fallback: QUERY_HISTORY

When `CORTEX_AGENT_USAGE_HISTORY` hasn't populated yet (within the first 2 hours after testing):

```sql
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
```

### 15k. Real-Time: Cortex Search Serving Credits

```sql
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
```

### 15l. Daily Summary Dashboard

```sql
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
```

### Monitoring Coverage Summary

| Query | Coverage | Latency |
|-------|----------|---------|
| 15a | Raw invocation log | 2h |
| 15b | Latency percentiles (avg/p50/p95/max) | 2h |
| 15c | Daily token & credit spend | 2h |
| 15d | Tool-level invocation breakdown | 2h |
| 15e | User adoption leaderboard | 2h |
| 15f | SLA violation tracking (>10s) | 2h |
| 15g | Hourly usage heatmap | 2h |
| 15h | Granular cost by model/service | 2h |
| 15i | Role-based access audit | 2h |
| 15j | Real-time fallback via QUERY_HISTORY | Immediate |
| 15k | Cortex Search serving credits | Immediate |
| 15l | Executive one-liner dashboard | 2h |

---

## Phase 17: Automated Monitoring — Stored Procedure & Scheduled Tasks

### 17a. Snapshot Table

```sql
CREATE OR REPLACE TABLE MCP_DEMO_DB.MCP_SCHEMA.agent_monitoring_snapshots (
    snapshot_id    NUMBER AUTOINCREMENT,
    snapshot_time  TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    report_name    VARCHAR,
    lookback_days  NUMBER,
    report_data    VARIANT
);
```

### 17b. Stored Procedure

```sql
CREATE OR REPLACE PROCEDURE MCP_DEMO_DB.MCP_SCHEMA.sp_agent_monitoring_report(LOOKBACK_DAYS FLOAT)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    -- 1. DAILY_SUMMARY
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

    -- 2. RESPONSE_TIME_ANALYTICS
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

    -- 3. TOKEN_CONSUMPTION
    INSERT INTO MCP_DEMO_DB.MCP_SCHEMA.agent_monitoring_snapshots
        (report_name, lookback_days, report_data)
    SELECT 'TOKEN_CONSUMPTION', :LOOKBACK_DAYS,
        OBJECT_CONSTRUCT(
            'day', day, 'total_requests', total_requests,
            'total_tokens', total_tokens,
            'total_credits', total_credits,
            'avg_tokens_per_request', avg_tokens_per_request,
            'avg_credits_per_request', avg_credits_per_request
        )
    FROM (
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
          AND START_TIME >= DATEADD('day', -:LOOKBACK_DAYS, CURRENT_TIMESTAMP())
        GROUP BY day
    );

    -- 4. USER_ACTIVITY
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

    -- 5. REALTIME_INVOCATIONS
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

    -- 6. CORTEX_SEARCH_CREDITS
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

    -- 7. ROLE_AUDIT
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
```

### 17c. Manual Execution

```sql
CALL MCP_DEMO_DB.MCP_SCHEMA.sp_agent_monitoring_report(1);   -- last 24 hours
CALL MCP_DEMO_DB.MCP_SCHEMA.sp_agent_monitoring_report(7);   -- last 7 days
CALL MCP_DEMO_DB.MCP_SCHEMA.sp_agent_monitoring_report(30);  -- last 30 days
```

### 17d. Scheduled Tasks

```sql
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

-- Tasks are created in SUSPENDED state - resume them explicitly
ALTER TASK MCP_DEMO_DB.MCP_SCHEMA.TASK_DAILY_MONITORING_REPORT RESUME;
ALTER TASK MCP_DEMO_DB.MCP_SCHEMA.TASK_WEEKLY_MONITORING_REPORT RESUME;
```

### 17e. Query Snapshots

```sql
SELECT report_name, lookback_days, report_data, snapshot_time
FROM MCP_DEMO_DB.MCP_SCHEMA.agent_monitoring_snapshots
ORDER BY snapshot_time DESC
LIMIT 50;
```

### 17f. Flatten Snapshots for Tabular Views

```sql
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
```

### 17g. Retention Policy

```sql
DELETE FROM MCP_DEMO_DB.MCP_SCHEMA.agent_monitoring_snapshots
WHERE snapshot_time < DATEADD('day', -90, CURRENT_TIMESTAMP());
```

---

## Teardown / Cleanup

```sql
-- Suspend tasks before dropping
ALTER TASK MCP_DEMO_DB.MCP_SCHEMA.TASK_DAILY_MONITORING_REPORT SUSPEND;
ALTER TASK MCP_DEMO_DB.MCP_SCHEMA.TASK_WEEKLY_MONITORING_REPORT SUSPEND;

-- Drop tasks
DROP TASK IF EXISTS MCP_DEMO_DB.MCP_SCHEMA.TASK_DAILY_MONITORING_REPORT;
DROP TASK IF EXISTS MCP_DEMO_DB.MCP_SCHEMA.TASK_WEEKLY_MONITORING_REPORT;
DROP PROCEDURE IF EXISTS MCP_DEMO_DB.MCP_SCHEMA.SP_AGENT_MONITORING_REPORT(FLOAT);
DROP TABLE IF EXISTS MCP_DEMO_DB.MCP_SCHEMA.AGENT_MONITORING_SNAPSHOTS;

-- Auth & integrations
ALTER USER SATISH DROP PROGRAMMATIC ACCESS TOKEN mcp_test_token;
ALTER USER SATISH UNSET AUTHENTICATION POLICY;
DROP AUTHENTICATION POLICY IF EXISTS MCP_DEMO_DB.MCP_SCHEMA.MCP_PAT_AUTH_POLICY;
DROP SECURITY INTEGRATION IF EXISTS MCP_OAUTH_INTEGRATION;

-- Core objects (reverse dependency order)
DROP AGENT IF EXISTS MCP_DEMO_DB.MCP_SCHEMA.BANKING_ASSISTANT;
DROP MCP SERVER IF EXISTS MCP_DEMO_DB.MCP_SCHEMA.MCP_BANKING_SERVER;
DROP SEMANTIC VIEW IF EXISTS MCP_DEMO_DB.MCP_SCHEMA.REVENUE_SEMANTIC_VIEW;
DROP CORTEX SEARCH SERVICE IF EXISTS MCP_DEMO_DB.MCP_SCHEMA.PRODUCT_SEARCH_SERVICE;
DROP FUNCTION IF EXISTS MCP_DEMO_DB.MCP_SCHEMA.CALCULATE_COMPOUND_INTEREST(FLOAT, FLOAT, FLOAT);
DROP FUNCTION IF EXISTS MCP_DEMO_DB.MCP_SCHEMA.CLASSIFY_CUSTOMER_RISK(FLOAT, FLOAT, FLOAT);
DROP TABLE IF EXISTS MCP_DEMO_DB.MCP_SCHEMA.PRODUCTS;
DROP TABLE IF EXISTS MCP_DEMO_DB.MCP_SCHEMA.REVENUE;
DROP ROLE IF EXISTS MCP_CONSUMER_ROLE;
DROP SCHEMA IF EXISTS MCP_DEMO_DB.MCP_SCHEMA;
DROP DATABASE IF EXISTS MCP_DEMO_DB;
```

---

## Objects Created

| Object Type | Name / Details |
|-------------|----------------|
| Database | MCP_DEMO_DB |
| Schema | MCP_DEMO_DB.MCP_SCHEMA |
| Tables | products (5 rows), revenue (7 rows) |
| Cortex Search | PRODUCT_SEARCH_SERVICE |
| Semantic View | REVENUE_SEMANTIC_VIEW |
| UDFs | CALCULATE_COMPOUND_INTEREST, CLASSIFY_CUSTOMER_RISK |
| MCP Server | MCP_BANKING_SERVER (4 tools) |
| Cortex Agent | BANKING_ASSISTANT (4 tools) |
| Security | MCP_OAUTH_INTEGRATION, MCP_PAT_AUTH_POLICY |
| Roles | MCP_CONSUMER_ROLE |
| PAT | MCP_TEST_TOKEN (SATISH, 1-day expiry) |
| Snapshot Table | AGENT_MONITORING_SNAPSHOTS |
| Stored Procedure | SP_AGENT_MONITORING_REPORT(FLOAT) |
| Tasks | TASK_DAILY_MONITORING_REPORT, TASK_WEEKLY_MONITORING_REPORT |

---

## Connect & Follow

If this walkthrough helped, connect with the author for more Snowflake content:

- 🔗 **LinkedIn:** [Satish Kumar — Snowflake](https://www.linkedin.com/in/satishkumar-snowflake/)
- ✍️ **Medium:** [@snowflakechronicles](https://medium.com/@snowflakechronicles)

---

## Tags

`#Snowflake` `#MCP` `#ModelContextProtocol` `#AIAgents` `#CortexAI` `#DataEngineering` `#GenerativeAI` `#LLM` `#EnterpriseAI` `#Python`
