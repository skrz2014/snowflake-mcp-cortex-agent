#!/usr/bin/env bash
# ============================================================
# Snowflake MCP Banking Agent — curl Test Script
# Author: Satish Kumar
# LinkedIn: https://www.linkedin.com/in/satishkumar-snowflake/
# Medium:   https://medium.com/@snowflakechronicles
# ============================================================
# Usage:
#   export SNOWFLAKE_ACCOUNT="inbpfyy-gvc29587"   # org-account, no region suffix
#   export PAT_TOKEN="<your-programmatic-access-token>"
#   chmod +x test_mcp.sh && ./test_mcp.sh
# ============================================================

set -euo pipefail

ACCOUNT="${SNOWFLAKE_ACCOUNT:-inbpfyy-gvc29587}"
PAT="${PAT_TOKEN:?Set PAT_TOKEN env variable}"
ENDPOINT="https://${ACCOUNT}.snowflakecomputing.com/api/v2/databases/MCP_DEMO_DB/schemas/MCP_SCHEMA/mcp-servers/MCP_BANKING_SERVER"

AUTH_HEADERS=(
  -H "Authorization: Bearer $PAT"
  -H "Content-Type: application/json"
  -H "X-Snowflake-Authorization-Token-Type: PROGRAMMATIC_ACCESS_TOKEN"
)

echo "=== 1. List Tools (expect 4) ==="
curl -s -X POST "$ENDPOINT" "${AUTH_HEADERS[@]}" \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}' | jq .

echo ""
echo "=== 2. Product Search (savings account) ==="
curl -s -X POST "$ENDPOINT" "${AUTH_HEADERS[@]}" \
  -d '{"jsonrpc":"2.0","id":2,"method":"tools/call",
      "params":{"name":"product-search","arguments":{"query":"savings account","limit":3}}}' | jq .

echo ""
echo "=== 3. Compound Interest (100000 @ 7.5% for 5 years → 43562.93) ==="
curl -s -X POST "$ENDPOINT" "${AUTH_HEADERS[@]}" \
  -d '{"jsonrpc":"2.0","id":3,"method":"tools/call",
      "params":{"name":"compound-interest-calculator",
               "arguments":{"principal":100000,"rate":7.5,"years":5}}}' | jq .

echo ""
echo "=== 4. Risk Classifier (age=50, balance=750000, tenure=8 → Low Risk) ==="
curl -s -X POST "$ENDPOINT" "${AUTH_HEADERS[@]}" \
  -d '{"jsonrpc":"2.0","id":4,"method":"tools/call",
      "params":{"name":"customer-risk-classifier",
               "arguments":{"age":50,"balance":750000,"tenure_years":8}}}' | jq .

echo ""
echo "=== 5. SQL Executor (top 3 revenue rows) ==="
curl -s -X POST "$ENDPOINT" "${AUTH_HEADERS[@]}" \
  -d '{"jsonrpc":"2.0","id":5,"method":"tools/call",
      "params":{"name":"sql-executor",
               "arguments":{"sql":"SELECT * FROM revenue ORDER BY revenue_amount DESC LIMIT 3"}}}' | jq .

echo ""
echo "All tests complete!"
