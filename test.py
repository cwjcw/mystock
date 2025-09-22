"""Quick check for AKShare fund-flow ranking with proxy override.

Disable any system proxy to avoid connection resets before calling the API.
"""

import os

import akshare as ak

# Disable proxies to minimise request failures during local testing
for key in ["http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY"]:
    os.environ.pop(key, None)
os.environ.setdefault("NO_PROXY", "localhost,127.0.0.1")

stock_individual_fund_flow_rank_df = ak.stock_individual_fund_flow_rank(indicator="今日")
print(stock_individual_fund_flow_rank_df.head())
stock_individual_fund_flow_rank_df.to_csv("test.csv", index=False)
