# HDFC Bank Lakehouse - Starting Fresh

This branch hdfc/bank-initial is reset for HDFC client.

## Structure (medallion - same pattern, new domain)
- 
otebooks/bronze/ - raw ingests for bank sources (transactions, accounts, customers CDC)
- 
otebooks/silver/ - cleaned + SCD2 + quality gate -> quarantine
- 
otebooks/gold/ - business aggregates (e.g., daily balances, NPA, churn)
- generator/ - bank data generator (replaces StreamFlix generators)
- src/core/ - KEEP: scd2.py, quality_checks.py, transformations.py (reusable)
- src/jobs/ - KEEP skeleton, will adapt for bank

## Next steps - tell me your HDFC dataset
1. What tables/files did client give? (e.g., accounts.csv, transactions CDC)
2. What business question for Gold? (e.g., daily active accounts, fraud)
