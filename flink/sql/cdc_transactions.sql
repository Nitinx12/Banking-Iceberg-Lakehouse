-- Flink CDC for transactions + accounts (stretch Phase 7)
-- Checkpoint 60s, exactly-once, RocksDB per Architecture 10.3
CREATE TABLE mongo_transactions_cdc ( _id STRING, transaction_id INT, amount DECIMAL(18,2), PRIMARY KEY(_id) NOT ENFORCED) WITH ('\''connector'\''='\''mongodb-cdc'\'', '\''hosts'\''='\''mongo:27017'\'', '\''checkpoint'\''='\''60s'\'');

