-- ============================================================
-- Banking Payment Risk Detection Platform
-- Unity Catalog Foundation
-- ============================================================

-- 1. Create project catalog
CREATE CATALOG IF NOT EXISTS banking_risk;


-- 2. Create Medallion schemas
CREATE SCHEMA IF NOT EXISTS banking_risk.bronze;

CREATE SCHEMA IF NOT EXISTS banking_risk.silver;

CREATE SCHEMA IF NOT EXISTS banking_risk.gold;


-- 3. Raw landing volume
CREATE VOLUME IF NOT EXISTS banking_risk.bronze.raw;


-- 4. Optional: separate quarantine volume
-- Used later for malformed / rejected source records
CREATE VOLUME IF NOT EXISTS banking_risk.bronze.quarantine;


-- 5. Optional: checkpoints volume
-- Used later for Structured Streaming checkpoints
CREATE VOLUME IF NOT EXISTS banking_risk.bronze.checkpoints;