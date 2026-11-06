BEGIN;

CREATE INDEX IF NOT EXISTS ix_abstract_customer_amo_customers_gin
    ON abstract_customer USING gin (amo_customers jsonb_path_ops) WITH (fastupdate = off);
CREATE INDEX IF NOT EXISTS ix_abstract_customer_amo_leads_gin
    ON abstract_customer USING gin (amo_leads jsonb_path_ops) WITH (fastupdate = off);

COMMIT;
