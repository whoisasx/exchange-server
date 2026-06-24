ALTER TABLE ledger_events
ADD COLUMN logical_event_id TEXT;

ALTER TABLE ledger_events
ADD CONSTRAINT ledger_events_logical_event_id_non_empty
CHECK(logical_event_id IS NULL OR logical_event_id <> '');

CREATE UNIQUE INDEX ledger_events_logical_event_id_idx
ON ledger_events(logical_event_id)
WHERE logical_event_id IS NOT NULL;
