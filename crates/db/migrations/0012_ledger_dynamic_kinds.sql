ALTER TABLE ledger_entries DROP CONSTRAINT ledger_entries_kind_check;

ALTER TABLE ledger_entries
  ADD CONSTRAINT ledger_entries_kind_check CHECK(kind <> '');
