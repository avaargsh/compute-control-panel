-- Make DRA source-key attestation atomic with a signed journal append.
-- PostgreSQL's FK insertion into dra_unprepare_source_revocations obtains a
-- KEY SHARE lock on the referenced source key. Restricted receipt writes
-- must hold FOR UPDATE on that SAME source row from authentication until
-- journal INSERT transaction commit. Revocation then cannot overtake ingest.
--
-- FOR UPDATE requires the writer to possess an UPDATE privilege in PG16.
-- The only granted column is an inert lock token, NOT the public key,
-- scope, lifetime or other source authority. The existing ENABLE ALWAYS
-- immutable trigger rejects *every* UPDATE regardless of this grant.
-- Historical migrations 0001..0018 remain unchanged.

ALTER TABLE public.dra_unprepare_source_keys
  ADD COLUMN lock_token SMALLINT NOT NULL DEFAULT 0
  CONSTRAINT dra_unprepare_source_lock_token_zero CHECK (lock_token = 0);

GRANT UPDATE (lock_token) ON public.dra_unprepare_source_keys
  TO ccp_dra_receipt_ingest;

COMMENT ON COLUMN public.dra_unprepare_source_keys.lock_token IS
  'Inert PG16 FOR UPDATE lock privilege only; immutable trigger rejects actual UPDATE. Serializes revocation against receipt ingest.';
