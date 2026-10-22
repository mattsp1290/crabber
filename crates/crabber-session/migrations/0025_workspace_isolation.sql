-- Workspace isolation. Trigger/function inventory (32 existing functions plus
-- the invoker-security raw session birth guard introduced below).
-- Function | reads | writes | classification and policy dependency
-- snapshot_record_guard | runs | sessions | same-scope
-- reserve_tool_call_identity | tool_call_identities | tool_call_identities | writes-other; role-stamped identities policy
-- release_tool_call_identity | tool_call_identities | tool_call_identities | writes-other; role-stamped identities policy
-- crabber_immutable_ownership_protocol | NEW/OLD only | NEW/OLD only | same-scope
-- crabber_guard_deleted_session | session_tombstones | NEW/OLD only | global-read; tombstone SELECT exception
-- crabber_lock_tombstone | NEW/OLD only | NEW/OLD only | same-scope
-- crabber_immutable_tombstone | NEW/OLD only | NEW/OLD only | same-scope
-- crabber_immutable_preparation_admission | NEW/OLD only | NEW/OLD only | same-scope
-- crabber_immutable_execution_protocol | NEW/OLD only | NEW/OLD only | same-scope
-- crabber_protect_execution_source | session_tombstones | NEW/OLD only | global-read; tombstone SELECT exception
-- crabber_owner_protocol_immutable | NEW/OLD only | NEW/OLD only | same-scope
-- crabber_owner_publication_guard | session_tombstones | NEW/OLD only | global-read; tombstone SELECT exception
-- crabber_owner_cohort_guard | session_owner_publications, session_tombstones | NEW/OLD only | global-read; tombstone SELECT exception
-- crabber_owner_scope_marker | NEW/OLD only | NEW/OLD only | same-scope
-- crabber_owner_context_marker | NEW/OLD only | NEW/OLD only | same-scope
-- crabber_owner_scope_live | session_tombstones | NEW/OLD only | global-read; tombstone SELECT exception
-- crabber_native_session_identity | session_tombstones | NEW/OLD only | global-read; tombstone SELECT exception
-- crabber_native_context_identity | session_tombstones | NEW/OLD only | global-read; tombstone SELECT exception
-- crabber_native_run_identity | session_tombstones, sessions | NEW/OLD only | global-read; tombstone SELECT exception
-- crabber_native_source_guard | native_run_source_heads, native_run_sources, session_tombstones | NEW/OLD only | global-read; tombstone SELECT exception
-- crabber_native_head_guard | native_run_sources, session_tombstones | NEW/OLD only | global-read; tombstone SELECT exception
-- crabber_publish_native_source | NEW/OLD only | native_run_source_heads | writes-other; same-session native heads policy
-- crabber_continuation_input_guard | session_tombstones | NEW/OLD only | global-read; tombstone SELECT exception
-- crabber_approval_input_identity | session_tombstones | NEW/OLD only | global-read; tombstone SELECT exception
-- crabber_tool_input_identity | session_tombstones | NEW/OLD only | global-read; tombstone SELECT exception
-- crabber_queue_identity | future_queue, session_tombstones | NEW/OLD only | global-read; tombstone SELECT exception
-- crabber_queue_source | queue_publication_cohorts, session_tombstones | NEW/OLD only | global-read; tombstone SELECT exception
-- crabber_queue_cohort | queue_publication_sources, session_tombstones | NEW/OLD only | global-read; tombstone SELECT exception
-- crabber_execution_run_identity | session_tombstones | NEW/OLD only | global-read; tombstone SELECT exception
-- crabber_execution_source_guard | session_tombstones | NEW/OLD only | global-read; tombstone SELECT exception
-- crabber_pending_initial_guard | pending_initial_closes, pending_initial_publications, session_tombstones | NEW/OLD only | global-read; tombstone SELECT exception
-- crabber_initial_consumer_guard | initial_consumer_closes, initial_consumer_origins, session_tombstones | NEW/OLD only | global-read; tombstone SELECT exception
-- crabber_guard_session_birth | session_owner_contexts | session_owner_contexts (rolled-back probe only) | writes-other; own-scope visible root or globally unoccupied key
-- All functions remain invoker-security; crabber_session_born below is the
-- sole definer helper and answers only session existence with a pinned path.
-- ON CONFLICT inventory (same-scope upserts require both USING and WITH CHECK):
-- postgres/approval/suspend.rs:60 continuation_barriers: same-scope upsert; hidden target fails closed under RLS.
-- postgres/execution.rs:419 extension_state: same-scope upsert; hidden target fails closed under RLS.
-- postgres/management.rs sessions: same-scope upsert; hidden target fails closed under RLS.
-- postgres/management.rs:183 session_management: same-scope upsert; hidden target fails closed under RLS.
-- postgres/public_state.rs:12 session_public_state: same-scope upsert; hidden target fails closed under RLS.
-- postgres/run_control/suspension.rs:28 run_suspension_proofs: same-scope upsert; hidden target fails closed under RLS.
-- postgres/session_owner/reads.rs:313 session_owner_contexts: DO NOTHING re-read fails closed on a hidden context.
-- postgres/session_owner/reads.rs:316 session_owner_current: same-scope upsert; hidden target fails closed under RLS.
-- postgres/tool_continuation/suspend.rs:13 tool_continuation_barriers: same-scope upsert; hidden target fails closed under RLS.
-- sessions_workspace_birth trigger: rolled-back DO NOTHING probe; hidden context rejects birth.
-- tool identity trigger: DO NOTHING plus locked re-read rejects a hidden id.
-- Migration schema_version inserts are owner-only and monotonic.
-- Owner publication cohorts/publications/scope origins use pre-birth scope:
-- reserve_session_owner publishes them before any sessions row exists.

ALTER TABLE sessions ADD COLUMN IF NOT EXISTS workspace_id text GENERATED ALWAYS AS (data->>'workspace_id') STORED;
CREATE INDEX IF NOT EXISTS sessions_workspace ON sessions(workspace_id, id);
ALTER TABLE session_owner_contexts ADD COLUMN IF NOT EXISTS workspace_id text GENERATED ALWAYS AS ((data_json::jsonb)->>'workspace') STORED;
CREATE INDEX IF NOT EXISTS session_owner_contexts_workspace ON session_owner_contexts(workspace_id, session_id);
CREATE TABLE crabber_workspace_grants (
 -- PostgreSQL role names and CURRENT_USER have C collation. The key must
 -- match it so correlated role/workspace equality can use this primary key.
 role_name text COLLATE "C" NOT NULL CHECK(octet_length(role_name) BETWEEN 1 AND 63),
 workspace_id text NOT NULL, PRIMARY KEY(role_name,workspace_id)
);
DO $$ BEGIN
 IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='crabber_worker') THEN CREATE ROLE crabber_worker NOLOGIN; END IF;
 IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='crabber_host') THEN CREATE ROLE crabber_host NOLOGIN; END IF;
EXCEPTION WHEN insufficient_privilege THEN
 RAISE EXCEPTION 'crabber_roles_missing' USING HINT='create NOLOGIN roles crabber_worker and crabber_host before migrating';
END $$;
DO $$ BEGIN
 EXECUTE format('CREATE FUNCTION %I.crabber_session_born(text) RETURNS boolean LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS %L', current_schema(), format('SELECT EXISTS(SELECT 1 FROM %I.sessions WHERE id = $1)',current_schema()));
END $$;
REVOKE ALL ON FUNCTION crabber_session_born(text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION crabber_session_born(text) TO crabber_worker, crabber_host;
DO $$ BEGIN
 EXECUTE format('GRANT USAGE ON SCHEMA %I TO crabber_worker, crabber_host',current_schema());
 EXECUTE format('GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA %I TO crabber_worker, crabber_host',current_schema());
 EXECUTE format('GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA %I TO crabber_worker, crabber_host',current_schema());
 EXECUTE format('REVOKE CREATE ON SCHEMA %I FROM PUBLIC, crabber_worker, crabber_host',current_schema());
 IF has_schema_privilege('crabber_worker',current_schema(),'CREATE') OR has_schema_privilege('crabber_host',current_schema(),'CREATE') THEN
  RAISE EXCEPTION 'crabber_schema_not_owned';
 END IF;
EXCEPTION WHEN insufficient_privilege THEN
 RAISE EXCEPTION 'crabber_schema_not_owned' USING HINT='revoke public schema CREATE as schema owner, or migrate into a schema the migrating role owns';
END $$;
REVOKE INSERT, UPDATE, DELETE ON schema_version, snapshot_auth FROM crabber_worker, crabber_host;
REVOKE INSERT, UPDATE, DELETE ON crabber_workspace_grants FROM crabber_worker;
REVOKE UPDATE, DELETE ON session_tombstones FROM crabber_worker, crabber_host;
ALTER TABLE tool_call_identities ADD COLUMN IF NOT EXISTS owner_role text NOT NULL DEFAULT current_user;

-- Keep tenant membership checks correlated to the requested row's primary key.
-- PostgreSQL14 can replace plain EXISTS with hashed whole-relation alternatives;
-- OFFSET 0 prevents that transformation without changing the authorization test.
-- Qualify every outer column: an unqualified inner name could become a tautology.
-- CURRENT_USER::text keeps the role comparison in the grant key's text type.
-- Grant-roster scans remain a planner choice for tiny rosters; tenant parents
-- must use indexed equality probes rather than materializing visible relations.
-- Classification uses the migration's explicitly enumerated table roster. A new
-- unclassified table fails the coverage test rather than inheriting privileges.
DO $$
DECLARE t text; predicate text; has_session boolean; has_run boolean;
BEGIN
 FOREACH t IN ARRAY ARRAY[
  'abandonment_commits',
  'admission_execution_sources',
  'admission_executions',
  'admission_receipts',
  'approval_input_cohorts',
  'approval_input_sources',
  'continuation_barriers',
  'continuation_interactions',
  'continuations',
  'epochs',
  'events',
  'execution_attempts',
  'extension_state',
  'frontend_tool_origins',
  'future_queue',
  'future_queue_audit',
  'imported_message_provenance',
  'imported_tool_calls',
  'inbox',
  'initial_consumer_assignments',
  'initial_consumer_calls',
  'initial_consumer_closes',
  'initial_consumer_cohorts',
  'initial_consumer_heads',
  'initial_consumer_origins',
  'initial_consumer_publications',
  'initial_consumer_results',
  'initial_consumer_reverse',
  'initial_consumer_roster',
  'messages',
  'native_execution_origins',
  'native_execution_unclassified',
  'native_parent_witnesses',
  'native_run_issuances',
  'native_run_source_heads',
  'native_run_sources',
  'operation_receipts',
  'parts',
  'pending_initial_assignments',
  'pending_initial_closes',
  'pending_initial_cohorts',
  'pending_initial_epochs',
  'pending_initial_heads',
  'pending_initial_publications',
  'pending_initial_registered_completes',
  'pending_initial_source_slots',
  'preparation_admission_witnesses',
  'queue_publication_cohorts',
  'queue_publication_sources',
  'run_control_progress',
  'run_controls',
  'run_execution_bindings',
  'run_suspension_proofs',
  'runs',
  'session_fork_audit',
  'session_invocations',
  'session_management',
  'session_management_audit',
  'session_owner_audits',
  'session_owner_contexts',
  'session_owner_current',
  'session_owner_publication_cohorts',
  'session_owner_publications',
  'session_owner_scope_origins',
  'session_ownership_origins',
  'session_public_state',
  'session_tombstones',
  'sessions',
  'tool_call_identities',
  'tool_calls',
  'tool_continuation_barriers',
  'tool_continuation_frontend_calls',
  'tool_continuation_interactions',
  'tool_continuation_phases',
  'tool_continuations',
  'tool_input_cohorts',
  'tool_input_sources',
  'tool_phases',
  'crabber_workspace_grants'
 ] LOOP
  EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY',t);
  EXECUTE format('CREATE POLICY crabber_host_all ON %I FOR ALL TO crabber_host USING (true) WITH CHECK (true)',t);
  IF t='crabber_workspace_grants' THEN
   EXECUTE format('CREATE POLICY crabber_worker_read ON %I FOR SELECT TO crabber_worker USING (role_name=current_user::text)',t);
   CONTINUE;
  ELSIF t='session_tombstones' THEN
   EXECUTE format('CREATE POLICY crabber_worker_read ON %I FOR SELECT TO crabber_worker USING (true)',t);
   EXECUTE format('CREATE POLICY crabber_worker_insert ON %I FOR INSERT TO crabber_worker WITH CHECK (session_id IN (SELECT id FROM sessions))',t);
   CONTINUE;
  ELSIF t='sessions' THEN
   predicate := 'EXISTS (SELECT 1 FROM crabber_workspace_grants g WHERE g.role_name=current_user::text AND g.workspace_id=sessions.workspace_id OFFSET 0)';
  ELSIF t='session_owner_contexts' THEN
   predicate := 'EXISTS (SELECT 1 FROM crabber_workspace_grants g WHERE g.role_name=current_user::text AND g.workspace_id=session_owner_contexts.workspace_id OFFSET 0) AND (NOT crabber_session_born(session_owner_contexts.session_id) OR EXISTS (SELECT 1 FROM sessions s WHERE s.id=session_owner_contexts.session_id OFFSET 0))';
  ELSIF t LIKE 'pending_initial_%' OR t LIKE 'initial_consumer_%' OR t IN ('session_owner_current','session_owner_audits','session_owner_publication_cohorts','session_owner_publications','session_owner_scope_origins') THEN
   predicate := format('EXISTS (SELECT 1 FROM sessions s WHERE s.id=%I.session_id OFFSET 0) OR (NOT crabber_session_born(%I.session_id) AND EXISTS (SELECT 1 FROM session_owner_contexts c WHERE c.session_id=%I.session_id OFFSET 0))',t,t,t);
  ELSIF t='tool_call_identities' THEN
   predicate := 'owner_role=current_user';
  ELSIF t='parts' THEN
   predicate := 'EXISTS (SELECT 1 FROM messages m WHERE m.id=parts.message_id OFFSET 0)';
  ELSE
   SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema=current_schema() AND table_name=t AND column_name='session_id'),
          EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema=current_schema() AND table_name=t AND column_name='run_id') INTO has_session,has_run;
   IF has_session AND has_run THEN
    predicate := format('EXISTS (SELECT 1 FROM sessions s WHERE s.id=%I.session_id OFFSET 0) AND (%I.run_id IS NULL OR EXISTS (SELECT 1 FROM runs r WHERE r.id=%I.run_id OFFSET 0))',t,t,t);
   ELSIF has_session THEN predicate := format('EXISTS (SELECT 1 FROM sessions s WHERE s.id=%I.session_id OFFSET 0)',t);
   ELSIF has_run THEN predicate := format('EXISTS (SELECT 1 FROM runs r WHERE r.id=%I.run_id OFFSET 0)',t);
   ELSE RAISE EXCEPTION 'crabber_unclassified_isolation_table';
   END IF;
  END IF;
  EXECUTE format('CREATE POLICY crabber_worker_rw ON %I FOR ALL TO crabber_worker USING (%s) WITH CHECK (%s)',t,predicate,predicate);
 END LOOP;
END $$;
-- A worker cannot adopt another workspace's pre-birth ledger by creating its
-- sessions row first. RLS alone cannot see a hidden occupied context key; the
-- speculative insert proves occupancy, then rolls back every probe effect.
CREATE FUNCTION crabber_guard_session_birth() RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp AS $$
DECLARE visible_workspace text; inserted integer := 0;
BEGIN
 -- Trusted owner/host SQL keeps the same bypass as the RLS policy model.
 IF NOT row_security_active(TG_RELID) THEN RETURN NEW; END IF;
 IF pg_has_role(current_user,'crabber_host','USAGE') THEN RETURN NEW; END IF;
 -- Nested existing invoker triggers need this schema too. The function SET
 -- option restores the caller's search_path on exit, including exceptions.
 PERFORM set_config('search_path',format('pg_catalog,%I,pg_temp',TG_TABLE_SCHEMA),true);
 PERFORM pg_advisory_xact_lock(hashtextextended(NEW.id,0));
 -- Generated columns are not populated yet in BEFORE INSERT; read NEW.data.
 -- Qualify every relation with the trigger's schema to prevent temp shadowing.
 EXECUTE format('SELECT workspace_id FROM %I.session_owner_contexts WHERE session_id=$1',TG_TABLE_SCHEMA)
  INTO visible_workspace USING NEW.id;
 IF visible_workspace IS NOT NULL THEN
  IF visible_workspace IS DISTINCT FROM NEW.data->>'workspace_id' THEN
   RAISE EXCEPTION 'crabber_session_workspace_conflict' USING ERRCODE='42501';
  END IF;
  RETURN NEW;
 END IF;
 BEGIN
  EXECUTE format('INSERT INTO %I.session_owner_contexts(session_id,data_json) VALUES($1,$2) ON CONFLICT(session_id) DO NOTHING',TG_TABLE_SCHEMA)
   USING NEW.id,jsonb_build_object('workspace',NEW.data->>'workspace_id')::text;
  GET DIAGNOSTICS inserted = ROW_COUNT;
  -- This private exception rolls back ONLY the nested probe subtransaction;
  -- PL/pgSQL retains the local row count. Other SQL errors propagate normally.
  RAISE EXCEPTION 'crabber_session_birth_probe_rollback' USING ERRCODE='PZ025';
 EXCEPTION WHEN SQLSTATE 'PZ025' THEN NULL;
 END;
 IF inserted<>1 THEN
  RAISE EXCEPTION 'crabber_session_workspace_conflict' USING ERRCODE='42501';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER sessions_workspace_birth BEFORE INSERT ON sessions
 FOR EACH ROW EXECUTE FUNCTION crabber_guard_session_birth();
INSERT INTO schema_version(version) VALUES(25) ON CONFLICT DO NOTHING;
