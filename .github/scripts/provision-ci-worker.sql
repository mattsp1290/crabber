-- CI-only literal for a throwaway trust-auth database, never a deployed secret.
-- Workspace roster is audited in the local agentcraft execution handoff.
DO $$ BEGIN
 IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='crabber_ci_worker') THEN
  CREATE ROLE crabber_ci_worker LOGIN PASSWORD 'crabber-ci-only' IN ROLE crabber_worker;
 END IF;
END $$;
INSERT INTO crabber_workspace_grants(role_name, workspace_id)
SELECT 'crabber_ci_worker', workspace_id FROM (VALUES
 ('default'),
 ('defaults-workspace'),
 ('other-workspace'),
 ('workspace/所有者'),
 (E'workspace\nopaque'),
 (repeat(chr(1),512)),
 ('fixture'),
 ('parent-controls'),
 ('frontend-workspace'),
 ('quiet-deletion'),
 ('management-workspace'),
 ('fork-workspace')
) AS workspaces(workspace_id)
ON CONFLICT DO NOTHING;
-- No listed journey truncates tables or needs DDL. Ordinary own-row DML covers
-- the durable-interaction corruption fixtures; no extra privileges are granted.
