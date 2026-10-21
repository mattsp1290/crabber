//! Deliberately relational fixtures: the test-only scope never certifies readiness.
use crate::{SnapshotLimits, SnapshotOutcome, SnapshotRequest, Store};
use crabber_core::{EventCursor, SessionId};
use crabber_test_support::ChildDatabase;

pub(crate) struct Graph {
    pub session: SessionId,
    pub run: String,
    pub message: String,
    pub call: String,
    pub imported: String,
    prefix: String,
}
impl Graph {
    pub fn new(session: SessionId) -> Self {
        let prefix = uuid::Uuid::new_v4().simple().to_string();
        Self {
            session,
            run: format!("{prefix}-run"),
            message: format!("{prefix}-message"),
            call: format!("{prefix}-call"),
            imported: format!("{prefix}-imported"),
            prefix,
        }
    }
    pub fn run_sql(&self) -> String {
        format!(
            "INSERT INTO runs(id,session_id,status,claim_token,lease_until,data,native_source_protocol) VALUES('{}','{}','completed','fixture-token',0,'{{}}',1)",
            self.run, self.session.0
        )
    }
    pub fn event_sql(&self) -> String {
        format!(
            "INSERT INTO events(session_id,run_id,data) VALUES('{}','{}','{{}}') RETURNING seq",
            self.session.0, self.run
        )
    }
    pub fn statements(&self, cursor: i64, postgres: bool) -> Vec<String> {
        let mut statements = self.core(postgres);
        statements.extend(self.native(cursor));
        statements.extend(self.continuations());
        statements.extend(self.metadata(cursor));
        statements
    }
    fn core(&self, postgres: bool) -> Vec<String> {
        let Self {
            session,
            run,
            message,
            call,
            imported,
            prefix,
        } = self;
        let session = &session.0;
        let snapshot = if postgres { ",snapshot_record" } else { "" };
        let record = if postgres { ",'{}'" } else { "" };
        vec![
            format!(
                "INSERT INTO messages(id,session_id,run_id,data,snapshot_parts,snapshot_text,snapshot_bytes{snapshot}) VALUES('{message}','{session}','{run}','{{}}',0,0,2{record})"
            ),
            format!(
                "INSERT INTO parts(id,message_id,ordinal,data) VALUES('{prefix}-part','{message}',0,'{{}}')"
            ),
            format!(
                "INSERT INTO tool_calls(id,run_id,status,data,snapshot_parts,snapshot_text,snapshot_bytes{snapshot}) VALUES('{call}','{run}','completed','{{}}',0,0,2{record})"
            ),
            format!(
                "INSERT INTO epochs(id,session_id,run_id,data) VALUES('{prefix}-epoch','{session}','{run}','{{}}')"
            ),
            format!(
                "INSERT INTO inbox(session_id,kind,consumed_by_run,data{snapshot}) VALUES('{session}','steer','{run}','{{}}'{record})"
            ),
            format!(
                "INSERT INTO extension_state(session_id,extension_id,key,value) VALUES('{session}','fixture','secret','erased-fixture')"
            ),
            format!(
                "INSERT INTO admission_receipts(session_id,admission_key,run_id,user_message_id,data) VALUES('{session}','{prefix}-key','{run}','{message}','{{}}')"
            ),
            format!(
                "INSERT INTO admission_executions(run_id,session_id,admission_key,capsule_version,start_state,data) VALUES('{run}','{session}','{prefix}-key',1,'Started','{{}}')"
            ),
            format!(
                "INSERT INTO run_execution_bindings(run_id,capsule_version,data_json) VALUES('{run}',2,'{{}}')"
            ),
            format!(
                "INSERT INTO session_public_state(session_id,format_version,state_json,activities_json) VALUES('{session}',1,'{{}}','[]') ON CONFLICT(session_id) DO NOTHING"
            ),
            format!(
                "INSERT INTO imported_tool_calls(id,session_id,ordinal,data_json,snapshot_parts,snapshot_text,snapshot_bytes) VALUES('{imported}','{session}',1,'{{}}',0,0,2)"
            ),
            format!(
                "INSERT INTO imported_message_provenance(message_id,session_id,ordinal,data_json,snapshot_bytes) VALUES('{message}','{session}',1,'{{}}',2)"
            ),
        ]
    }
    fn native(&self, cursor: i64) -> Vec<String> {
        let Self {
            session,
            run,
            call,
            prefix,
            ..
        } = self;
        let session = &session.0;
        vec![
            format!(
                "INSERT INTO execution_attempts(id,run_id,generation,owner,claim_token,run_claim_token,lease_until,state,cleanup_state,outcome_json) VALUES('{prefix}-attempt','{run}',1,'fixture','x','fixture-token',0,'Completed','Complete','{{}}')"
            ),
            format!(
                "INSERT INTO tool_phases(id,run_id,call_id,attempt_id,generation,claim_token,state,quiescence_state,outcome_json) VALUES('{prefix}-phase','{run}','{call}','{prefix}-attempt',1,'','Completed','Complete','{{}}')"
            ),
            format!(
                "INSERT INTO session_invocations(id,session_id,parent_run_id,parent_call_id,generation,owner,claim_token,lease_until,state,cleanup_state,data_json) VALUES('{prefix}-invocation','{session}','{run}','{call}',1,'fixture','',0,'Completed','Complete','{{}}')"
            ),
            format!(
                "INSERT INTO abandonment_commits(run_id,event_seq,data) VALUES('{run}',{cursor},'{{}}')"
            ),
            format!(
                "INSERT INTO native_parent_witnesses(event_seq,session_id,run_id,kind,operation_id,data_json) VALUES({cursor},'{session}','{run}','attempt','{prefix}-operation','{{}}')"
            ),
            format!(
                "INSERT INTO native_run_issuances(session_id,run_id,sequence,data_json) VALUES('{session}','{run}',1,'{{}}')"
            ),
            format!("INSERT INTO run_suspension_proofs(run_id,data_json) VALUES('{run}','{{}}')"),
        ]
    }
    fn continuations(&self) -> Vec<String> {
        let Self {
            session,
            run,
            call,
            prefix,
            ..
        } = self;
        let session = &session.0;
        vec![
            format!(
                "INSERT INTO continuations(id,session_id,run_id,generation,state,format_version,data_json) VALUES('{prefix}-approval','{session}','{run}',1,'released',1,'{{}}')"
            ),
            format!(
                "INSERT INTO continuation_barriers(session_id,run_id,continuation_id) VALUES('{session}','{run}','{prefix}-approval')"
            ),
            format!(
                "INSERT INTO continuation_interactions(id,continuation_id,run_id,call_id) VALUES('{prefix}-interaction','{prefix}-approval','{run}','{call}')"
            ),
            format!(
                "INSERT INTO tool_continuations(id,session_id,run_id,generation,state,format_version,data_json) VALUES('{prefix}-continuation','{session}','{run}',1,'released',1,'{{}}')"
            ),
            format!(
                "INSERT INTO tool_continuation_barriers(session_id,run_id,continuation_id) VALUES('{session}','{run}','{prefix}-continuation')"
            ),
            format!(
                "INSERT INTO tool_continuation_interactions(id,continuation_id,run_id,call_id,phase_id) VALUES('{prefix}-tool-interaction','{prefix}-continuation','{run}','{call}','{prefix}-phase')"
            ),
            format!(
                "INSERT INTO tool_continuation_phases(continuation_id,run_id,phase_id) VALUES('{prefix}-continuation','{run}','{prefix}-phase')"
            ),
            format!(
                "INSERT INTO frontend_tool_origins(call_id,session_id,run_id,format_version,data_json) VALUES('{call}','{session}','{run}',1,'{{}}')"
            ),
            format!(
                "INSERT INTO tool_continuation_frontend_calls(continuation_id,run_id,call_id,phase_id) VALUES('{prefix}-continuation','{run}','{call}','{prefix}-phase')"
            ),
        ]
    }
    fn metadata(&self, cursor: i64) -> Vec<String> {
        let Self {
            session,
            run,
            prefix,
            ..
        } = self;
        let session = &session.0;
        vec![
            format!(
                "INSERT INTO operation_receipts(session_id,operation_id,kind,semantic_digest,event_seq,data_json) VALUES('{session}','{prefix}-operation','phase','{}',{cursor},'{{}}')",
                "a".repeat(64)
            ),
            format!(
                "INSERT INTO session_fork_audit(session_id,operation_id,data_json) VALUES('{session}','{prefix}-operation','{{}}')"
            ),
            format!(
                "INSERT INTO future_queue(session_id,queue_id,sequence,state,prepared,summary_json,data_json,admission_key) VALUES('{session}','{prefix}-queue',1,'Cancelled',false,'{{}}','{{}}','{prefix}-queue-key')"
            ),
            format!(
                "INSERT INTO future_queue_audit(session_id,operation_id,queue_id,revision,data_json) VALUES('{session}','{prefix}-operation','{prefix}-queue',1,'{{}}')"
            ),
            format!(
                "INSERT INTO run_controls(session_id,run_id,operation_id,sequence,request_json,summary_json) VALUES('{session}','{run}','{prefix}-operation',1,'{{}}','{{}}')"
            ),
            format!(
                "INSERT INTO run_control_progress(session_id,operation_id,event_seq,summary_json) VALUES('{session}','{prefix}-operation',{cursor},'{{}}')"
            ),
            format!(
                "INSERT INTO session_owner_contexts(session_id,data_json) VALUES('{session}','{{}}') ON CONFLICT(session_id) DO NOTHING"
            ),
            format!(
                "INSERT INTO session_owner_current(session_id,owner_id,data_json) VALUES('{session}','{prefix}-owner','{{}}')"
            ),
            format!(
                "INSERT INTO session_owner_audits(session_id,operation_id,owner_id,sequence,operation,state,data_json) VALUES('{session}','{prefix}-operation','{prefix}-owner',1,'Reserve','Joined','{{}}')"
            ),
        ]
    }
}

pub(crate) async fn first_snapshot(store: &impl Store, session: &SessionId) -> SnapshotRequest {
    let mut request = SnapshotRequest {
        session_id: session.clone(),
        continuation: None,
        limits: SnapshotLimits {
            messages: 1,
            tool_calls: 1,
            parts: 8,
            text_bytes: 65_536,
            encoded_bytes: 131_072,
        },
    };
    let SnapshotOutcome::Page(page) = store.snapshot(request.clone()).await.unwrap() else {
        panic!("snapshot page")
    };
    request.continuation = Some(page.continuation.expect("retained history continuation"));
    request
}
pub(crate) async fn unchanged_snapshot(store: &impl Store, request: SnapshotRequest) {
    assert!(matches!(
        store.snapshot(request).await.unwrap(),
        SnapshotOutcome::Page(_)
    ));
}
pub(crate) fn cursor(value: i64) -> EventCursor {
    EventCursor(u64::try_from(value).unwrap())
}

/// Kill before commit, or deliberately withhold the committed receipt.
pub(crate) async fn spawn(
    helper: &str,
    request: &crate::DeleteSession,
    path: ChildDatabase<'_>,
    crash: bool,
) {
    let mut command = std::process::Command::new(std::env::current_exe().unwrap());
    command.args(["--exact", helper, "--nocapture"]).env(
        "CRABBER_PURGE_CHILD",
        serde_json::to_string(&(request, crash)).unwrap(),
    );
    path.configure(&mut command, "CRABBER_SQLITE_CHILD_PATH");
    let mut child = command.spawn().unwrap();
    let status = tokio::time::timeout(std::time::Duration::from_secs(30), async {
        loop {
            if let Some(status) = child.try_wait().unwrap() {
                return status;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await;
    if status.is_err() {
        let _ = child.kill();
        let _ = child.wait();
    }
    assert_eq!(
        status.expect("purge process timed out").code(),
        Some(if crash { 83 } else { 0 })
    );
}
