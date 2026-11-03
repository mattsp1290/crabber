use crate::{Result, profile};
use crabber_core::{
    ControlId, SessionContext, SessionId, SessionOwnerId, SessionOwnerOperationId, SessionRevision,
};
use crabber_session::session_owner::owned_scope::{
    OwnedMainDisposition, OwnedScopeActivation, OwnedScopeActivationOutcome,
    OwnedScopeParentAuthority, OwnedScopeSource, OwnedTaskRegistration, PendingDriverTasks,
    PendingInitialProducer, PendingInitialProducerStatus,
};
use crabber_session::session_owner::preparation::{
    JoinedPreparation, OWNED_PREPARATION_COMPONENT, PreparationGrant, PreparationKey,
    PreparationPurpose, PreparationResourceOutcome, PreparedOperation, ReservePreparation,
    admission_intent,
};
use crabber_session::session_owner::{
    MutateSessionOwner, ReserveSessionOwner, SessionOwnerBinding, SessionOwnerCommand,
    SessionOwnerKind, SessionOwnerOutcome, SessionOwnerState, SessionOwnerTarget,
};
use crabber_session::{ManageSession, SessionChange, SessionDefaults, Store};
use serde_json::Value;
use std::sync::Arc;

type RegisteredPreparation =
    OwnedTaskRegistration<std::result::Result<((), PreparedOperation), String>>;

pub enum Observer {
    #[cfg(feature = "postgres")]
    Postgres(sqlx::PgPool),
    #[cfg(feature = "sqlite")]
    Sqlite(sqlx::SqlitePool),
}
macro_rules! observe {
    ($self:expr, $query:expr, $bindings:expr, $ty:ty, $fetch:ident) => {
        match $self {
            #[cfg(feature = "postgres")]
            Observer::Postgres(pool) => {
                let mut query = sqlx::query_scalar::<_, $ty>($query);
                for value in $bindings {
                    query = query.bind(value);
                }
                query.$fetch(pool).await?
            }
            #[cfg(feature = "sqlite")]
            Observer::Sqlite(pool) => {
                let mut query = sqlx::query_scalar::<_, $ty>($query);
                for value in $bindings {
                    query = query.bind(value);
                }
                query.$fetch(pool).await?
            }
        }
    };
}
impl Observer {
    #[cfg(feature = "postgres")]
    pub async fn postgres(url: &str) -> Result<Self> {
        Ok(Self::Postgres(
            sqlx::postgres::PgPoolOptions::new()
                .max_connections(1)
                .connect(url)
                .await?,
        ))
    }
    #[cfg(feature = "sqlite")]
    pub async fn sqlite(path: &std::path::Path) -> Result<Self> {
        let options = sqlx::sqlite::SqliteConnectOptions::new()
            .filename(path)
            .read_only(true);
        Ok(Self::Sqlite(
            sqlx::sqlite::SqlitePoolOptions::new()
                .max_connections(1)
                .connect_with(options)
                .await?,
        ))
    }
    pub async fn close(self) {
        match self {
            #[cfg(feature = "postgres")]
            Self::Postgres(pool) => pool.close().await,
            #[cfg(feature = "sqlite")]
            Self::Sqlite(pool) => pool.close().await,
        }
    }
    async fn integer(&self, sql: &str, values: &[&str]) -> Result<i64> {
        Ok(observe!(self, sql, values, i64, fetch_one))
    }
    async fn strings(&self, sql: &str, values: &[&str]) -> Result<Vec<String>> {
        Ok(observe!(self, sql, values, String, fetch_all))
    }
    async fn json(&self, sql: &str, values: &[&str]) -> Result<Value> {
        let rows = self.strings(sql, values).await?;
        check_eq!(rows.len(), 1, "one native record required");
        Ok(serde_json::from_str(&rows[0])?)
    }
}

fn preparation_reservation(
    session: &SessionId,
    revision: SessionRevision,
    bound: &crabber_session::BoundKeyedAdmitRequest,
    keyed: bool,
) -> Result<ReservePreparation> {
    let behavior = bound.binding.behavior_fingerprint.clone();
    let reservation = ReservePreparation {
        owner: ReserveSessionOwner {
            id: SessionOwnerOperationId::new(),
            binding: SessionOwnerBinding {
                session_id: session.clone(),
                workspace_id: profile::WORKSPACE.into(),
                directory: profile::DIRECTORY.into(),
                id: SessionOwnerId::new(),
                kind: SessionOwnerKind::Preparation,
                component_id: OWNED_PREPARATION_COMPONENT.into(),
                behavior_fingerprint: behavior,
                resource_contract: None,
                parent: None,
            },
            owner: "backend-selection-preparation".into(),
            lease_ms: 30_000,
        },
        key: PreparationKey::Unkeyed(ControlId::new()),
        source: None,
        resource_contract: Some(profile::resource_contract()),
        expected_revision: revision,
        purpose: PreparationPurpose::OrdinaryAdmission,
        intent: admission_intent(
            &bound.binding.request,
            bound
                .binding
                .input_envelope
                .as_ref()
                .ok_or("input missing")?,
            keyed.then_some(&bound.keyed.options),
        )?,
    };
    let key = if keyed {
        PreparationKey::Admission(bound.keyed.options.key.clone())
    } else {
        PreparationKey::Unkeyed(ControlId::new())
    };
    reservation.bind_key(key).map_err(Into::into)
}

async fn prepare(
    store: &dyn Store,
    keyed: bool,
) -> Result<(
    SessionContext,
    JoinedPreparation,
    RegisteredPreparation,
    SessionOwnerId,
)> {
    let session = SessionId::new();
    let revision = store
        .manage_session(ManageSession {
            id: ControlId::new(),
            session_id: session.clone(),
            expected_revision: SessionRevision::INITIAL,
            change: SessionChange::Create {
                workspace_id: profile::WORKSPACE.into(),
                directory: profile::DIRECTORY.into(),
                title: "native selection proof".into(),
                defaults: SessionDefaults::default(),
            },
        })
        .await?
        .revision;
    let bound = profile::bound(&session, revision)?;
    let reservation = preparation_reservation(&session, revision, &bound, keyed)?;
    let SessionOwnerOutcome::Applied {
        authority: Some(authority),
        ..
    } = store.reserve_preparation(reservation.clone()).await?
    else {
        return Err("fresh preparation reservation required".into());
    };
    let root_owner = authority.target().id.clone();
    let begin = store
        .mutate_session_owner(
            MutateSessionOwner {
                id: reservation.reconciliation()?.begin_id,
                session_id: session.clone(),
                target: authority.target().clone(),
                command: SessionOwnerCommand::Begin,
            },
            Some(authority.clone()),
        )
        .await?;
    check!(matches!(begin, SessionOwnerOutcome::Applied { .. }));
    let grant = PreparationGrant::from_fresh_begin(reservation, authority)?;
    let PreparationResourceOutcome::Applied { receipt, grant } =
        store.retain_preparation_resources(grant).await?
    else {
        return Err("fresh retained resource authority required".into());
    };
    let operation = if keyed {
        PreparedOperation::AdmitKeyed(Box::new(bound))
    } else {
        PreparedOperation::Admit(Box::new(crabber_session::BoundAdmitRequest {
            request: bound.keyed.request,
            binding: bound.binding,
        }))
    };
    let mut registered = OwnedTaskRegistration::register(
        OwnedScopeSource::from_receipts(&receipt.reserved, &receipt.began)?,
        || async move { Ok::<_, String>(((), operation)) },
    )?;
    let OwnedScopeActivationOutcome::Applied { permit, .. } = store
        .activate_owned_scope(
            registered.activation().clone(),
            grant
                .retained_resources_authority()
                .ok_or("resource authority missing")?
                .clone(),
            Some(OwnedScopeParentAuthority::preparation((*grant).clone())),
        )
        .await?
    else {
        return Err("fresh registered preparation activation required".into());
    };
    registered.dispatch(*permit)?;
    let ((), original) = JoinedPreparation::observe_registered(*grant, &mut registered)
        .await?
        .map_err(|_| "preparation task failed")?;
    check_eq!(
        registered.join().await?.disposition(),
        OwnedMainDisposition::Returned
    );
    Ok((
        SessionContext::new(session, profile::WORKSPACE, profile::DIRECTORY),
        original,
        registered,
        root_owner,
    ))
}

pub async fn run(case: &str, store: Arc<dyn Store>, observer: &Observer) -> Result<()> {
    let (keyed, scripted) = match case {
        "native-register" => (false, false),
        "native-register-keyed" => (true, false),
        "native-scripted" => (false, true),
        "native-scripted-keyed" => (true, true),
        _ => return Err("unknown native case".into()),
    };
    let (context, original, registered, root_owner) = prepare(store.as_ref(), keyed).await?;
    let session = context.session_id().clone();
    let tasks = PendingDriverTasks::default();
    let mut producer = if scripted {
        let source = crabber_extension::LibraryScriptedSource::new(
            crabber_providers::Selection {
                provider_id: "library".into(),
                model_id: "text".into(),
            },
            vec![profile::RESPONSE.into()],
        )?;
        PendingInitialProducer::register_scripted(&tasks, store.clone(), context, original, source)?
    } else {
        PendingInitialProducer::register(&tasks, store.clone(), context, original)?
    };
    let control = producer.control();
    let result = async {
        check!(matches!(
            control.status(),
            PendingInitialProducerStatus::Parked
        ));
        producer.start()?;
        let completion = tokio::time::timeout(crate::BOUND, producer.join()).await??;
        check!(matches!(
            completion.status(),
            PendingInitialProducerStatus::ClosedAssignment
        ));
        check_eq!(
            completion.disposition(),
            if scripted {
                OwnedMainDisposition::Returned
            } else {
                OwnedMainDisposition::NoDispatch
            }
        );
        check!(completion.error().is_none());
        check!(!completion.observation_failed());
        check!(completion.take_retry().is_none());
        let report = tasks.observe_supervision(crate::BOUND).await;
        check_eq!(report.pending_supervisors(), 0);
        check_eq!(report.failed_supervisors(), 0);
        check!(report.is_complete());
        tokio::time::timeout(crate::BOUND, tasks.wait_for_supervision()).await?;
        check_eq!(
            store.get_session_owner(&session, &root_owner).await?.state,
            SessionOwnerState::Joined
        );
        verify(observer, store.as_ref(), &session, keyed, scripted).await?;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    }
    .await;
    if result.is_err() {
        control.request_stop();
        let joined = tokio::time::timeout(crate::BOUND, producer.join()).await;
        let supervision = tasks.observe_supervision(crate::BOUND).await;
        if !matches!(joined, Ok(Ok(_))) || !supervision.is_complete() {
            eprintln!("native case failed and original cleanup remains incomplete");
            return Err("native primary failure and cleanup incomplete".into());
        }
    }
    drop((control, producer, tasks, registered));
    result
}

struct NativeAssignment {
    session: SessionId,
    source: String,
    epoch: String,
    run: String,
    assignment: Value,
}
impl NativeAssignment {
    fn scope(&self) -> [&str; 3] {
        [&self.session.0, &self.source, &self.epoch]
    }
}

const RESULT_SQL: &str = "SELECT r.data_json FROM initial_consumer_results r JOIN initial_consumer_calls c ON r.session_id=c.session_id AND r.epoch_id=c.epoch_id AND r.call_id=c.call_id WHERE r.session_id=$1 AND r.source_key=$2 AND r.epoch_id=$3 AND c.kind=$4";

async fn read_assignment(observer: &Observer, session: &SessionId) -> Result<NativeAssignment> {
    let assignment = observer
        .json(
            "SELECT data_json FROM pending_initial_assignments WHERE session_id=$1",
            &[&session.0],
        )
        .await?;
    let source = observer
        .strings(
            "SELECT source_key FROM pending_initial_assignments WHERE session_id=$1",
            &[&session.0],
        )
        .await?;
    let epoch = observer
        .strings(
            "SELECT epoch_id FROM pending_initial_assignments WHERE session_id=$1",
            &[&session.0],
        )
        .await?;
    let run = observer
        .strings(
            "SELECT run_id FROM pending_initial_assignments WHERE session_id=$1",
            &[&session.0],
        )
        .await?;
    check_eq!((source.len(), epoch.len(), run.len()), (1, 1, 1));
    Ok(NativeAssignment {
        session: session.clone(),
        source: source[0].clone(),
        epoch: epoch[0].clone(),
        run: run[0].clone(),
        assignment,
    })
}

async fn verify(
    observer: &Observer,
    store: &dyn Store,
    session: &SessionId,
    keyed: bool,
    scripted: bool,
) -> Result<()> {
    let facts = read_assignment(observer, session).await?;
    verify_counts(observer, &facts, keyed, scripted).await?;
    let birth_close = observer.json("SELECT data_json FROM pending_initial_closes WHERE session_id=$1 AND source_key=$2 AND epoch_id=$3", &facts.scope()).await?;
    check_eq!(facts.assignment["run_id"], facts.run);
    if !scripted {
        return Ok(());
    }
    let close = verify_consumer_correlation(observer, &facts, &birth_close).await?;
    verify_calls(observer, &facts, keyed).await?;
    verify_output(observer, &facts).await?;
    verify_public_run(store, &facts).await?;
    verify_retirement(observer, store, &facts, &close).await?;
    Ok(())
}

async fn verify_counts(
    observer: &Observer,
    facts: &NativeAssignment,
    keyed: bool,
    scripted: bool,
) -> Result<()> {
    let scope = facts.scope();
    for (table, expected) in [
        ("pending_initial_assignments", 1),
        ("pending_initial_closes", 1),
        ("pending_initial_publications", 3),
        ("initial_consumer_assignments", i64::from(scripted)),
        ("initial_consumer_closes", i64::from(scripted)),
        (
            "initial_consumer_calls",
            if scripted {
                if keyed { 12 } else { 11 }
            } else {
                0
            },
        ),
        (
            "initial_consumer_results",
            if scripted {
                if keyed { 12 } else { 11 }
            } else {
                0
            },
        ),
        (
            "initial_consumer_roster",
            if scripted {
                if keyed { 12 } else { 11 }
            } else {
                0
            },
        ),
        (
            "initial_consumer_reverse",
            if scripted {
                if keyed { 12 } else { 11 }
            } else {
                0
            },
        ),
    ] {
        let sql = format!(
            "SELECT count(*) FROM {table} WHERE session_id=$1 AND source_key=$2 AND epoch_id=$3"
        );
        check_eq!(observer.integer(&sql, &scope).await?, expected, "{table}");
    }
    check_eq!(
        observer
            .integer(
                "SELECT MAX(sequence) FROM native_run_issuances WHERE session_id=$1 AND run_id=$2",
                &[&facts.session.0, &facts.run]
            )
            .await?,
        if scripted { 2 } else { 1 }
    );
    check_eq!(observer.integer("SELECT count(*) FROM session_owner_audits WHERE session_id=$1 AND operation='Join' AND owner_id LIKE 'driver-owner-%'", &[&facts.session.0]).await?, i64::from(scripted));
    Ok(())
}

async fn verify_consumer_correlation(
    observer: &Observer,
    facts: &NativeAssignment,
    birth_close: &Value,
) -> Result<Value> {
    let scope = facts.scope();
    let consumer_assignment = observer.json("SELECT data_json FROM initial_consumer_assignments WHERE session_id=$1 AND source_key=$2 AND epoch_id=$3 AND run_id=$4", &[scope[0], scope[1], scope[2], &facts.run]).await?;
    check_eq!(consumer_assignment["assignment"], facts.assignment);
    let close = observer.json("SELECT data_json FROM initial_consumer_closes WHERE session_id=$1 AND source_key=$2 AND epoch_id=$3", &scope).await?;
    check_eq!(close["disposition"], "Returned");
    check_eq!(close["retirement"]["retirement"], "activated");
    for record in [&facts.assignment, birth_close, &consumer_assignment, &close] {
        check!(record["correlation"].is_object());
        check!(record["correlation"]["key"].is_object());
        check!(record["correlation"]["identity"].is_object());
        check_eq!(record["correlation"]["key"]["session_id"], facts.session.0);
        check_eq!(record["correlation"]["key"]["identity"], facts.source);
        check_eq!(record["correlation"]["identity"]["epoch"], facts.epoch);
        check!(record["correlation"]["identity"]["complete"].is_string());
        check!(record["correlation"]["identity"]["seal"].is_string());
    }
    check_eq!(close["correlation"], consumer_assignment["correlation"]);
    check_eq!(
        close["correlation"]["key"],
        birth_close["correlation"]["key"]
    );
    check_eq!(
        close["correlation"]["identity"],
        birth_close["correlation"]["identity"]
    );
    Ok(close)
}

async fn verify_calls(observer: &Observer, facts: &NativeAssignment, keyed: bool) -> Result<()> {
    let scope = facts.scope();
    let rows = observer.strings("SELECT kind FROM initial_consumer_calls WHERE session_id=$1 AND source_key=$2 AND epoch_id=$3", &scope).await?;
    for kind in [
        "register",
        "activate",
        "reserve_initial",
        "begin_initial",
        "assistant_message",
        "conclude",
        "run_admitted",
        "run_started",
        "turn_started",
        "message_committed",
        "turn_completed",
    ] {
        check_eq!(
            rows.iter().filter(|value| value.as_str() == kind).count(),
            1,
            "{kind}"
        );
    }
    check_eq!(
        rows.iter()
            .filter(|value| value.as_str() == "keyed_begin")
            .count(),
        usize::from(keyed)
    );
    Ok(())
}

async fn verify_output(observer: &Observer, facts: &NativeAssignment) -> Result<()> {
    let scope = facts.scope();
    let message = observer
        .json(
            RESULT_SQL,
            &[scope[0], scope[1], scope[2], "assistant_message"],
        )
        .await?;
    let stored: crabber_core::Message =
        serde_json::from_value(message["original"]["original"]["message"].clone())?;
    let text: String = stored
        .parts
        .iter()
        .filter_map(|part| match &part.content {
            crabber_core::ContentBlock::Text { text } => Some(text.as_str()),
            _ => None,
        })
        .collect();
    check_eq!(text, profile::RESPONSE);
    let terminal = observer
        .json(RESULT_SQL, &[scope[0], scope[1], scope[2], "conclude"])
        .await?;
    let terminal_status: crabber_core::RunStatus =
        serde_json::from_value(terminal["original"]["original"]["after_run"]["status"].clone())?;
    check_eq!(terminal_status, crabber_core::RunStatus::Completed);
    Ok(())
}

async fn verify_public_run(store: &dyn Store, facts: &NativeAssignment) -> Result<()> {
    let run_id = crabber_core::RunId(facts.run.clone());
    check!(matches!(
        store.get_run(&run_id).await,
        Err(crabber_session::StoreError::HostUnsupported)
    ));
    let crabber_session::PublicRunOutcome::State(state) =
        store.public_run_state(&facts.session, 8192).await?
    else {
        return Err("bounded native run readback required".into());
    };
    check_eq!(state.session_id, facts.session);
    let actual_run = state.run.ok_or("native run missing")?;
    check_eq!(actual_run.run_id, run_id);
    check_eq!(actual_run.session_id, facts.session);
    check_eq!(actual_run.status, crabber_core::RunStatus::Completed);
    let events = store.list_events(&facts.session, None, 100).await?;
    for kind in [
        crabber_core::EventKind::RunAdmitted,
        crabber_core::EventKind::RunStarted,
        crabber_core::EventKind::TurnStarted,
        crabber_core::EventKind::MessageCommitted,
        crabber_core::EventKind::TurnCompleted,
    ] {
        check_eq!(
            events
                .iter()
                .filter(|event| event.run_id == actual_run.run_id && event.kind == kind)
                .count(),
            1
        );
    }
    Ok(())
}

async fn verify_retirement(
    observer: &Observer,
    store: &dyn Store,
    facts: &NativeAssignment,
    close: &Value,
) -> Result<()> {
    let scope = facts.scope();
    let register = observer
        .json(RESULT_SQL, &[scope[0], scope[1], scope[2], "register"])
        .await?;
    let activate = observer
        .json(RESULT_SQL, &[scope[0], scope[1], scope[2], "activate"])
        .await?;
    let audit = &close["retirement"]["audit"];
    check_eq!(audit["receipt"]["operation"], "Join");
    let retired_activation: OwnedScopeActivation =
        serde_json::from_value(audit["record"]["owned_join"]["activation"].clone())
            .inspect_err(|_| eprintln!("native retired activation decode failed"))?;
    let actual_activation: OwnedScopeActivation =
        serde_json::from_value(activate["original"]["original"]["request"].clone())
            .inspect_err(|_| eprintln!("native original activation decode failed"))?;
    check_eq!(retired_activation, actual_activation);
    let retired_target: SessionOwnerTarget =
        serde_json::from_value(audit["record"]["snapshot"]["target"].clone())
            .inspect_err(|_| eprintln!("native retired target decode failed"))?;
    let registered_target: SessionOwnerTarget = serde_json::from_value(
        register["original"]["original"]["begin"]["record"]["snapshot"]["target"].clone(),
    )
    .inspect_err(|_| eprintln!("native original target decode failed"))?;
    check_eq!(retired_target, registered_target);
    let driver = store
        .get_session_owner(&facts.session, &retired_target.id)
        .await
        .inspect_err(|_| eprintln!("native driver public owner readback failed"))?;
    check_eq!(driver.target, retired_target);
    check_eq!(driver.state, SessionOwnerState::Joined);
    Ok(())
}
