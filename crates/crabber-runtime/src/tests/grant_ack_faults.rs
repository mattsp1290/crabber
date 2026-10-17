use super::*;
pub(super) struct Capabilities {
    attempt: crabber_session::AttemptAuthority,
    phase: crabber_session::PhaseAuthority,
}
pub(super) fn interpose(
    gate: &SettlementGate,
    outcome: &mut crabber_session::ToolContinuationClaimOutcome,
    fault: usize,
) -> Result<(), crabber_session::ToolContinuationError> {
    if fault == 18 {
        *outcome = gate.foreign_grant_ack.lock().unwrap().take().unwrap();
        return Ok(());
    }
    let crabber_session::ToolContinuationClaimOutcome::Applied { receipt, grant } = outcome else {
        return Ok(());
    };
    if fault == 15 {
        *gate.foreign_grant_capabilities.lock().unwrap() = Some(Capabilities {
            attempt: grant.authority.clone(),
            phase: grant
                .phases
                .first()
                .map(|phase| &phase.authority)
                .or_else(|| grant.frontend.first().map(|phase| &phase.authority))
                .unwrap()
                .clone(),
        });
        let placeholder = crabber_session::ToolContinuationClaimOutcome::Replayed(receipt.clone());
        *gate.foreign_grant_ack.lock().unwrap() = Some(std::mem::replace(outcome, placeholder));
        return Err(crabber_session::ToolContinuationError::UnknownStoreOutcome);
    }
    match fault {
        2 => return Err(crabber_session::ToolContinuationError::UnknownStoreOutcome),
        16 => {
            grant.authority = gate
                .foreign_grant_capabilities
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .attempt
                .clone();
        }
        17 => {
            let foreign = gate
                .foreign_grant_capabilities
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .phase
                .clone();
            if let Some(phase) = grant.phases.first_mut() {
                phase.authority = foreign;
            } else {
                grant.frontend[0].authority = foreign;
            }
        }
        11 => grant.turns_used += 1,
        12 => grant.source_turn_id = Some(crabber_core::TurnId::new()),
        8 | 10 if !grant.phases.is_empty() => {
            let phase = &mut grant.phases[0];
            if fault == 10 {
                phase.responses[0].value = Some(json!("changed accepted answer"));
            } else {
                phase.checkpoint = crabber_core::ToolCheckpoint::new(
                    phase.checkpoint.schema_id(),
                    phase.checkpoint.schema_version(),
                    json!({"effect":"rewritten valid checkpoint"}),
                )
                .unwrap();
            }
        }
        8 | 10 => {
            let phase = &mut grant.frontend[0];
            if fault == 10 {
                phase
                    .frozen
                    .descriptor
                    .description
                    .push_str(" changed descriptor");
            } else {
                phase.result = crabber_core::FrontendToolResult::new(
                    phase.result.call_id().clone(),
                    crabber_core::FrontendResultStatus::Completed,
                    crabber_core::ToolOutput::from_json(&json!("rewritten valid external result"))
                        .unwrap(),
                )
                .unwrap();
            }
        }
        _ => (),
    }
    Ok(())
}
