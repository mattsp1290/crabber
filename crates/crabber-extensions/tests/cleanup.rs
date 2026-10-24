#![cfg(unix)]
#[path = "support/agent.rs"]
mod agent_support;
#[path = "cleanup/deadline.rs"]
mod deadline;
#[path = "cleanup/delegation.rs"]
mod delegation;
#[allow(dead_code)]
#[path = "contextual/fixture.rs"]
mod fixture;
#[path = "contextual/options.rs"]
mod options;
#[allow(dead_code)]
#[path = "support/owned_process.rs"]
mod owned_process;
#[path = "cleanup/processes.rs"]
mod processes;
#[path = "support/scripted.rs"]
mod scripted;
