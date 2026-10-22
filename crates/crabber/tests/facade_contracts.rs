//! Public facade contracts sharing one integration-test harness.

#[path = "facade_contracts/operational_observation.rs"]
mod operational_observation;
#[path = "facade_contracts/output_token_cap.rs"]
mod output_token_cap;
#[path = "facade_contracts/prompt_contribution_facade.rs"]
mod prompt_contribution_facade;
#[path = "facade_contracts/session_owners.rs"]
mod session_owners;
#[path = "facade_contracts/sqlite_builder.rs"]
mod sqlite_builder;
#[path = "facade_contracts/storable_records.rs"]
mod storable_records;
#[path = "facade_contracts/typed_input.rs"]
mod typed_input;
#[path = "facade_contracts/unknown_tool_depth.rs"]
mod unknown_tool_depth;
