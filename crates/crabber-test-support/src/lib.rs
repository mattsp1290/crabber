//! Private development fixtures. This crate never depends on Crabber's runtime,
//! facade or session implementation; migrations belong to the outer runner.

#[cfg(feature = "postgres")]
pub mod postgres;
