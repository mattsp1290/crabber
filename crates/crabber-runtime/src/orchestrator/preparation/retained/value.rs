//! Explicit output variants keep the retained registry bounded and inspectable.
use crabber_extension::RunPlan;
use crabber_providers::BoundProvider;

pub(in crate::orchestrator) enum RetainedValue {
    Plan(Box<RunPlan>),
    Admission(Box<(RunPlan, Option<BoundProvider>)>),
    Resume(Box<(RunPlan, BoundProvider)>),
    Empty,
}
pub(in crate::orchestrator) trait PreparationValue: Send + 'static {
    fn retain(self) -> RetainedValue;
    fn recover(value: RetainedValue) -> Result<Self, RetainedValue>
    where
        Self: Sized;
}
impl PreparationValue for RunPlan {
    fn retain(self) -> RetainedValue {
        RetainedValue::Plan(Box::new(self))
    }
    fn recover(value: RetainedValue) -> Result<Self, RetainedValue> {
        match value {
            RetainedValue::Plan(plan) => Ok(*plan),
            other => Err(other),
        }
    }
}
impl PreparationValue for (RunPlan, BoundProvider) {
    fn retain(self) -> RetainedValue {
        RetainedValue::Resume(Box::new(self))
    }
    fn recover(value: RetainedValue) -> Result<Self, RetainedValue> {
        match value {
            RetainedValue::Resume(resources) => Ok(*resources),
            other => Err(other),
        }
    }
}
impl PreparationValue for () {
    fn retain(self) -> RetainedValue {
        RetainedValue::Empty
    }
    fn recover(value: RetainedValue) -> Result<Self, RetainedValue> {
        match value {
            RetainedValue::Empty => Ok(()),
            other => Err(other),
        }
    }
}

impl PreparationValue for (RunPlan, Option<BoundProvider>) {
    fn retain(self) -> RetainedValue {
        RetainedValue::Admission(Box::new(self))
    }
    fn recover(value: RetainedValue) -> Result<Self, RetainedValue> {
        match value {
            RetainedValue::Admission(value) => Ok(*value),
            other => Err(other),
        }
    }
}
