use super::{LoadedModule, Loader, turn_metadata, turn_metadata_from_projection};
use crabber_extension::{
    ContextAssemble, EventPublished, ExtensionError, GuardContext, GuardDecision, ModelStream,
    Point, Registrar, RunSettled, RunStarted, ToolGuard, ToolPrepare, ToolResultTransform,
    TurnCompleted, TurnStarted,
};
use serde_json::{Value, json};
use std::sync::Arc;
use wasmtime::component::Val;

#[allow(clippy::needless_pass_by_value)]
fn error(error: impl ToString) -> ExtensionError {
    ExtensionError::Plan(error.to_string())
}

fn str_field<'a>(fields: &'a [(String, Val)], name: &str) -> Option<&'a str> {
    fields.iter().find_map(|(key, value)| {
        if key == name {
            if let Val::String(value) = value {
                Some(value.as_str())
            } else {
                None
            }
        } else {
            None
        }
    })
}

fn string(name: &str, value: impl Into<String>) -> (String, Val) {
    (name.into(), Val::String(value.into()))
}

fn replacement(value: Val) -> Result<Option<Value>, ExtensionError> {
    match value {
        Val::Variant(name, None) if name == "unchanged" => Ok(None),
        Val::Variant(name, Some(value)) if name == "json" => {
            let Val::String(json) = *value else {
                return Err(error("invalid replacement JSON"));
            };
            serde_json::from_str(&json).map(Some).map_err(error)
        }
        Val::Variant(name, _) if name == "error" => Err(error("guest rejected replacement")),
        _ => Err(error("invalid replacement")),
    }
}

pub(super) fn report_notify(
    module: &LoadedModule,
    source: &str,
    result: Result<Val, super::WasmError>,
) -> Result<(), ExtensionError> {
    let failure = match result {
        Ok(Val::Result(Ok(None))) => return Ok(()),
        Ok(Val::Result(Err(Some(detail)))) => format!("{source} rejected event: {detail:?}"),
        Ok(other) => format!("{source} returned invalid result: {other:?}"),
        Err(error) => format!("{source} failed: {error}"),
    };
    (module.log_observer)(&module.config.name, "error", &failure);
    Err(error(failure))
}

#[allow(clippy::too_many_lines)]
pub(super) async fn mount(
    module: &Arc<LoadedModule>,
    loader: &Arc<Loader>,
    registrar: &mut Registrar,
) -> Result<(), ExtensionError> {
    if module.roles.contains(&"permissions-policy") {
        registrar.guard(Arc::new(WasmGuard {
            module: Arc::clone(module),
            _loader: Arc::clone(loader),
        }));
    }
    if module.roles.contains(&"context-source") {
        let module = Arc::clone(module);
        let loader = Arc::clone(loader);
        registrar.on_transform(
            ContextAssemble::ID,
            0,
            "wasm-context",
            Arc::new(move |mut value| {
                let module = Arc::clone(&module);
                let _loader = Arc::clone(&loader);
                Box::pin(async move {
                    let result = module
                        .call("context-source-api", "load-context", &[turn_metadata(None)])
                        .await
                        .map_err(error)?;
                    let Val::Result(Ok(Some(messages))) = result else {
                        return Err(error("context source rejected call"));
                    };
                    let Val::List(messages) = *messages else {
                        return Err(error("invalid context messages"));
                    };
                    for message in messages {
                        let Val::Record(fields) = message else {
                            return Err(error("invalid context message"));
                        };
                        let role = fields
                            .iter()
                            .find_map(|(key, value)| {
                                if key == "role" {
                                    if let Val::Enum(role) = value {
                                        Some(role.as_str())
                                    } else {
                                        None
                                    }
                                } else {
                                    None
                                }
                            })
                            .ok_or_else(|| error("invalid context role"))?;
                        let blocks = fields
                            .iter()
                            .find_map(|(key, value)| {
                                if key == "blocks" {
                                    if let Val::List(blocks) = value {
                                        Some(blocks)
                                    } else {
                                        None
                                    }
                                } else {
                                    None
                                }
                            })
                            .ok_or_else(|| error("invalid context blocks"))?;
                        for block in blocks {
                            let Val::Variant(kind, Some(text)) = block else {
                                return Err(error("media references disabled"));
                            };
                            if kind != "text" {
                                return Err(error("media references disabled"));
                            }
                            let Val::String(text) = &**text else {
                                return Err(error("invalid context text"));
                            };
                            let key = if role == "system" {
                                "system_prelude"
                            } else {
                                "user_suffix"
                            };
                            value[key]
                                .as_array_mut()
                                .ok_or_else(|| error("invalid context projection"))?
                                .push(Value::String(text.clone()));
                        }
                    }
                    Ok(value)
                })
            }),
        );
    }
    if module.roles.contains(&"prompt-section") {
        let result = module
            .call("prompt-section-api", "sections", &[])
            .await
            .map_err(error)?;
        let Val::List(sections) = result else {
            return Err(error("invalid prompt sections"));
        };
        for section in sections {
            let Val::Record(fields) = section else {
                return Err(error("invalid prompt section"));
            };
            let name = str_field(&fields, "name")
                .ok_or_else(|| error("prompt section without name"))?
                .to_owned();
            let order = fields
                .iter()
                .find_map(|(key, value)| {
                    if key == "order" {
                        if let Val::S32(order) = value {
                            Some(*order)
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                })
                .unwrap_or(0);
            let module = Arc::clone(module);
            let loader = Arc::clone(loader);
            registrar.on_transform(
                ContextAssemble::ID,
                order,
                format!("wasm-prompt-{name}"),
                Arc::new(move |mut value| {
                    let module = Arc::clone(&module);
                    let _loader = Arc::clone(&loader);
                    let name = name.clone();
                    Box::pin(async move {
                        let rendered = module
                            .call(
                                "prompt-section-api",
                                "render",
                                &[Val::String(name), turn_metadata_from_projection(&value)],
                            )
                            .await
                            .map_err(error)?;
                        let Val::Result(Ok(Some(text))) = rendered else {
                            return Err(error("prompt render rejected"));
                        };
                        let Val::String(text) = *text else {
                            return Err(error("invalid prompt text"));
                        };
                        value["prompt_sections"]
                            .as_array_mut()
                            .ok_or_else(|| error("invalid prompt projection"))?
                            .push(Value::String(text));
                        Ok(value)
                    })
                }),
            );
        }
    }
    if module.roles.contains(&"tool-middleware") {
        let module_before = Arc::clone(module);
        let loader_before = Arc::clone(loader);
        registrar.on_transform(
            ToolPrepare::ID,
            0,
            "wasm-before-tool",
            Arc::new(move |mut value| {
                let module = Arc::clone(&module_before);
                let _loader = Arc::clone(&loader_before);
                Box::pin(async move {
                    let name = value["name"].as_str().unwrap_or_default().to_owned();
                    let call_id = value["call_id"].as_str().unwrap_or_default().to_owned();
                    let input = value["input"].to_string();
                    let result = module
                        .call(
                            "tool-middleware-api",
                            "before-tool-call",
                            &[
                                Val::String(name),
                                Val::String(call_id),
                                Val::String(input),
                                turn_metadata(None),
                            ],
                        )
                        .await
                        .map_err(error)?;
                    if let Some(updated) = replacement(result)? {
                        value["input"] = updated;
                    }
                    Ok(value)
                })
            }),
        );
        let module_after = Arc::clone(module);
        let loader_after = Arc::clone(loader);
        registrar.on_transform(
            ToolResultTransform::ID,
            0,
            "wasm-after-tool",
            Arc::new(move |mut value| {
                let module = Arc::clone(&module_after);
                let _loader = Arc::clone(&loader_after);
                Box::pin(async move {
                    let output = value["result"].to_string();
                    let is_error = value["is_error"].as_bool().unwrap_or(false);
                    let result = module
                        .call(
                            "tool-middleware-api",
                            "after-tool-call",
                            &[
                                Val::String(String::new()),
                                Val::String(String::new()),
                                Val::String(String::new()),
                                Val::String(output),
                                Val::Bool(is_error),
                                turn_metadata(None),
                            ],
                        )
                        .await
                        .map_err(error)?;
                    if let Some(updated) = replacement(result)? {
                        value["result"] = updated;
                    }
                    Ok(value)
                })
            }),
        );
    }
    if module.roles.contains(&"event-sink") {
        let module = Arc::clone(module);
        let loader = Arc::clone(loader);
        registrar.on_notify(
            EventPublished::ID,
            0,
            "wasm-events",
            Arc::new(move |value| {
                let module = Arc::clone(&module);
                let _loader = Arc::clone(&loader);
                Box::pin(async move {
                    let event = Val::Record(vec![
                        string("kind", value["kind"].as_str().unwrap_or_default()),
                        string(
                            "session-id",
                            value["session_id"].as_str().unwrap_or_default(),
                        ),
                        string("run-id", value["run_id"].as_str().unwrap_or_default()),
                        string("turn-id", value["turn_id"].as_str().unwrap_or_default()),
                        string(
                            "message-id",
                            value["message_id"].as_str().unwrap_or_default(),
                        ),
                        string(
                            "tool-call-id",
                            value["tool_call_id"].as_str().unwrap_or_default(),
                        ),
                        string("epoch-id", value["epoch_id"].as_str().unwrap_or_default()),
                        ("timestamp-unix-millis".into(), Val::S64(0)),
                        string("payload-summary", value.to_string()),
                    ]);
                    report_notify(
                        &module,
                        "event-sink",
                        module.call("event-sink-api", "emit", &[event]).await,
                    )?;
                    Ok(Value::Null)
                })
            }),
        );
    }
    if module.roles.contains(&"hook") {
        for (point, function) in [
            (RunStarted::ID, "before-run"),
            (RunSettled::ID, "after-run"),
            (TurnStarted::ID, "before-turn"),
            (TurnCompleted::ID, "after-turn"),
        ] {
            let module = Arc::clone(module);
            let loader = Arc::clone(loader);
            registrar.on_notify(
                point,
                0,
                format!("wasm-{function}"),
                Arc::new(move |_value| {
                    let module = Arc::clone(&module);
                    let _loader = Arc::clone(&loader);
                    Box::pin(async move {
                        let mut args = vec![turn_metadata(None)];
                        if function == "after-run" {
                            args.push(Val::String("settled".into()));
                        }
                        report_notify(
                            &module,
                            function,
                            module.call("hook-api", function, &args).await,
                        )?;
                        Ok(Value::Null)
                    })
                }),
            );
        }
    }
    if module.roles.contains(&"model-controls") {
        let module = Arc::clone(module);
        let loader = Arc::clone(loader);
        registrar.on_around(ModelStream::ID, 0, "wasm-model-controls", Arc::new(move |mut value, next| {
            let module = Arc::clone(&module);
            let _loader = Arc::clone(&loader);
            Box::pin(async move {
                let controls = json!({
                    "temperature": value.get("temperature"), "top-p": value.get("top_p"),
                    "max-tokens": value.get("max_tokens"), "tool-choice": value.get("tool_choice"),
                    "stop": value.get("stop"),
                });
                let result = module.call("model-controls-api", "before-model-request", &[turn_metadata(None), Val::String(controls.to_string())]).await.map_err(error)?;
                if let Some(updated) = replacement(result)? {
                    for (guest_key, native_key) in [("temperature", "temperature"), ("top-p", "top_p"), ("max-tokens", "max_tokens"), ("tool-choice", "tool_choice"), ("stop", "stop")] {
                        if let Some(field) = updated.get(guest_key) { value[native_key] = field.clone(); }
                    }
                }
                next.call(value).await
            })
        }));
    }
    Ok(())
}

struct WasmGuard {
    module: Arc<LoadedModule>,
    _loader: Arc<Loader>,
}

impl ToolGuard for WasmGuard {
    fn id(&self) -> &'static str {
        "wasm-permissions-policy"
    }
    fn check(&self, name: &str, arguments: &Value) -> GuardDecision {
        self.decide(name, arguments, "", "", "", "", "")
    }
    fn check_with_context(&self, context: GuardContext<'_>) -> GuardDecision {
        let mut decision = GuardDecision::Abstain;
        for permission in context
            .tool
            .required_permissions
            .iter()
            .map(String::as_str)
            .chain(
                std::iter::once("").take(usize::from(context.tool.required_permissions.is_empty())),
            )
        {
            let next = self.decide(
                &context.tool.name,
                context.arguments,
                &context.call_id.to_string(),
                &context.session_id.to_string(),
                &context.run_id.to_string(),
                permission,
                permission,
            );
            decision = match (decision, next) {
                (GuardDecision::Deny, _) | (_, GuardDecision::Deny) => GuardDecision::Deny,
                (GuardDecision::Ask, _) | (_, GuardDecision::Ask) => GuardDecision::Ask,
                (GuardDecision::Allow, _) | (_, GuardDecision::Allow) => GuardDecision::Allow,
                _ => GuardDecision::Abstain,
            };
            if decision == GuardDecision::Deny {
                break;
            }
        }
        decision
    }
}

impl WasmGuard {
    #[allow(clippy::too_many_arguments)]
    fn decide(
        &self,
        name: &str,
        arguments: &Value,
        call_id: &str,
        session_id: &str,
        run_id: &str,
        permission: &str,
        pattern: &str,
    ) -> GuardDecision {
        let module = Arc::clone(&self.module);
        let name = name.to_owned();
        let summary = arguments.to_string();
        let call_id = call_id.to_owned();
        let session_id = session_id.to_owned();
        let run_id = run_id.to_owned();
        let permission = permission.to_owned();
        let pattern = pattern.to_owned();
        std::thread::spawn(move || {
            let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            else {
                return GuardDecision::Deny;
            };
            runtime.block_on(async move {
                let request = Val::Record(vec![
                    string("tool-name", name),
                    string("tool-call-id", call_id),
                    string("permission", permission),
                    string("pattern", pattern),
                    string("arguments-summary", summary),
                    string("session-id", session_id),
                    string("run-id", run_id),
                ]);
                match module
                    .call("permissions-policy-api", "decide", &[request])
                    .await
                {
                    Ok(Val::Result(Ok(Some(decision)))) => match *decision {
                        Val::Record(fields) => match fields.iter().find_map(|(key, value)| {
                            if key == "action" {
                                if let Val::Enum(action) = value {
                                    Some(action.as_str())
                                } else {
                                    None
                                }
                            } else {
                                None
                            }
                        }) {
                            Some("allow") => GuardDecision::Allow,
                            Some("ask") => GuardDecision::Ask,
                            Some("deny") => GuardDecision::Deny,
                            _ => GuardDecision::Deny,
                        },
                        _ => GuardDecision::Deny,
                    },
                    _ => GuardDecision::Deny,
                }
            })
        })
        .join()
        .unwrap_or(GuardDecision::Deny)
    }
}
