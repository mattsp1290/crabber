# Independent session jobs

A native tool can explicitly start a new independent session job before returning or suspending. Use `ToolContext::session_invocations().spawn(request, task_factory)` and retain the `SessionInvocationRequest` for reconciliation. Its component and behavior fingerprint describe the actual job implementation/configuration; credentials never belong in either field.

The runtime authenticates the exact running native phase, reserves an independent owner, and acknowledges its fresh Begin before invoking the task factory. An identical request returns `Replayed` and never invokes the supplied factory. An unknown Reserve or Begin acknowledgement also dispatches no factory; reconcile the original request rather than generating a new job identity. This API does not restore a crashed job's future or infer that its effects stopped.

The factory receives a cancellation signal independent of the parent and a cleanup tracker. Parent cancellation, terminal completion, or suspension leaves this job running. Archival permits an authenticated existing native phase to start an independent job; ordinary new admission remains rejected while archived. Include all retained task/process cleanup in the supplied scope. Returning success promises that resources directly owned by the main future have finished their cleanup; registered children are actually joined separately. A failed cleanup or panic leaves an uncertain blocker. Lease expiry never means Joined.

`Agent::drain_session_invocations(session, cancel, bound)` observes exactly that controller's selected session jobs. Set `cancel` to request cooperative cancellation of those jobs. The deadline retains unfinished joins; inspect both `local_joined` and resource pending/failure fields, then reconcile durable owner state. A fresh controller's empty local report does not prove there are no durable owners on another controller. Closing extensions alone does not cancel these explicitly independent jobs.

Successful durable Joined publication follows the actual main-task and child-resource joins. An unknown Joined acknowledgement is retried with its original operation identity and never reruns the job. Finished clean local registrations are reaped to reuse capacity; active and uncertain owners remain retained. Custom stores that do not implement durable session owners reject registration before dispatch.

This API starts the supplied factory under a new owner. It does not move an already registered phase task or its resources into that owner; those remain in their original cleanup scope until an explicit acknowledged transfer and actual cleanup establish otherwise.

This capability alone does not certify full runtime/factory ownership coverage or enable session deletion. Preparation and session-factory coverage are separate prerequisites.
