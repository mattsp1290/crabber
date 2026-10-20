# Actual callbacks from owned mounts

Plans acquired from a registry retain callback invocation ownership for handlers
installed by `Registry::mount_owned`. An extracted `plan.dispatcher.clone()` can
outlive the plan, but each invocation still enters its original concrete mount.
An idle dispatcher clone does not itself keep that mount open.

Admission checks the actual mount before callback construction and again in the
registered task body. If close seals first, the callback is never constructed.
Already admitted work is retained independently of its caller. Dropping a caller
requests cancellation; the mount still owns the original future's disposal and
actual task join. Mount shutdown waits those observations, the original member
leases, and registered cleanup. A timeout retains the same work for a later
close call. Abnormal construction, polling, disposal, or joining leaves cleanup
incomplete rather than granting a successful `OwnedMountJoin`.

Callback output is handed back only after original future disposal and actual
task join. Result-transform chains keep their existing cancellation/deadline
and accepted-output rules: a late result cannot become a committed output after
interruption. A callback still being disposed remains mount-owned even when its
chain waiter has returned. A typed result chain with actual phase cleanup
resources registers one observer for the whole chain; that phase cannot finish
cleanup until the same original callback joins complete. Around middleware preserves the original `Next`
single-call/expiry rules and the complete ancestor mount context. Initializer
reentry guards and the exact existing fenced state sink also travel with the
retained callback through construction, polling, and disposal.

At most 64 callback mains can be retained by one mount at once. Capacity refuses
before callback construction. Actually observed successful joins are reaped, so
sequential callbacks do not exhaust a lifetime allowance. Failed observations
remain sticky. This bound concerns callback tasks, not the size of their input
or arbitrary host resources.

Other callers can attach a non-cloneable `CallbackInvocationOwner` to actual
`ToolResources`, then scope calls with `with_callback_invocations` and the owner's
opaque capability. Exactly one retained observer is registered in those parent
resources. Owner Drop or actual parent close seals ingress immediately and
awaits the original callback joins. Capability clones cannot delay or reverse
that seal. Its active roster also reaps only actual successful joins, so a long
worker can run sequential callbacks without consuming a parent slot per call.
Without an attachment there is no caller-quiescence proof; mount ownership alone
does not establish phase or worker cleanup. Runtime tool/model integration must
bind this API to its actual owners before consuming this component as evidence.

This component changes fingerprints only for concrete owned registry mounts.
Public `Dispatcher::new` and legacy mounts gain no owned invocation source.
Other extracted tools, guards, contributors, provider references, arbitrary
callback-created detached tasks, and returned streams need their own ownership
bridges. This callback-main component grants no native permission, whole-Run
resource policy, or complete physical-generation shutdown evidence.
