# Local Contextual constructor scheduling

`ContextualPlanProvider::new_with_generation_host` opts Workspace and Session
factories into a local scheduling hook fixed before `ready()` or acquisition.
The ordinary constructor retains its existing behavior. Preconstructed Shared
extensions remain on their legacy path.

The host receives a move-only `ContextualGenerationWork`. It can inspect the
original selected generation and bounded frozen constructor identities. To admit
that local work, return `ContextualGenerationAdmission::Admitted(work)` with the
exact input carrier. Another slot's carrier is refused. The carrier exposes no
constructor invocation or dispatch method and owns neither actual task handle.
Local admission is **not native permission**, a resource contract, or evidence of
physical shutdown.

The producer retains the real parked constructor task and the actual host call
before the host future is constructed. Only a matching local admission opens the
constructor gate. Closing is checked after the host call and inside the actual
constructor body. Dropping or timing out an acquisition waiter never drops these
joins. An admitted constructor that is already running remains owned until its
actual future and registered cleanup return.

`Refused` leaves the original generation without invoking its factory. `Replayed`,
`Uncertain`, a wrong carrier, and a host construction/poll/disposal panic also
invoke no factory and retain an incomplete original outcome. Repeated acquisition
does not register another host call, select another generation, or treat metadata
as permission. Known refused work can close after its actual no-dispatch join;
uncertain work continues to report incomplete cleanup. A cooperative host can
await `work.closing()` to finish its original pending call when the owner closes.

The constructor's actual main observation and its cleanup observations stay
separate. Constructor return does not close its mounted services. Existing plan,
member, provider-binding and stream references still govern their own lifetime.
Closing Session A releases its references while a shared Workspace generation
can continue serving Session B. Shutdown and dependency drains remain concurrent
so a shutdown callback can release a nested plan without deadlock.

This hook has no Runtime/Store adapter or native source protocol. It supports no
Global factory, namespace, `PhysicalClosed` aggregate, default owned-preparation
declaration, or whole-Run policy upgrade. Escaped callbacks and legacy/global
resources retain their existing coverage limits. Native ownership and a complete
physical generation graph require separate implementation and qualification.

Host future construction, polling and disposal, and the actual constructor body
retain the original initializer's owner context. A host or factory trying to
close its own initialization receives `SelfClose` before mutating that owner;
close from another caller still signals and observes the retained tasks.
