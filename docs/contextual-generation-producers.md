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

`work.observation()` retains that exact generation independently of acquisition
waiters and the scheduling carrier. Capture it during the original host call.
If only a refused carrier survives after its registration expires, `observation()`
returns `ContextInitializationIncomplete`; it never consults a replacement slot.
`request_close()` signals only the original
slot; it refuses a replacement generation and detects initializer/mount
self-close. `registered_graph_join(bound)` waits for the original initializer,
host and constructor task observations, original factory cleanup, and actual owned
mount joins. It does not initiate close. A timeout leaves those original joins
retained for another observation.

The opaque `JoinedContextualGenerationGraph` preserves the frozen constructor
roster. Its dispositions distinguish members never entered from entered members
whose cleanup joined, including mounted members with an actual owned mount join.
A published initializer result, discarded operational record, or a subsequent
successful generation cannot replace the original task evidence. Shutdown and
cleanup still run concurrently to support nested dependencies.

This aggregate proves only joins in the registered graph. Held extension/tool/
service objects can retain resources after their invocation ingress closes.
Neither this aggregate nor `FullySealedFactory` certifies final object disposal,
physical generation closure, native ownership, or a whole-Run resource policy.

## Non-exposing Box roots

`ContextualPlanProvider::new_with_product_roots` is a distinct opt-in factory
family for Workspace and Session roots. `ContextualProductFactory::create_root`
returns an owned `Box<dyn ContextualProductRoot>`; this first route exposes no
object, registration or callable projection. The frozen `root-cell-v1` component
includes the original factory identity, configuration and scope. Existing Arc
factories and their fingerprints keep their existing behavior.

The producer installs the actual returned Box in its original retained cell
**before disposing the original create future**. Closing an installed cell, or
installing into an already-closing cell, synchronously retains an actual blocking
destructor task before opening its gate. Open roots occupy no parked blocking
threads. The destructor can run while create-future disposal is still pending;
either task's completion substitutes for the other's join.

Capture `work.observation()` during the original host call, then obtain
`observation.product_root(member)` after that original member enters. Its
`disposed(bound)` waits without initiating close and returns opaque
`DisposedContextualProductRoot` only after the original registered graph and the
original Box destructor have joined. Timeouts and dropped observation waiters
retain those same tasks. A constructor that never returned a Box cannot produce
this evidence, even when its registered graph joined. Destructor, panic-payload
and create-future disposal failures remain incomplete and never repeat Drop.
Original initializer self-close detection also covers destructor execution and
panic-payload disposal.

This evidence concerns only the returned Box's actual destructor. It does not
certify disposal of factory objects, hidden shared references, globals, arbitrary
background work or a complete physical generation. Existing cleanup-policy labels
keep their registered-cleanup meaning. No native permission, Runtime ownership,
`PhysicalClosed`, default preparation declaration or whole-Run policy is added.

## One ordinary Box Tool node

`new_with_tool_products` is a separate opt-in family. Its factory returns a
move-only `ContextualToolProduct::new(info, Box<dyn ToolExecutor>)`. This first
tier accepts ordinary tools only: a resumable declaration or capability is
refused, and the original live identities are checked again inside actual
invocations. Existing resumable/Arc factory behavior is unchanged.

The original returned Box is installed before original create-future disposal.
Registry records contain private templates; each projected executor facade gets
one controlled reference lease. All `Arc` clones of that facade share the lease,
which releases on final actual facade Drop. Holding an extracted executor after
plan Drop keeps Box disposal pending. After close it cannot start a new call.
Up to 64 distinct projected facades may be live per node; a further projection
returns capacity and rolls back that acquisition. Released allocations do not
consume a lifetime limit.

The same concrete owned-mount invocation task constructs, polls and disposes the
original borrowed Tool future. Node close seals admission and waits that exact
original invocation roster and final facade releases before transferring the Box
into its actual retained blocking destructor. It can drive Box Drop concurrently
with still-pending original create-future disposal and factory cleanup. No open
node occupies a parked blocking thread. Failure stays incomplete while actual
cleanup still runs; neither task completion nor an empty roster replaces its
original joins.

`observation.tool_product(member).disposed(bound)` waits for the exact original
node and original graph without initiating close. Its opaque result covers this
Box, projected references and invocations only. Factory objects, opaque shared
references, globals, other interfaces and complete physical/resource policy
remain outside this evidence. No native permission, default owned declaration,
`PhysicalClosed` or whole-Run policy is added.
