# Provider borrows of Contextual services

An explicit owned preparation can let a provider binding borrow a real
Contextual Workspace or Session factory member. The neutral API lives in
`crabber-providers`; it does not depend on `crabber-extension`.

An actual `ExtensionFactory::create` receives a `FactoryContext`. Its
`provider_service_source()` returns an opaque source for that original factory
member and generation. Keep this capability in the provider adapter that uses
the service. The source holds weak references and does not itself keep an
acquisition or generation alive. It cannot be reconstructed from metadata.

In `Resolver::bind_owned` or `ProviderAdapter::bind_owned`, pass that source to
`ProviderResourceContext::acquire_service_binding` together with the binding
constructor. Clone the values needed by the constructor into its owned future.
The Runtime accepts the concrete Contextual producer, checks the exact session
context and current generation, and checks native dependency access before
invoking the constructor. It checks the resulting binding against the original
frozen provider descriptor.

The Runtime retains the actual acquisition task before native I/O. Dropping its
response waiter does not cancel or detach that task. An escaped preparation
context cannot start another constructor after the original preparation main
returns. Failed future disposal leaves cleanup incomplete.

The returned binding, its clones, extracted streamers and returned streams retain
both a real mount-member lease and the exact session's dependency reference.
Disposing the original provider object precedes releasing these references.
Pending stream-construction futures also retain them; a poll or destructor panic
leaves the lease and release observer unresolved.
Session close waits for these references and actual native dependency task and
resource joins. Closing one session does not close another session's Workspace
service. Physical Workspace close also waits for the real mount-member lease.

This gate covers the provider binding constructor and the per-session borrow.
The Contextual physical factory may already have executed before that borrow.
Reference release does not certify physical generation shutdown, global provider
service cleanup, arbitrary callbacks or whole Runtime coverage. Shared physical
generations, legacy factory mounts and custom producer implementations are
unsupported by this adapter. Default Contextual preparation declarations and
default HTTP providers remain unchanged; this bridge does not make them owned.
