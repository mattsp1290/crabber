"""Opt-in driver using existing host Datadog APM instrumentation.

Install the official ddtrace package in a host-managed environment and configure
its existing Datadog Agent endpoint with DD_TRACE_AGENT_URL. No keys are printed.
This emits data; linked-product verification is a separate, required read step.
"""
import os
import subprocess
import time
from ddtrace import tracer
from ddtrace._trace.context import Context

for required in ("DD_SITE", "DD_API_KEY", "DD_APP_KEY", "DD_TRACE_AGENT_URL"):
    if not os.environ.get(required):
        raise SystemExit(f"{required} required")
source = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
marker = f"crabber-host-{time.time_ns()}"
admission = tracer.start_span("crabber.host.admission", child_of=Context(), service="crabber-host")
recovery = tracer.start_span("crabber.host.recovery", child_of=Context(), service="crabber-host")
for span in (admission, recovery):
    span.set_tag("verify", marker)
    span.set_tag("source_sha", source)
environment = os.environ.copy()
environment.update({
    "CRABBER_TRACE_LIVE": "1", "CRABBER_TRACE_MARKER": marker,
    "CRABBER_HOST_TRACE_ID": f"{admission.trace_id:032x}",
    "CRABBER_HOST_SPAN_ID": f"{admission.span_id:016x}",
    "CRABBER_RECOVERY_TRACE_ID": f"{recovery.trace_id:032x}",
    "CRABBER_RECOVERY_SPAN_ID": f"{recovery.span_id:016x}",
    "DD_SERVICE": "crabber-host", "DD_VERSION": source,
})
try:
    subprocess.run(["cargo", "run", "--quiet", "-p", "host-trace", "--", "memory"], env=environment, check=True)
finally:
    admission.finish()
    recovery.finish()
    tracer.flush()
    tracer.shutdown()
print(f"source={source} marker={marker} live_correlation=UNVERIFIED")
print(f"admission_apm_trace={admission.trace_id:032x} admission_apm_span={admission.span_id}")
print(f"recovery_apm_trace={recovery.trace_id:032x} recovery_apm_span={recovery.span_id}")
