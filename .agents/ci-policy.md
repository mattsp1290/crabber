# CI policy

CI must run only on Linux. Keep the reference workspace check on
`ubuntu-latest`. Linux arm64 jobs may use `ubuntu-24.04-arm`; keep the complete
PostgreSQL step list in one shared matrix job across both architectures.
Do not add macOS runners, jobs, or matrix entries.
