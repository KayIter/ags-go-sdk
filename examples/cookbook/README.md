# Cookbook

Each example is a small, compilable user journey. Replace placeholder Tool or Sandbox IDs before
running it. All creation examples close local resources and delete only the instance they created.

| Journey | Example | Contract |
| --- | --- | --- |
| Environment-backed create, command, delete | [01_default](01_default/main.go) | `default-create-command-delete` |
| Explicit Client and isolated configuration | [02_explicit_client](02_explicit_client/main.go) | `explicit-client-lifecycle`, `multiple-cloud-identities` |
| Managed Code context | [03_code_context](03_code_context/main.go) | `code-context` |
| Metrics with Start and End | [04_metrics](04_metrics/main.go) | `metrics-window` |

The journey IDs are machine-readable in [`contracts/journeys.json`](../../contracts/journeys.json).
File streaming, Watch, PTY, lifecycle, and cleanup behavior are covered by the root README and
offline contract tests.
