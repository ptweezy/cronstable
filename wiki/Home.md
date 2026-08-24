# cronstable™ wiki

cronstable is a cron replacement built on asyncio that runs natively on Linux, macOS, and Windows. Its "crontab" is written in YAML ([classic Vixie crontabs](Classic-Crontabs) are accepted as-is too), so jobs, schedules, and behavior are all declared in configuration. The daemon reports job failures by email, Sentry, webhook, push notification, or shell command. It retries failing jobs with exponential backoff, emits job metrics to statsd, and serves them to Prometheus from a built-in endpoint. It can expose an optional HTTP control API and a built-in [web dashboard](Web-Dashboard) to watch, run, cancel, and tail jobs live.

An opt-in [durable state store](Durable-State) adds restart-surviving run history, missed-run catch-up, and state primitives for jobs. An optional [DAG engine](Orchestration-and-DAGs) runs dependency-ordered workflows on top of it. An optional [MCP server](MCP) lets AI agents observe the daemon and, when you opt in, act on jobs and DAGs.

When one instance is not enough, opt-in [clustering and leader election](Clustering-and-Leader-Election) lets several replicas run one config without double-running jobs, coordinated by mTLS gossip or fenced through a Kubernetes or etcd lease.

The daemon runs in the foreground, logs to stdout/stderr, and supports arbitrary time zones, which suits Docker, Kubernetes, and 12-factor deployments.

cronstable is a fork of [gjcarneiro/yacron](https://github.com/gjcarneiro/yacron) (by Gustavo Carneiro), continuing development from version 0.19.

## Contents

Every page's sidebar is the canonical index. This list uses its grouping:

- **Getting Started**: [Installation](Installation) · [Command-Line Reference](CLI-Reference) · [Running on Windows](Running-on-Windows) · [Production and Container Deployment](Production-Deployment)
- **Configuration**: [Configuration Reference](Configuration-Reference) · [Classic Crontabs](Classic-Crontabs) · [Schedules and Timezones](Schedules-and-Timezones) · [Business-Day Schedules](Business-Day-Schedules) · [Schedule Linting](Schedule-Linting) · [Hashed Schedules (H)](Hashed-Schedules) · [Commands and Environment](Commands-and-Environment) · [Environment-Variable Interpolation](Environment-Variable-Interpolation) · [Output Capturing](Output-Capturing) · [Includes, Defaults, and Multi-File Config](Includes-and-Defaults) · [Logging Configuration](Logging-Configuration)
- **Job Behavior**: [Concurrency and Timeouts](Concurrency-and-Timeouts) · [Failure Detection and Retries](Failure-Detection-and-Retries) · [Pausing Jobs](Pausing-Jobs) · [Late-Run Detection (SLA)](Late-Run-Detection) · [Inbound Heartbeats](Inbound-Heartbeats) · [Resource Monitoring](Resource-Monitoring) · [Durable State](Durable-State) · [Orchestration and DAGs](Orchestration-and-DAGs) · [Clustering and Leader Election](Clustering-and-Leader-Election) · [Job-Set ID](Job-Set-ID)
- **Integrations**: [Reporting (Mail, Sentry, Shell, Webhook)](Reporting) · [Push Notifications](Push-Notifications) · [Metrics with Prometheus](Metrics-with-Prometheus) · [Metrics with statsd](Metrics-with-Statsd) · [HTTP Control API](HTTP-API) · [LAN Discovery (Bonjour/mDNS)](LAN-Discovery) · [Listener TLS](Listener-TLS) · [Calendar Export (iCal)](Calendar-Export) · [Schedule Pressure](Schedule-Pressure) · [Duplicate Schedule Detection](Duplicate-Schedule-Detection) · [Suggest a Slot](Suggest-a-Slot) · [Why Didn't It Run?](Why-No-Run) · [Web Dashboard](Web-Dashboard) · [Terminal Dashboard](Terminal-Dashboard) · [MCP Server (Model Context Protocol)](MCP)
- **Reference and Development**: [Architecture and Internals](Architecture-and-Internals) · [MCP Server Design](MCP-Server-Design) · [Contributing and Releasing](Contributing-and-Releasing) · [Performance Benchmarks](Performance-Benchmarks) · [Troubleshooting and FAQ](Troubleshooting)
