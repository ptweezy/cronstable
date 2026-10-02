Hey all! Cheers from North Carolina. I built [cronstable](https://github.com/ptweezy/cronstable), an open-source job scheduler with web and terminal dashboards + a native iPhone and iPad app. It started as a fork of Gustavo Carneiro's yacron and is now its official successor. I use it for backups, Minecraft server snapshots, data pipelines, and monitoring my mining operations (that net me a whopping total of $0.04/day)! I'm hoping the /r/selfhosted community is able to get some use out of it as well.

A bare install is just the scheduler. Nearly everything below is opt-in. Dashboards, durable state, clustering, push alerts, and the MCP server are all optional, of course, and can be switched on in your config when and if you want them.

Features include, but are not limited to:

* Scheduling: use YAML or bring your existing crontab. Time zones, business-day schedules, and tools to catch scheduling mistakes before they catch you by surprise.
* Failure handling: define what “failed” means for your jobs, retry automatically, and get alerts for failures, missed runs, or jobs taking too long.
* Dashboards: live logs, run history, CPU/memory charts, cluster views, and way more
* Workflows: chain tasks together, pass data between them, add approval gates, and retry the failed parts while keeping successful results.
* Survives restarts: durable state keeps your run history and pending retries, and cronstable catches up on the runs it missed while it was down.
* iOS companion (paid app): scan a QR code to pair, check jobs and logs from your phone, and approve workflow gates from your Lock Screen. Home screen widgets are included. Way more to come soon here.
* Private push alerts: end-to-end encrypted. The notification relay can't read them. Where the platform supports it, the encryption is a post-quantum hybrid (ML-KEM-768 + X25519).
* Clustering: run cronstable on several machines and they elect a leader, so extra nodes give you failover instead of every machine running every job. If the leader dies, another node picks up the schedule. Coordinate through Kubernetes, etcd, a shared folder, or plain mutual TLS between the nodes. The TLS mode needs nothing else running, and it can spread jobs across the fleet to share the load.
* MCP server: connect your favorite agent and ask “what's failing and why?” or “why didn't my backup run at 9?” The agent reads the same jobs, logs, and run history you see in the dashboard, and it checks schedules against cronstable's own engine instead of guessing at cron syntax. It's read-only by default. Opt in and it can run, pause, or approve things for you.
* Deployment: releases ship 38 prebuilt binaries for Linux, macOS, Windows, FreeBSD, OpenBSD, NetBSD, and illumos, on x86, ARM, RISC-V, POWER, IBM Z, and a few stranger chips (the GitHub Actions minutes are free on public repos, so I'm being sure to get my use out of them). Docker images come in eight variants, including Alpine and distroless, and you can also install with pip, Homebrew, or WinGet.
* Integrations: Prometheus metrics and a REST API.

The core is free and MIT-licensed. The iOS app is a $2.99 download that covers one server and 500 push alerts per device each month. An optional Pro subscription makes both unlimited.

[GitHub](https://github.com/ptweezy/cronstable) · [iOS app](https://apps.apple.com/app/cronstable/id6801933039)

If there's something you need from a job scheduler/coordinator that I'm missing, I want to hear about it. I am excited to continue to improve and expand the cronstable suite! Thanks y'all!
