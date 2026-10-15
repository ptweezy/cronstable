# Status badges

cronstable serves small SVG images that show the health of one job, or of
every job at once, in the style of the build badges on many README files.
Put one in a README, a wiki page, or a home dashboard, and readers can see
whether a job passes without opening the [web dashboard](Web-Dashboard).

![nightly-backup: passing](https://raw.githubusercontent.com/ptweezy/cronstable/main/docs/img/badge-passing.svg)
![db-vacuum: failing](https://raw.githubusercontent.com/ptweezy/cronstable/main/docs/img/badge-failing.svg)
![weekly-report: overdue](https://raw.githubusercontent.com/ptweezy/cronstable/main/docs/img/badge-overdue.svg)
![cronstable: 12 passing](https://raw.githubusercontent.com/ptweezy/cronstable/main/docs/img/badge-fleet.svg)

## The badge endpoints

| Endpoint | Shows |
|----------|-------|
| `GET /jobs/{name}/badge.svg` | The job's name and its verdict. An unknown job name gets a `404`. |
| `GET /badge.svg` | `cronstable` and a count of the jobs in the most urgent state. |

Both endpoints are on by default whenever the [HTTP API](HTTP-API) is on.
They follow the API's authentication: a daemon that requires a token serves
them only to callers that send it. A browser also refuses to load a badge
from the daemon into a page from another origin. To lift both limits,
[make the badges public](#make-badges-public).

```console
curl http://localhost:8080/jobs/nightly-backup/badge.svg
curl http://localhost:8080/badge.svg
```

## What a job's badge says

A job's badge shows the first message in this table that applies to the job:

| Message | Color | When |
|---------|-------|------|
| `disabled` | gray | The job has `enabled: false`. |
| `paused` | gray | A [runtime pause](Pausing-Jobs) is in effect. |
| `failing` | red | The last run failed. |
| `overdue` | orange | An [SLA check](Late-Run-Detection) is in breach. |
| `passing` | green | The last run succeeded. |
| `cancelled` | gray | The last run was canceled. |
| `unknown` | gray | The last run was interrupted before it recorded an outcome. |
| `running` | blue | A run is in progress, and the daemon has no earlier run on record. |
| `pending` | gray | The daemon has no run of the job on record, such as before its first run. |

The badge reads the same state as the dashboard's status column and its
OVERDUE chip. A run that is in progress, or waiting in a
[resource pool](Resource-Pools) queue, leaves the badge on the last run's
result, so the badge of a job that runs every minute holds steady at
`passing` or `failing`. A scheduled run that a pause held back ran nothing,
so the badge keeps the last run's result while the dashboard shows the slot
as **Skipped**. While the daemon runs, a failing job still reads `failing`
after a maintenance pause of any length.

Without a [`state` section](Durable-State), the daemon keeps run history in
memory, so after a restart a job's badge reads `pending` until the job runs
again. With one, the daemon reads each job's newest records back from its
[run ledger](Durable-State#the-durable-run-ledger) after a restart, and
the badge shows the last run they hold. When a job's newest records are
all slots that a pause skipped, the daemon looks further back for the
job's last run after the startup reads finish, one job at a time. Until
that look-back finishes, the badge reads `pending`, or `paused` while the
pause holds. If a look-back times out, for example because the store is
slow, the daemon stops looking, and the badges of that job and of the jobs
still waiting read `pending` until each job runs again. A badge also reads
`pending` until the job runs again when the daemon can't read the ledger
at all after a restart.

The ledger keeps `state.maxRunsPerJob` records per job (1000 by default),
skipped slots included. The look-back stops after 10,000 records, even with
`maxRunsPerJob: 0`, which keeps every record. If a pause skips as many slots
as the ledger keeps, or 10,000 slots if that's fewer, the last run is out of
reach, and the badge reads `pending` until the job runs again.

A badge reports the runs that the node serving it has on record, as that
node's dashboard does. In a [cluster](Clustering-and-Leader-Election), point
a job's badge at the node that runs the job, not at a load balancer in front
of several nodes. The fleet badge counts the jobs as that node sees them. On
a gossip cluster, or a lease cluster with `cluster.observability`,
[`GET /fleet`](HTTP-API#get-fleet) has the cluster-wide view.

## What the fleet badge says

The fleet badge counts the jobs whose own badges show the most urgent of
three messages:

1. `failing`, in red, such as `2 failing`.
2. `overdue`, in orange, when no job is failing.
3. `passing`, in green, when no job is failing or overdue.

The fleet badge reads `no status` when no job shows any of the three, for
example before any job has run. It reads `no jobs` when the configuration
has none.

## Query parameters

| Parameter | Default | Meaning |
|-----------|---------|---------|
| `label` | The job's name, or `cronstable` on the fleet badge | The text of the badge's left half: 1 to 64 characters, with no control characters or line breaks. |
| `style` | `flat` | `flat` draws rounded corners and a soft gradient. `flat-square` draws square corners with no gradient. |

A `label` that is blank, too long, or contains a control character or a
line break gets a `400` whose JSON error body names the problem, and so
does a `style` outside those two. An empty `style` takes the default. A
label made only of spaces and zero-width characters, such as a zero-width
space or a combining mark, counts as blank.

```console
curl "http://localhost:8080/jobs/nightly-backup/badge.svg?label=nightly%20backup&style=flat-square"
```

![nightly backup: passing, flat-square style](https://raw.githubusercontent.com/ptweezy/cronstable/main/docs/img/badge-flat-square.svg)

## Embed a badge

The [web dashboard](Web-Dashboard)'s command palette lists
**Copy badge Markdown** for each job, and **Copy fleet badge Markdown**. The
[terminal dashboard](Terminal-Dashboard)'s palette has the same commands.
The palettes list them only when the daemon serves badges, which they read
from [`GET /whoami`](HTTP-API#get-whoami) about once a minute, so the
commands follow a configuration reload. No URL reaches the badge of a job
whose name is empty, `.`, or `..`, or contains `{` or `}`, so the palettes
leave out that job's **Copy badge Markdown**. Each command copies Markdown
that shows the badge and links it to the dashboard. Unless the badges are
public, the confirmation adds "embeds need web.badges.public" (see
[make badges public](#make-badges-public)). A job's badge links to the
job's drawer:

```markdown
[![nightly-backup](https://cron.example.com/jobs/nightly-backup/badge.svg)](https://cron.example.com/#job/nightly-backup)
```

The image's alt text is the job's name, with a space in place of each line
break. A backslash escapes each character in the name that Markdown would
read as syntax, such as a bracket, a backtick, or `|`. The Markdown uses the
address that the dashboard connects to, without any user name or password in
it. If readers reach the daemon at another address, such as a reverse proxy,
change the host before you publish the Markdown. On a daemon that requires a
token, the link opens the
[token prompt](Web-Dashboard#authentication) first, and the job's drawer
opens after the reader saves the token. A daemon with `web.ui: false`
serves no dashboard page, so the dashboards copy its badges without the
link.

A page that accepts HTML can use an image tag instead:

```html
<a href="https://cron.example.com/"><img src="https://cron.example.com/badge.svg" alt="cronstable status"></a>
```

## Make badges public

Pages fetch embedded images without your bearer token. GitHub fetches README
images through its image proxy, and a wiki or a home dashboard fetches them
from each reader's browser. On a daemon that requires a token, both fetches
fail.

Even when the daemon requires no token, a browser refuses to load a badge
from the daemon into a page from another origin. The badge's
`Cross-Origin-Resource-Policy: same-origin` header blocks it as an image,
and its `frame-ancestors 'self'` policy blocks it in a frame, such as an
`<object>`, `<embed>`, or `<iframe>`. Without these headers, any website
that a reader visits could load a badge and tell the job's verdict from
its size. GitHub's image proxy fetches the badge from GitHub's servers and
serves it with its own headers, which leave out
`Cross-Origin-Resource-Policy`, so on a daemon that requires no token, a
badge in a GitHub README shows without `web.badges.public`.

Set `web.badges.public` to lift both limits:

```yaml
web:
  listen:
    - http://0.0.0.0:8080
  authToken:
    fromEnvVar: CRONSTABLE_WEB_TOKEN
  badges:
    public: true
```

The setting exempts only the two badge routes from the token, and their
headers then let pages on any origin load them as images or in frames (see
[security headers](#security-headers)). A public badge shows a job's name
and verdict. Anyone who can reach the listener, and any website open in a
browser that can reach it, can request the badge of any job name, learn
from a `404` whether a job by that name exists, and read the verdict of a
job that does. Leave `public` off when job names or their health are
sensitive.

GitHub's image proxy fetches from the internet, so a badge in a GitHub
README also needs an address that the internet can reach, such as a reverse
proxy in front of the daemon. An `https://` listener with `web.tls.clientCa`
requires a client certificate, which GitHub's image proxy can't present, so
serve the badges through a listener or a reverse proxy that doesn't require
one.

The `view` scope covers both badge routes, so a daemon that serves a
[public read-only board](HTTP-API#public-read-only-access-webanonymousscopes)
through `web.anonymousScopes` serves its badges without a token. A page
from another origin can load them straight from the daemon only with
`web.badges.public`.

## Caching

Each badge image carries an `ETag`, a hash of the image's bytes, and
`Cache-Control: no-cache`. The `no-cache` directive tells a cache, such as
the image proxy that GitHub uses for README images, to check with the daemon
before it reuses a stored copy. While the badge is unchanged, that check gets
`304 Not Modified` and no image data. The daemon always sets these two
headers itself, so a `web.headers` entry can't replace them.

While nothing changes, the daemon reuses one count of the fleet badge's jobs
for up to a second, however often pages request it. A run that starts or
finishes, a pause that you set or lift, or a configuration reload makes the
next request count again. A pause that runs out, or a new SLA breach, shows
within a second.

## Configuration

The `web.badges` option takes a boolean or a map:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `enabled` | bool | `true` | Serve both badge routes. |
| `public` | bool | `false` | Serve both badge routes without the bearer token, and let pages from other origins load the badges. |

`badges: true` is the same as the default. With `badges: false`, the daemon
doesn't register the badge routes, so a request for a badge gets the same
answer as a request for any unknown path.

## Security headers

A browser that opens a badge URL directly renders the image as a document of
the daemon's origin. The daemon escapes all of the text in the image, and
every badge image, including a `304` reply, carries these headers:

| Header | Default | [Public badges](#make-badges-public) |
|--------|---------|---------------|
| `Content-Security-Policy` | `default-src 'none'; frame-ancestors 'self'` | `default-src 'none'` |
| `X-Content-Type-Options` | `nosniff` | `nosniff` |
| `X-Frame-Options` | `SAMEORIGIN` | Not sent |
| `Cross-Origin-Resource-Policy` | `same-origin` | `cross-origin` |

By default, `Cross-Origin-Resource-Policy` stops a page from another origin
from loading a badge as an image, and `frame-ancestors` stops it from
framing one. `X-Frame-Options` still stops the frame when a
`Content-Security-Policy` entry in `web.headers` leaves out
`frame-ancestors`. A `web.headers` entry with the same name, in any letter
case, replaces a default, as on the dashboard page. The entries apply to
public badges too.

See also: [HTTP Control API](HTTP-API), [Web Dashboard](Web-Dashboard),
[Late-Run Detection (SLA)](Late-Run-Detection), and
[Calendar Export (iCal)](Calendar-Export).
