#!/usr/bin/env bash
#
# Install the demo daemon, demo-only gateway and tunnel as launchd agents,
# for a macOS host with no container runtime. The container path
# (../docker-compose.yml) is the default; see ../README.md for when to prefer
# which. Safe to re-run: it re-derives the config, reloads all three agents,
# and waits for the previous daemon and gateway to let go of their ports
# before the next ones start (the comment above the bootout says why).
#
#   brew install cronstable cloudflared
#   cloudflared tunnel login
#   cloudflared tunnel create cronstable-demo
#   cloudflared tunnel route dns cronstable-demo demo.cronstable.com
#   ./install.sh [tunnel-name]            # default: cronstable-demo
#
# The view token is public by design; override to rotate:
#   CRONSTABLE_DEMO_VIEW_TOKEN=new-value ./install.sh
#
# The operator token is not public: it carries control+approve for the
# gateway and demo-operator job. One is generated on first install and reused after that
# (see OPERATOR_TOKEN_FILE below); override to rotate:
#   CRONSTABLE_DEMO_OPERATOR_TOKEN=new-value ./install.sh
#
# The daemon defaults to the `cronstable` on PATH. Point CRONSTABLE_BIN at
# another build (a venv's bin/cronstable) to deploy that one; the agent runs
# it, and jobs resolve the bare `cronstable` name to the same build:
#   CRONSTABLE_BIN=/path/to/venv/bin/cronstable ./install.sh
#
set -euo pipefail

TUNNEL_NAME="${1:-cronstable-demo}"
VIEW_TOKEN="${CRONSTABLE_DEMO_VIEW_TOKEN:-cronstable-public-demo-view}"
HOSTNAME_PUBLIC="${CRONSTABLE_DEMO_HOSTNAME:-demo.cronstable.com}"

die() { printf 'install.sh: %s\n' "$1" >&2; exit 1; }

[ "$(uname -s)" = "Darwin" ] || die "launchd agents are macOS only"
command -v brew        >/dev/null || die "Homebrew not found"
command -v cloudflared >/dev/null || die "cloudflared not on PATH (brew install cloudflared)"

# Which cronstable serves the board: CRONSTABLE_BIN, else the one on PATH.
if [ -n "${CRONSTABLE_BIN:-}" ]; then
    [ -x "$CRONSTABLE_BIN" ] || die "CRONSTABLE_BIN=$CRONSTABLE_BIN is not an executable"
    # made absolute: it goes verbatim into the plist, and launchd resolves
    # nothing.
    CRONSTABLE="$(cd "$(dirname "$CRONSTABLE_BIN")" && pwd)/$(basename "$CRONSTABLE_BIN")"
else
    command -v cronstable >/dev/null || die "cronstable not on PATH (brew install cronstable, or set CRONSTABLE_BIN)"
    CRONSTABLE="$(command -v cronstable)"
fi
CRONSTABLE_DIR="$(dirname "$CRONSTABLE")"

# The gateway is a separate Python process using the standard daemon library
# only to validate its config. A source-install venv can serve both processes;
# a frozen/Homebrew daemon needs a Python venv for the gateway alongside it.
GATEWAY_PYTHON="${CRONSTABLE_DEMO_PYTHON:-$CRONSTABLE_DIR/python3}"
if [ ! -x "$GATEWAY_PYTHON" ]; then
    [ -z "${CRONSTABLE_DEMO_PYTHON:-}" ] || die "CRONSTABLE_DEMO_PYTHON is not executable"
    GATEWAY_PYTHON="$(command -v python3)"
fi
"$GATEWAY_PYTHON" -c 'import aiohttp; from cronstable.config import parse_config_with_sources' \
    >/dev/null 2>&1 || die "gateway needs a Python venv with cronstable installed; set CRONSTABLE_DEMO_PYTHON to its bin/python (see ../README.md)"

# This board runs on jobs that shell out to the cronstable CLI (state,
# cursor, lock, xcom, secret). A PyInstaller build older than 1.2.43 leaks
# its bootloader's _PYI_* variables into job environments, and the CLI a job
# invokes then refuses to start ("parent process has different executable")
# while the job still exits 0: every one of those features fails silently.
# 1.2.43 scrubs the leak and a source install never had it, so refuse
# exactly the frozen-and-older combination rather than stand up a dark
# board. The _PYI_ marker exists only inside PyInstaller bootloaders; a venv
# entry script cannot match it.
FIRST_SCRUBBED=1.2.43
if grep -aq _PYI_ "$CRONSTABLE" 2>/dev/null; then
    # `|| true`: under set -e a binary whose --version fails (or prints no
    # x.y.z) would otherwise abort the script here wordlessly; an empty
    # version must fall through to the refusal below instead.
    CRONSTABLE_VERSION="$("$CRONSTABLE" --version 2>/dev/null | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1 || true)"
    if [ -z "$CRONSTABLE_VERSION" ] || \
       [ "$(printf '%s\n' "$FIRST_SCRUBBED" "$CRONSTABLE_VERSION" | sort -V | head -1)" != "$FIRST_SCRUBBED" ]; then
        die "frozen cronstable ${CRONSTABLE_VERSION:-of unknown version} predates the $FIRST_SCRUBBED PyInstaller env scrub; its jobs cannot invoke the cronstable CLI, so this board fails silently. Upgrade it, or install from source into a venv and set CRONSTABLE_BIN (see ../README.md)"
    fi
fi

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC_CONFIG="$HERE/../cronstable.yaml"
SRC_CRONTAB="$HERE/../legacy.crontab"
[ -f "$SRC_CONFIG" ]  || die "cannot find $SRC_CONFIG"
# `include:` in the config resolves relative to the deployed config, so the
# crontab has to travel with it.
[ -f "$SRC_CRONTAB" ] || die "cannot find $SRC_CRONTAB (cronstable.yaml includes it)"

PREFIX="$(brew --prefix)"
ETC_DIR="$PREFIX/etc/cronstable-demo"
STATE_DIR="$PREFIX/var/cronstable-demo/state"
LOG_DIR="$PREFIX/var/log"
AGENT_DIR="$HOME/Library/LaunchAgents"
CF_DIR="$HOME/.cloudflared"

# Resolve the tunnel by name. `tunnel create` must have run already; doing it
# here would make re-running the script mint duplicate tunnels. Capture the
# listing first: under `set -e` a failing pipeline would kill the script
# before the `die` below, with cloudflared's own error discarded.
#
# stderr goes to a file, not into the listing: since 2026.8.2 a cloudflared
# that is not the newest release appends a JSON "outdated version" warning
# to stderr on every successful command. Merged into stdout it lands after
# the listing, the parse fails, and that used to be reported as a missing
# tunnel for a tunnel that was up. A listing that does not parse is its
# own error now.
TUNNEL_ERR="$(mktemp -t cronstable-tunnel-list)"
trap 'rm -f "$TUNNEL_ERR"' EXIT
TUNNEL_LIST="$(cloudflared tunnel list --output json 2>"$TUNNEL_ERR")" \
  || die "cloudflared tunnel list failed (run 'cloudflared tunnel login' first?): $(cat "$TUNNEL_ERR")"
TUNNEL_ID="$(printf '%s' "$TUNNEL_LIST" | python3 -c "
import json, sys
name = sys.argv[1]
try:
    tunnels = json.load(sys.stdin)
except Exception as exc:
    print(f'install.sh: cloudflared tunnel list did not return JSON: {exc}', file=sys.stderr)
    sys.exit(1)
for t in tunnels:
    # A live tunnel carries Go's zero timestamp here, not an empty string.
    stamp = t.get('deleted_at') or ''
    deleted = bool(stamp) and not stamp.startswith('0001-01-01')
    if t.get('name') == name and not deleted:
        print(t['id']); break
" "$TUNNEL_NAME")" || die "could not read 'cloudflared tunnel list --output json' (its error is above)"
[ -n "$TUNNEL_ID" ] || die "no tunnel named '$TUNNEL_NAME' (run: cloudflared tunnel create $TUNNEL_NAME)"

CREDS="$CF_DIR/$TUNNEL_ID.json"
[ -f "$CREDS" ] || die "missing tunnel credentials $CREDS"

mkdir -p "$ETC_DIR" "$STATE_DIR" "$LOG_DIR" "$AGENT_DIR"

# The operator credential. Unlike the view token there is no safe default: a
# literal in a public repo would hand every reader control of any host that
# ran this script unmodified. Generate one on first install, keep it in a
# 0600 file so re-running does not invalidate the running daemon's token, and
# let the environment override it to rotate.
OPERATOR_TOKEN_FILE="$ETC_DIR/operator-token"
if [ -n "${CRONSTABLE_DEMO_OPERATOR_TOKEN:-}" ]; then
    OPERATOR_TOKEN="$CRONSTABLE_DEMO_OPERATOR_TOKEN"
    (umask 077; printf '%s' "$OPERATOR_TOKEN" > "$OPERATOR_TOKEN_FILE")
elif [ -s "$OPERATOR_TOKEN_FILE" ]; then
    OPERATOR_TOKEN="$(cat "$OPERATOR_TOKEN_FILE")"
else
    OPERATOR_TOKEN="$(python3 -c 'import secrets; print(secrets.token_urlsafe(32))')"
    (umask 077; printf '%s' "$OPERATOR_TOKEN" > "$OPERATOR_TOKEN_FILE")
    printf 'install.sh: generated a new operator token at %s\n' "$OPERATOR_TOKEN_FILE"
fi
chmod 600 "$OPERATOR_TOKEN_FILE"

# Both tokens are interpolated into a plist below. Generated tokens are
# URL-safe base64, but the header documents overriding either from the
# environment, and a `&`, `<` or `>` in one would emit malformed XML: the
# lint gate further down would then refuse to load an agent that is
# already running, so escape rather than rely on being caught.
xml_escape() {
    printf '%s' "$1" | sed -e 's/&/\&amp;/g' -e 's/</\&lt;/g' -e 's/>/\&gt;/g'
}
VIEW_TOKEN_XML="$(xml_escape "$VIEW_TOKEN")"
OPERATOR_TOKEN_XML="$(xml_escape "$OPERATOR_TOKEN")"

# The kit's config is written for a container. Retarget exactly two values for
# a non-container host, and fail loudly if either stops matching upstream
# rather than silently deploying a config that differs from what is reviewed.
#
#   state.path  -> a writable prefix path, since /var/lib needs root
#   listen      -> loopback, because the container published only a loopback
#                  port; binding the wildcard natively would newly expose the
#                  daemon to the LAN
grep -q '^  path: /var/lib/cronstable/state$'  "$SRC_CONFIG" || die "state.path line not found in cronstable.yaml; update install.sh"
grep -q '^    - http://0.0.0.0:8080$'          "$SRC_CONFIG" || die "listen line not found in cronstable.yaml; update install.sh"

sed -e "s#^  path: /var/lib/cronstable/state\$#  path: $STATE_DIR#" \
    -e "s#^    - http://0.0.0.0:8080\$#    - http://127.0.0.1:8080#" \
    "$SRC_CONFIG" > "$ETC_DIR/cronstable.yaml"

# `include: legacy.crontab` resolves next to the deployed config, not next to
# the source one.
cp "$SRC_CRONTAB" "$ETC_DIR/legacy.crontab"
for asset in gateway.py gateway.json gateway-overlay.html; do
    cp "$HERE/../$asset" "$ETC_DIR/$asset"
done

CRONSTABLE_DEMO_VIEW_TOKEN="$VIEW_TOKEN" \
CRONSTABLE_DEMO_OPERATOR_TOKEN="$OPERATOR_TOKEN" \
  "$CRONSTABLE" -c "$ETC_DIR/cronstable.yaml" --validate-config >/dev/null \
  || die "derived config failed validation"

CRONSTABLE_DEMO_VIEW_TOKEN="$VIEW_TOKEN" \
CRONSTABLE_DEMO_OPERATOR_TOKEN="$OPERATOR_TOKEN" \
  "$GATEWAY_PYTHON" "$ETC_DIR/gateway.py" --config "$ETC_DIR/cronstable.yaml" \
  --origin "https://$HOSTNAME_PUBLIC" --validate \
  || die "gateway policy failed validation"

# This is cloudflared's global default config path; preserve anything a
# human put there before claiming it.
if [ -f "$CF_DIR/config.yml" ] \
  && ! grep -q "Generated by example/demo-instance" "$CF_DIR/config.yml"; then
  BACKUP="$CF_DIR/config.yml.bak.$(date +%Y%m%d%H%M%S)"
  cp "$CF_DIR/config.yml" "$BACKUP"
  printf 'install.sh: existing %s backed up to %s\n' "$CF_DIR/config.yml" "$BACKUP"
fi
cat > "$CF_DIR/config.yml" <<EOF
# Generated by example/demo-instance/launchd/install.sh
#
# Native counterpart of the \`tunnel\` service in ../docker-compose.yml. There
# the tunnel reaches the gateway over the compose network; here the processes
# are launchd agents on one host, so it is plain loopback.
tunnel: $TUNNEL_ID
credentials-file: $CREDS

# Ride out a daemon restart instead of surfacing 502s to the demo.
retries: 5
grace-period: 30s

ingress:
  - hostname: $HOSTNAME_PUBLIC
    service: http://127.0.0.1:8081
  - service: http_status:404
EOF

# Note for editors: a literal double hyphen is illegal inside an XML comment,
# and plutil accepts it anyway. Keep prose in these comments free of it.
cat > "$AGENT_DIR/com.cronstable.demo.plist" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<!-- Generated by example/demo-instance/launchd/install.sh. Edits are lost on re-run. -->
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.cronstable.demo</string>
    <key>ProgramArguments</key>
    <array>
        <string>$CRONSTABLE</string>
        <string>-c</string>
        <string>$ETC_DIR/cronstable.yaml</string>
    </array>
    <key>EnvironmentVariables</key>
    <dict>
        <!-- Public by design; it ships in the companion app. Anonymous
             visitors already hold the same view scope. -->
        <key>CRONSTABLE_DEMO_VIEW_TOKEN</key>
        <string>$VIEW_TOKEN_XML</string>
        <!-- Not public: control+approve, for the demo-operator job's scripted
             approvals and pauses. Generated per host by this script. This
             plist is chmod 600 below, since it carries that secret. -->
        <key>CRONSTABLE_DEMO_OPERATOR_TOKEN</key>
        <string>$OPERATOR_TOKEN_XML</string>
        <!-- launchd hands out a minimal PATH, and several jobs and DAG tasks
             shell out to the cronstable CLI (xcom, state, cursor, lock,
             secret, artifact) and to python3. The deployed binary's own
             directory leads, so those bare cronstable calls resolve to the
             exact build launchd runs. -->
        <key>PATH</key>
        <string>$CRONSTABLE_DIR:$PREFIX/bin:$PREFIX/sbin:/usr/bin:/bin:/usr/sbin:/sbin</string>
    </dict>
    <key>WorkingDirectory</key>
    <string>$PREFIX/var/cronstable-demo</string>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <!-- Do not let a config error hot loop the machine. -->
    <key>ThrottleInterval</key>
    <integer>10</integer>
    <key>StandardOutPath</key>
    <string>$LOG_DIR/cronstable-demo.out.log</string>
    <key>StandardErrorPath</key>
    <string>$LOG_DIR/cronstable-demo.err.log</string>
    <key>ProcessType</key>
    <string>Background</string>
</dict>
</plist>
EOF

# The gateway is the only public ingress to interactive actions. Its token
# remains local, and the daemon still enforces ordinary bearer scopes.
GATEWAY_PYTHON_XML="$(xml_escape "$GATEWAY_PYTHON")"
ETC_DIR_XML="$(xml_escape "$ETC_DIR")"
LOG_DIR_XML="$(xml_escape "$LOG_DIR")"
HOSTNAME_PUBLIC_XML="$(xml_escape "$HOSTNAME_PUBLIC")"
(umask 077; cat > "$AGENT_DIR/com.cronstable.demo-gateway.plist" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<!-- Generated by example/demo-instance/launchd/install.sh. -->
<plist version="1.0">
<dict>
    <key>Label</key><string>com.cronstable.demo-gateway</string>
    <key>ProgramArguments</key>
    <array>
        <string>$GATEWAY_PYTHON_XML</string>
        <string>$ETC_DIR_XML/gateway.py</string>
        <string>--config</string><string>$ETC_DIR_XML/cronstable.yaml</string>
        <string>--origin</string><string>https://$HOSTNAME_PUBLIC_XML</string>
        <string>--origin</string><string>http://127.0.0.1:8081</string>
        <string>--origin</string><string>http://localhost:8081</string>
    </array>
    <key>EnvironmentVariables</key>
    <dict>
        <key>CRONSTABLE_DEMO_VIEW_TOKEN</key><string>$VIEW_TOKEN_XML</string>
        <key>CRONSTABLE_DEMO_OPERATOR_TOKEN</key><string>$OPERATOR_TOKEN_XML</string>
    </dict>
    <key>RunAtLoad</key><true/>
    <key>KeepAlive</key><true/>
    <key>ThrottleInterval</key><integer>10</integer>
    <key>StandardOutPath</key><string>$LOG_DIR_XML/cronstable-demo-gateway.out.log</string>
    <key>StandardErrorPath</key><string>$LOG_DIR_XML/cronstable-demo-gateway.err.log</string>
    <key>ProcessType</key><string>Background</string>
</dict>
</plist>
EOF
)

cat > "$AGENT_DIR/com.cronstable.tunnel.plist" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<!-- Generated by example/demo-instance/launchd/install.sh. Edits are lost on re-run. -->
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.cronstable.tunnel</string>
    <key>ProgramArguments</key>
    <array>
        <string>$PREFIX/bin/cloudflared</string>
        <string>--config</string>
        <string>$CF_DIR/config.yml</string>
        <string>--no-autoupdate</string>
        <string>tunnel</string>
        <string>run</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>ThrottleInterval</key>
    <integer>10</integer>
    <key>StandardOutPath</key>
    <string>$LOG_DIR/cronstable-tunnel.out.log</string>
    <key>StandardErrorPath</key>
    <string>$LOG_DIR/cronstable-tunnel.err.log</string>
    <key>ProcessType</key>
    <string>Background</string>
</dict>
</plist>
EOF

# The daemon's plist carries the operator token in cleartext, so it gets the
# same treatment as the token file rather than the shell's default 0644.
# launchd reads it as this user, so 0600 is enough.
chmod 600 "$AGENT_DIR/com.cronstable.demo.plist" "$AGENT_DIR/com.cronstable.demo-gateway.plist"

# Stopping the daemon takes more than the bootout below. launchd signals the
# process it started, and for a PyInstaller build that is a bootloader whose
# child holds the listening socket. The child drains running jobs on
# SIGTERM, launchd allows the parent 20s before SIGKILL, and on the demo host
# the child has outlived both as an orphan still bound to 127.0.0.1:8080 on
# nearly every restart: the replacement logged "address already in use" and
# exited, KeepAlive relaunched it every ThrottleInterval, and the orphan kept
# answering. The gateway proxies to whatever is on 8080, so every probe at
# the end still passed and the board ran the old build indefinitely. Even
# without an orphan there is a window: `launchctl print` stops resolving the
# label seconds before the process is actually gone. So after a label
# disappears, wait for every process of the previous instance to exit and
# for its port to free, then escalate, but only against processes that are
# provably that instance. The gateway gets the same treatment on 8081.
#
# Every incarnation of the daemon shares the argv tail "-c <deployed
# config>", whichever binary started it (an earlier CRONSTABLE_BIN, the
# bootloader, its child, an instance someone started by hand), and every
# gateway runs the deployed gateway.py, so those are the matches. The
# validation runs above put more arguments after those, and jobs invoke
# subcommands, never "-c". Only dots are escaped; a Homebrew prefix carries
# no other metacharacter.
re_escape() { printf '%s' "$1" | sed 's/[.]/\\./g'; }
ETC_RE="$(re_escape "$ETC_DIR")"
DAEMON_ARGV_RE="cronstable[^ ]* -c $ETC_RE/cronstable\.yaml\$"
GATEWAY_ARGV_RE="$ETC_RE/gateway\.py --config "
procs_matching() { pgrep -f "$1" 2>/dev/null || true; }
port_holders()   { lsof -nP -iTCP@127.0.0.1:"$1" -sTCP:LISTEN -t 2>/dev/null || true; }

for label in com.cronstable.demo com.cronstable.demo-gateway com.cronstable.tunnel; do
    plutil -lint "$AGENT_DIR/$label.plist" >/dev/null || die "$label.plist failed lint"
    # plutil tolerates malformed XML that a strict parser rejects, and these
    # files are re-read on every reboot. Check them the strict way too.
    python3 -c "import xml.dom.minidom,sys; xml.dom.minidom.parse(sys.argv[1])" \
        "$AGENT_DIR/$label.plist" || die "$label.plist is not well-formed XML"

    # bootout is asynchronous. Bootstrapping while the previous job is still
    # tearing down fails with EIO, and under `set -e` that would abort here,
    # after the bootout, leaving the service down and the demo dark. Wait for
    # the label to disappear, then retry the bootstrap rather than trusting it.
    launchctl bootout "gui/$UID/$label" 2>/dev/null || true
    for _ in $(seq 1 150); do
        launchctl print "gui/$UID/$label" >/dev/null 2>&1 || break
        sleep 0.2
    done

    # Which loopback port this label must own, and the argv shape of its
    # process, for the wait below. The tunnel owns no local port.
    case "$label" in
        com.cronstable.demo)         port=8080; argv_re="$DAEMON_ARGV_RE" ;;
        com.cronstable.demo-gateway) port=8081; argv_re="$GATEWAY_ARGV_RE" ;;
        *)                           port="";   argv_re="" ;;
    esac
    if [ -n "$port" ]; then
        # The label is gone; the previous process may not be. Give its drain
        # a bounded grace on top of what launchd allowed, then escalate.
        for _ in $(seq 1 150); do
            [ -n "$(procs_matching "$argv_re")$(port_holders "$port")" ] || break
            sleep 0.2
        done
        stale="$(procs_matching "$argv_re" | xargs)"
        if [ -n "$stale" ]; then
            printf 'install.sh: previous %s still running after bootout (pid %s), terminating it\n' "$label" "$stale"
            # unquoted on purpose: one pid per word
            kill -TERM $stale 2>/dev/null || true
            for _ in $(seq 1 25); do
                [ -n "$(procs_matching "$argv_re")" ] || break
                sleep 0.2
            done
            stale="$(procs_matching "$argv_re" | xargs)"
            if [ -n "$stale" ]; then
                printf 'install.sh: pid %s survived SIGTERM, killing it\n' "$stale"
                kill -KILL $stale 2>/dev/null || true
                sleep 1
            fi
        fi
        # Whatever holds the port now is not a process this script knows,
        # so it is not this script's to kill: refuse rather than bootstrap
        # one that will lose the bind.
        holder="$(port_holders "$port" | head -1)"
        [ -z "$holder" ] || die "127.0.0.1:$port is held by pid $holder ($(ps -o args= -p "$holder" 2>/dev/null || echo unknown)); free it and re-run"
    fi

    booted=""
    for _ in 1 2 3 4 5; do
        if launchctl bootstrap "gui/$UID" "$AGENT_DIR/$label.plist" 2>/dev/null; then
            booted=yes
            break
        fi
        sleep 1
    done
    [ -n "$booted" ] || die "could not bootstrap $label; it is not running"
done

# Never report success for a gateway that did not actually come up. The probe
# sends no credential on purpose, because on this board that is the visitor's
# path (web.anonymousScopes grants view), so a 200 here proves the experience
# a stranger actually gets.
code=""
for _ in $(seq 1 30); do
    code="$(curl -fsS -o /dev/null -w '%{http_code}' -m 5 \
        http://127.0.0.1:8081/summary 2>/dev/null || true)"
    [ "$code" = "200" ] && break
    sleep 1
done
[ "$code" = "200" ] || die "gateway is not answering on 127.0.0.1:8081 (see $LOG_DIR/cronstable-demo-gateway.err.log)"

# A visitor can run the disposable restore drill, but cannot invoke private
# housekeeping. A shared cooldown can be occupied by a real visitor, so retry.
mutate=""
for _ in $(seq 1 15); do
    mutate="$(curl -sS -o /dev/null -w '%{http_code}' -m 5 \
        -X POST http://127.0.0.1:8081/jobs/restore-drill/start 2>/dev/null || true)"
    [ "$mutate" = "200" ] && break
    [ "$mutate" = "429" ] || break
    sleep 1
done
[ "$mutate" = "200" ] || die "public restore drill answered $mutate, expected 200"
private="$(curl -sS -o /dev/null -w '%{http_code}' -m 5 \
    -X POST http://127.0.0.1:8081/jobs/demo-operator/start 2>/dev/null || true)"
[ "$private" = "403" ] || die "public operator start answered $private, expected 403"

# Bypassing the gateway must still leave anonymous visitors read-only.
direct="$(curl -sS -o /dev/null -w '%{http_code}' -m 5 \
    -X POST http://127.0.0.1:8080/jobs/restore-drill/start 2>/dev/null || true)"
[ "$direct" = "403" ] || die "direct daemon mutation answered $direct, expected 403"

# Those answers are exactly what the orphan trap produced (a stale process
# answering for a replacement that could not bind, and the gateway relays
# whatever is on 8080), so prove that the process on each port descends
# from the job launchd just started. For a PyInstaller build the listener
# is the bootloader's child, hence the walk up the parent chain rather than
# a direct comparison; it ends at launchd (pid 1) when the listener is
# someone else's.
listener_is_job() {
    local port="$1" label="$2" job_pid listener ancestor
    job_pid="$(launchctl print "gui/$UID/$label" 2>/dev/null \
        | awk '$1 == "pid" && $2 == "=" { print $3; exit }' || true)"
    listener="$(port_holders "$port" | head -1)"
    ancestor="$listener"
    while [ -n "$ancestor" ] && [ "$ancestor" != "$job_pid" ]; do
        case "$ancestor" in 0|1) ancestor=""; break ;; esac
        ancestor="$(ps -o ppid= -p "$ancestor" 2>/dev/null | tr -d ' ' || true)"
    done
    [ -n "$job_pid" ] && [ "$ancestor" = "$job_pid" ] \
        || die "127.0.0.1:$port is served by pid ${listener:-none}, which is not the $label job launchd started (pid ${job_pid:-none}); a previous instance is still alive"
}
listener_is_job 8080 com.cronstable.demo
listener_is_job 8081 com.cronstable.demo-gateway

# ...and that the daemon is the build this script deployed, not one left over.
want="$("$CRONSTABLE" --version 2>/dev/null | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1 || true)"
have="$(curl -fsS -m 5 http://127.0.0.1:8080/version 2>/dev/null | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1 || true)"
[ -n "$have" ] && [ "$have" = "$want" ] \
    || die "the daemon on 127.0.0.1:8080 reports version '${have:-none}' but $CRONSTABLE is '${want:-unknown}'"

printf '\ninstalled:\n'
printf '  daemon   %s (%s)\n' "$CRONSTABLE" "$have"
printf '  config   %s\n' "$ETC_DIR/cronstable.yaml"
printf '  crontab  %s\n' "$ETC_DIR/legacy.crontab"
printf '  state    %s\n' "$STATE_DIR"
printf '  operator %s (0600, control+approve)\n' "$OPERATOR_TOKEN_FILE"
printf '  gateway  %s/gateway.py (127.0.0.1:8081)\n' "$ETC_DIR"
printf '  logs     %s/cronstable-{demo,demo-gateway,tunnel}.*.log\n' "$LOG_DIR"
printf '  tunnel   %s (%s)\n' "$TUNNEL_NAME" "$TUNNEL_ID"
printf '\nverify (no credential needed, that is the point):\n'
printf '  curl https://%s/summary\n' "$HOSTNAME_PUBLIC"
printf '  open https://%s/\n\n' "$HOSTNAME_PUBLIC"
