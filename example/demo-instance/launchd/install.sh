#!/usr/bin/env bash
#
# Install the public demo daemon and its Cloudflare Tunnel as launchd agents,
# for a macOS host with no container runtime. The container path
# (../docker-compose.yml) is the default; see ../README.md for when to prefer
# which. Safe to re-run: it re-derives the config and reloads both agents.
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
# demo-operator job. One is generated on first install and reused after that
# (see OPERATOR_TOKEN_FILE below); override to rotate:
#   CRONSTABLE_DEMO_OPERATOR_TOKEN=new-value ./install.sh
#
set -euo pipefail

TUNNEL_NAME="${1:-cronstable-demo}"
VIEW_TOKEN="${CRONSTABLE_DEMO_VIEW_TOKEN:-cronstable-public-demo-view}"
HOSTNAME_PUBLIC="${CRONSTABLE_DEMO_HOSTNAME:-demo.cronstable.com}"

die() { printf 'install.sh: %s\n' "$1" >&2; exit 1; }

[ "$(uname -s)" = "Darwin" ] || die "launchd agents are macOS only"
command -v brew        >/dev/null || die "Homebrew not found"
command -v cronstable  >/dev/null || die "cronstable not on PATH (brew install cronstable)"
command -v cloudflared >/dev/null || die "cloudflared not on PATH (brew install cloudflared)"

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
TUNNEL_LIST="$(cloudflared tunnel list --output json 2>&1)" \
  || die "cloudflared tunnel list failed (run 'cloudflared tunnel login' first?): $TUNNEL_LIST"
TUNNEL_ID="$(printf '%s' "$TUNNEL_LIST" | python3 -c "
import json, sys
name = sys.argv[1]
try:
    tunnels = json.load(sys.stdin)
except Exception:
    sys.exit(0)
for t in tunnels:
    # A live tunnel carries Go's zero timestamp here, not an empty string.
    stamp = t.get('deleted_at') or ''
    deleted = bool(stamp) and not stamp.startswith('0001-01-01')
    if t.get('name') == name and not deleted:
        print(t['id']); break
" "$TUNNEL_NAME")"
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

CRONSTABLE_DEMO_VIEW_TOKEN="$VIEW_TOKEN" \
CRONSTABLE_DEMO_OPERATOR_TOKEN="$OPERATOR_TOKEN" \
  cronstable -c "$ETC_DIR/cronstable.yaml" --validate-config >/dev/null \
  || die "derived config failed validation"

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
# the tunnel reached the daemon over the compose network; here both processes
# are launchd agents on one host, so it is plain loopback.
tunnel: $TUNNEL_ID
credentials-file: $CREDS

# Ride out a daemon restart instead of surfacing 502s to the demo.
retries: 5
grace-period: 30s

ingress:
  - hostname: $HOSTNAME_PUBLIC
    service: http://127.0.0.1:8080
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
        <string>$PREFIX/bin/cronstable</string>
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
             secret, artifact) and to python3. -->
        <key>PATH</key>
        <string>$PREFIX/bin:$PREFIX/sbin:/usr/bin:/bin:/usr/sbin:/sbin</string>
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
chmod 600 "$AGENT_DIR/com.cronstable.demo.plist"

for label in com.cronstable.demo com.cronstable.tunnel; do
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
    for _ in $(seq 1 50); do
        launchctl print "gui/$UID/$label" >/dev/null 2>&1 || break
        sleep 0.2
    done

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

# Never report success for a daemon that did not actually come up. The probe
# sends no credential on purpose, because on this board that is the visitor's
# path (web.anonymousScopes grants view), so a 200 here proves the experience
# a stranger actually gets.
code=""
for _ in $(seq 1 30); do
    code="$(curl -fsS -o /dev/null -w '%{http_code}' -m 5 \
        http://127.0.0.1:8080/summary 2>/dev/null || true)"
    [ "$code" = "200" ] && break
    sleep 1
done
[ "$code" = "200" ] || die "daemon is not answering anonymously on 127.0.0.1:8080 (see $LOG_DIR/cronstable-demo.err.log)"

# ...and that a stranger still cannot act. A 403 here is the whole security
# claim of a public board, so failing the install is right if it regresses.
mutate="$(curl -fsS -o /dev/null -w '%{http_code}' -m 5 \
    -X POST http://127.0.0.1:8080/jobs/heartbeat/start 2>/dev/null || true)"
[ "$mutate" = "403" ] || die "anonymous POST /jobs/heartbeat/start answered $mutate, expected 403"

printf '\ninstalled:\n'
printf '  config   %s\n' "$ETC_DIR/cronstable.yaml"
printf '  crontab  %s\n' "$ETC_DIR/legacy.crontab"
printf '  state    %s\n' "$STATE_DIR"
printf '  operator %s (0600, control+approve)\n' "$OPERATOR_TOKEN_FILE"
printf '  logs     %s/cronstable-{demo,tunnel}.*.log\n' "$LOG_DIR"
printf '  tunnel   %s (%s)\n' "$TUNNEL_NAME" "$TUNNEL_ID"
printf '\nverify (no credential needed, that is the point):\n'
printf '  curl https://%s/summary\n' "$HOSTNAME_PUBLIC"
printf '  open https://%s/\n\n' "$HOSTNAME_PUBLIC"
