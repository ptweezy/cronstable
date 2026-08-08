"""Shared YAML config constants duplicated across test files (finding B13).

Each constant is the canonical text from the file named above it, byte for
byte.  Most were copied verbatim in the migration; the ones ``job_yaml``
can express are spelled as builder calls that render the identical string.
Consumers switch over file by file; until a file converts, its local copy
keeps working side by side with the one here.  A constant here is only a
drop-in where the local copy is byte-identical; near-miss variants (noted
below) stay in their files.

No imports: this module is pure data plus one string builder, so any test
file (or tests/conftest.py) can import it without dragging anything in.
"""


def job_yaml(name="a", command=None, schedule="* * * * *", extra=""):
    """The 4-line one-job YAML that appears ~35x (32 in test_fingerprint.py).

    Renders the canonical triple-quoted shape byte-for-byte, leading newline
    included:

        \\njobs:\\n  - name: a\\n    command: echo a\\n
        schedule: "* * * * *"\\n

    ``command`` defaults to ``echo <name>``.  ``extra`` is appended verbatim
    after the schedule line (already-indented job fields, or a whole second
    job entry).
    """
    if command is None:
        command = "echo " + name
    return (
        "\njobs:\n"
        "  - name: " + name + "\n"
        "    command: " + command + "\n"
        '    schedule: "' + schedule + '"\n' + extra
    )


# canonical: tests/test_state.py (byte-identical in
# test_cron_state_hardening.py and test_state_lifecycle_hardening.py;
# test_state_job_api.py's variant prepends a "state:" format template and
# stays local)
_ONE_JOB = (
    "jobs:\n  - name: j\n    command: 'true'\n    schedule: '* * * * *'\n"
)

# canonical: tests/test_state.py (same text split across more source lines in
# test_cron_state_hardening.py; test_state_scheduler_durability.py's _DEP_JOB
# is a different job entirely, name d / command ls, and stays local)
_DEP_JOB = (
    "jobs:\n  - name: j\n    command: 'true'\n    schedule: '* * * * *'\n"
    "    onlyIfLastSucceeded: true\n"
)

# the one home (test_state_fleet_ha.py imports it)
_PLAIN_JOB = job_yaml("j", "ls", "0 0 * * *")

# the one home (test_state_fleet_ha.py imports it).  Near-miss variants stay
# local: test_state_scheduler_durability.py's uses maximumRetries: 3, and
# test_ui_endpoints.py's is a different job (flaky / 'false' / "@reboot"
# with initialDelay: 8).
_RETRY_JOB = job_yaml(
    "j",
    "ls",
    "0 0 * * *",
    extra=(
        "    onFailure:\n"
        "      retry:\n"
        "        maximumRetries: 5\n"
        "        initialDelay: 1\n"
        "        maximumDelay: 60\n"
        "        backoffMultiplier: 2\n"
    ),
)

# the one home (test_cron_web.py imports it; test_web_scopes.py imports it
# as _DISABLED_JOB)
DISABLED_JOB = job_yaml("test", "echo hi", extra="    enabled: false\n")

# the one home; every consumer imports it from here
_PAUSABLE_JOB = job_yaml("p", "echo hi")

# canonical: tests/test_config_backends.py
_ETCD = (
    "cluster:\n"
    "  backend: etcd\n"
    "  nodeName: node-a\n"
    "  etcd:\n"
    "    endpoints:\n"
    "      - http://127.0.0.1:2379\n"
)

# canonical: tests/test_config_backends.py (byte-identical in
# test_state_dag.py)
_STATE = "state:\n  path: /tmp/x\n"

# canonical: tests/test_cron_lifecycle.py, where the former test_cron.py's
# six copies now live (one variant there appends a third peer or an
# observability block onto this base).
_TLS_CLUSTER_YAML = (
    "jobs:\n  - name: a\n    command: echo a\n"
    '    schedule: "* * * * *"\n'
    "cluster:\n"
    '  listen: "127.0.0.1:18443"\n'
    "  tls:\n"
    "    ca: /nonexistent/ca.pem\n"
    "    cert: /nonexistent/cert.pem\n"
    "    key: /nonexistent/key.pem\n"
    "  peers:\n"
    "    - host: b:8443\n"
    "    - host: c:8443\n"
    "  electLeader: true\n"
)
