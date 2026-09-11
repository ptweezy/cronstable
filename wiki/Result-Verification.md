# Result verification

Use `verify` when a command's exit status is insufficient to establish that
its output is usable. A verifier can check file contents, query a database,
or confirm that an uploaded object exists.

```yaml
jobs:
  - name: export
    command: python export.py
    schedule: "0 * * * *"
    verify:
      command: python check_export.py
      timeout: 30
```

The verifier runs after the command passes its `failsWhen` rules. It inherits
the job's environment, working directory, shell, privileges, secrets, and
job API context. `command` accepts a shell string or an argument list.
`timeout` defaults to 60 seconds and must be finite and positive. Set
`verify: null` to clear an inherited verifier.

The verifier must exit with code zero. A nonzero exit, timeout, or launch
failure fails the whole run, triggers its failure reporters, and follows its
retry policy. Retrying runs the command and verifier again. A failed or
cancelled main command skips verification. Cancellation during verification
terminates the verifier. Its timeout is separate from `executionTimeout`.

The command's exit code remains intact. History includes a separate
`verification` object with the check's outcome, exit code, timing, and
diagnostics. Diagnostic text is redacted, retains at most 64 lines per stream,
and is capped at 16384 characters per stream; oversized lines are discarded.
`output_truncated` identifies truncation of retained output.
Live output uses the `verify.stdout` and `verify.stderr` stream names.
Report templates can inspect `verification`.

Executable DAG tasks accept the same `verify` block. Downstream tasks wait
for verification to pass, and the task detail shows its result. Approval
gates reject `verify`. A [resource pool](Resource-Pools) claim remains occupied
through verification.
