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
canceled main command skips verification. The verifier's timeout is
separate from `executionTimeout`.

The timeout covers both the verifier process and its output streams. If the
verifier exits but a child process still holds its stdout or stderr open at
`timeout`, the run fails as a timeout. On timeout, the daemon cancels the
verifier using the job's
[cancellation sequence](Concurrency-and-Timeouts#cancellation-and-killtimeout),
waits up to 30 seconds for each of the verifier's two output streams to
close, and records the run with verification `exit_code: -100` and
`fail_reason: "verification failed: command exited with code -100"`.

A cancel during verification applies the same sequence to the verifier, and
the run has ended when the sequence finishes.
[Cancellation and killTimeout](Concurrency-and-Timeouts#cancellation-and-killtimeout)
also covers a cancel that arrives before the verifier has a process, and the
bound on output that a surviving process holds open.

The command's exit code remains intact. History includes a separate
`verification` object with the check's outcome, exit code, timing, and
diagnostics. Diagnostic text is redacted, retains at most 64 lines per stream,
and is capped at 16384 characters per stream; oversized lines are discarded.
`output_truncated` identifies truncation of retained output.
For a job with [`resourceLimits`](Resource-Limits), the verifier runs under
the same caps in a cgroup or Job Object of its own, and `limits` holds its
record (see
[verify checks and DAG tasks](Resource-Limits#verify-checks-and-dag-tasks)).
Live output uses the `verify.stdout` and `verify.stderr` stream names.
Report templates can inspect `verification`.

Executable DAG tasks accept the same `verify` block. Downstream tasks wait
for verification to pass, and the task detail shows its result. Approval
gates reject `verify`. A [resource pool](Resource-Pools) claim remains occupied
through verification.
