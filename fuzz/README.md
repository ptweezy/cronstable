# Fuzz targets

This directory holds coverage-guided fuzz targets for the parsers that read
input from users, jobs, and cluster peers. Each `fuzz_<name>.py` is an
[Atheris](https://github.com/google/atheris) harness that passes generated
input to one parser and checks that parser's contract. The fuzzer reports
any input that raises an unexpected exception or fails a check.

| Target | Input | Checks |
|---|---|---|
| `fuzz_cronexpr.py` | A schedule expression | `CronTab` raises only `ValueError`. An accepted schedule reparses to an equal one, and its next and previous fires match it. `describe_cron` and `lint_schedule` never raise. |
| `fuzz_crontab.py` | A classic crontab | `parse_crontab` raises only `CrontabError`, and loading the text as configuration raises only `ConfigError`. |
| `fuzz_config.py` | A YAML configuration file | `parse_config_string` raises only `ConfigError`. Every file the configuration names reads as missing, so a run never touches the disk. |
| `fuzz_taskxml.py` | A Task Scheduler export | The import raises only `TaskXmlError`. The YAML it writes loads with the same job names, and converting the file again gives the same bytes. |
| `fuzz_redact.py` | Captured job output | Redaction never raises and keeps the line count, and a second `redact_secrets` pass over a line changes nothing. |
| `fuzz_peer.py` | A JSON document from a peer, the state backend, or a pairing request | The validators never raise. Gossip they keep stays within its caps and encodes without `NaN` or `Infinity`, and run records survive a round trip. |

## How CI runs the targets

[ClusterFuzzLite](https://google.github.io/clusterfuzzlite/) builds the
targets from `.clusterfuzzlite/`, and the `fuzz` workflow runs them:

- On a pull request, the targets share five minutes of fuzzing.
- Every day, a batch run fuzzes `main` for an hour, saves each target's
  corpus as an Actions artifact for the next run, and then prunes the corpus.

A crash fails the run and uploads the input that caused it as an artifact.

## Seeds and dictionaries

`.clusterfuzzlite/build.sh` packs `seeds/<name>/` into the seed corpus of
`fuzz_<name>.py` and installs `<name>.dict` as its libFuzzer dictionary.
`config_dict.py` writes the configuration dictionary at build time from every
key in the configuration schema. Task Scheduler writes its exports in UTF-16,
so the build adds a UTF-16 copy of each Task Scheduler seed, which the
repository keeps as UTF-8 text.

## Running a target locally

Build the targets the way CI does, with Docker. Keep the output out of the
checkout, because every image build sends the repository root as its
context:

```sh
out="${TMPDIR:-/tmp}/cronstable-fuzz"
docker build -t cronstable-fuzz -f .clusterfuzzlite/Dockerfile .
docker run --rm -e FUZZING_LANGUAGE=python -v "$out:/out" cronstable-fuzz compile
```

Run one target for a minute:

```sh
docker run --rm -v "$out:/out" cronstable-fuzz /out/fuzz_cronexpr -max_total_time=60
```

To replay one input, such as a crash artifact from CI, replace `INPUT_FILE`
with its path:

```sh
docker run --rm -v "$out:/out" -v "INPUT_FILE:/input" cronstable-fuzz /out/fuzz_cronexpr /input
```

When a target finds a bug, fix the code and add the input as a regression
test in `tests/`.
