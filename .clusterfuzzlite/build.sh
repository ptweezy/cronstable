#!/bin/bash -eu
# Builds each fuzz/fuzz_<name>.py into $OUT with its seeds
# (fuzz/seeds/<name>/) and dictionary (fuzz/<name>.dict); see
# fuzz/README.md.

# A fixed version: setuptools_scm may not read the checkout in here.
SETUPTOOLS_SCM_PRETEND_VERSION=0.0.0 pip3 install .

for fuzzer in fuzz/fuzz_*.py; do
  target=$(basename -s .py "$fuzzer")
  name=${target#fuzz_}
  # zoneinfo falls back to the tzdata package where the runner has no
  # zoneinfo files.
  compile_python_fuzzer "$fuzzer" \
    --collect-submodules cronstable --collect-all tzdata
  if [ -d "fuzz/seeds/$name" ]; then
    zip -jq "$OUT/${target}_seed_corpus.zip" "fuzz/seeds/$name"/*
  fi
  if [ -f "fuzz/$name.dict" ]; then
    cp "fuzz/$name.dict" "$OUT/$target.dict"
  fi
done

python3 fuzz/config_dict.py > "$OUT/fuzz_config.dict"

# Task Scheduler exports are UTF-16; the checked-in seeds are UTF-8 text.
utf16=$(mktemp -d)
for seed in fuzz/seeds/taskxml/*.xml; do
  iconv -f UTF-8 -t UTF-16 "$seed" > "$utf16/utf16-$(basename "$seed")"
done
zip -jq "$OUT/fuzz_taskxml_seed_corpus.zip" "$utf16"/*
