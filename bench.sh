#!/usr/bin/env bash
set -euo pipefail

wi=5
i=25

whatBranch() {
    git rev-parse --abbrev-ref HEAD
}

bench() {
    git checkout $1
    bazel run //daml-lf/scenario-interpreter:scenario-perf -- -f 1 -i $i -wi $wi
}

run() {
    bench $1 2>&1 | grep 'Average]' | xargs echo $1
}

here=$(whatBranch)
#base=main
base=dc79830239


run $here
run $here

run $base
run $base

run $here
run $here

run $base
run $base

#git checkout $here
