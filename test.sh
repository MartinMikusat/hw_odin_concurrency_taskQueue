#!/bin/sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
odin test "$ROOT" -define:ODIN_TEST_THREADS=1
