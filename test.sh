#!/bin/sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
hw-odin test "$ROOT" -define:ODIN_TEST_THREADS=1
