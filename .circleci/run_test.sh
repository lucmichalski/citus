#!/bin/bash

set -euxo pipefail
IFS=$'\n\t'

status=0

make -C src/test/regress "${@}" || status=$?
diffs="src/test/regress/regression.diffs"

if test -f "${diffs}"; then cat "${diffs}"; fi

exit $status
