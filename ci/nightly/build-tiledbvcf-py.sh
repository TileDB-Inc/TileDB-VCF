#!/bin/bash
set -ex

# Build (and test) tiledbvcf-py assuming source code directory is
# ./TileDB-VCF/apis/python and libtiledbvcf shared library installed in
# $GITHUB_WORKSPACE/install/

OS=$(uname)
echo "OS: $OS"
if [[ $OS == Linux ]]
then
  export LD_LIBRARY_PATH=$GITHUB_WORKSPACE/install/lib:${LD_LIBRARY_PATH-}
  echo "LD_LIBRARY_PATH: $LD_LIBRARY_PATH"
elif [[ $OS == Darwin ]]
then
  export DYLD_LIBRARY_PATH=$GITHUB_WORKSPACE/install/lib:${DYLD_LIBRARY_PATH-}
  echo "DYLD_LIBRARY_PATH: $DYLD_LIBRARY_PATH"
fi

export LIBTILEDBVCF_PATH=$GITHUB_WORKSPACE/install/

CI_DIR=$GITHUB_WORKSPACE/TileDB-VCF/ci/nightly
DIAG_DIR=${DIAG_DIR:-$GITHUB_WORKSPACE/diagnostics}
mkdir -p "$DIAG_DIR"

cd TileDB-VCF/apis/python
python -m pip install .[test]
python -c "import tiledbvcf; print(tiledbvcf.version)"

# ---------------------------------------------------------------------------
# Diagnostics for the macOS nightly stall (one pytest test burns ~2h emitting
# no output). Everything below is non-fatal and can be deleted once the cause
# is known. See ci/nightly/diagnose-s3-read.py and sample-stacks.sh.
# ---------------------------------------------------------------------------
python -m pip install pytest-timeout py-spy || echo "WARNING: diagnostic tools unavailable"

# Only pass --timeout if the plugin actually installed; otherwise pytest would
# reject the unknown option and the whole run would be lost to a typo-level
# failure rather than producing diagnostics.
TIMEOUT_ARGS=()
if python -c "import pytest_timeout" 2> /dev/null; then
  TIMEOUT_ARGS=(--timeout="${PYTEST_TIMEOUT:-1200}" --timeout-method=thread)
  echo "pytest-timeout active: ${TIMEOUT_ARGS[*]}"
else
  echo "WARNING: pytest-timeout unavailable; suite will run unbounded"
fi

# Characterise the S3 read at increasing region counts before the full suite,
# so we get numbers even if pytest is later killed by its timeout.
python "$CI_DIR/diagnose-s3-read.py" 2>&1 | tee "$DIAG_DIR/diagnose-s3-read.txt" || true

# `set -e` must not abort before the diagnostics are printed, and pytest is
# backgrounded so its pid can be sampled while it runs.
#
# NOTE: do not pipe pytest into tee here. For a backgrounded pipeline `$!` is
# the *last* process (tee), so `wait` would report tee's exit status and mask a
# pytest failure. Redirect to a file and tail it instead, which also gives the
# sampler pytest's real pid directly.
set +e
export PYTHONUNBUFFERED=1
: > "$DIAG_DIR/pytest.txt"
pytest -v --durations=0 "${TIMEOUT_ARGS[@]}" \
  > "$DIAG_DIR/pytest.txt" 2>&1 &
PYTEST_PID=$!
echo "pytest pid=$PYTEST_PID"

# Stream pytest output into the job log as it appears, so a stall is visible
# live rather than only in the artifact.
tail -f "$DIAG_DIR/pytest.txt" &
TAIL_PID=$!

bash "$CI_DIR/sample-stacks.sh" "$PYTEST_PID" "$DIAG_DIR/stacks.txt" 60 &
SAMPLER_PID=$!

wait $PYTEST_PID
PYTEST_RC=$?
kill "$TAIL_PID" "$SAMPLER_PID" 2> /dev/null
wait "$SAMPLER_PID" 2> /dev/null

echo "===== slowest tests ====="
grep -E 'seconds? call|slowest' -A 25 "$DIAG_DIR/pytest.txt" | head -40 || true
echo "===== stack samples (tail) ====="
tail -120 "$DIAG_DIR/stacks.txt" 2> /dev/null || echo "(no stack samples captured)"

echo "pytest exit code: $PYTEST_RC"
exit $PYTEST_RC
