#!/bin/bash
# Diagnostic: periodically dump stacks of a running process.
#
# usage: sample-stacks.sh <pid> <outfile> [interval_seconds]
#
# The nightly's macOS leg spends ~2 hours inside one pytest test emitting no
# output at all, so wall-clock logs cannot say where the time goes. Sampling the
# process gives the answer directly. Prefers py-spy (shows Python plus native
# frames, so TileDB's C++ worker threads are visible); falls back to macOS
# `sample` and finally to nothing. Always exits 0.
set -u

PID=${1:?pid required}
OUT=${2:?outfile required}
INTERVAL=${3:-60}

have_pyspy=0
if command -v py-spy > /dev/null 2>&1; then
  have_pyspy=1
fi

{
  echo "sampler starting: pid=$PID interval=${INTERVAL}s py-spy=$have_pyspy"
  echo "date_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
} >> "$OUT" 2>&1

i=0
while kill -0 "$PID" 2> /dev/null; do
  i=$((i + 1))
  {
    echo ""
    echo "########## sample $i  $(date -u +%H:%M:%S)  ##########"
  } >> "$OUT" 2>&1

  if [[ $have_pyspy -eq 1 ]]; then
    # --native needs elevated privileges on macOS; GitHub runners allow
    # passwordless sudo. Fall back to a Python-only dump if that fails.
    sudo -n py-spy dump --pid "$PID" --native >> "$OUT" 2>&1 \
      || py-spy dump --pid "$PID" >> "$OUT" 2>&1 \
      || echo "(py-spy dump failed)" >> "$OUT" 2>&1
  elif [[ "$(uname)" == Darwin ]]; then
    sudo -n /usr/bin/sample "$PID" 3 100 -mayDie >> "$OUT" 2>&1 \
      || /usr/bin/sample "$PID" 3 100 -mayDie >> "$OUT" 2>&1 \
      || echo "(sample failed)" >> "$OUT" 2>&1
  else
    # Linux fallback: thread names plus kernel wait channels are enough to tell
    # "blocked on network" from "spinning on CPU".
    ps -L -o tid,stat,wchan:24,pcpu,comm -p "$PID" >> "$OUT" 2>&1 \
      || echo "(ps failed)" >> "$OUT" 2>&1
  fi

  sleep "$INTERVAL"
done

echo "" >> "$OUT" 2>&1
echo "sampler done after $i sample(s); pid $PID exited" >> "$OUT" 2>&1
exit 0
