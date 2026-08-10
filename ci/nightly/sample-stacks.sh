#!/bin/bash
# Diagnostic: periodically dump stacks and memory of a running process.
#
# usage: sample-stacks.sh <pid> <outfile> [interval_seconds]
#
# The nightly's macOS leg stalls inside TileDB array open emitting no output, so
# wall-clock logs cannot say where the time goes. Sampling answers it directly.
#
# Platform notes learned the hard way:
#   * py-spy cannot collect native frames on macOS at all ("--native is not
#     supported on your platform") and needs root there even for Python frames.
#   * macOS's own /usr/bin/sample is the native profiler and normally works on a
#     same-user process without sudo.
#   * sudo must be given py-spy's absolute path; it is not on root's PATH.
# So: prefer `sample` on macOS, py-spy --native on Linux, and try every variant
# before giving up. Always exits 0.
set -u

PID=${1:?pid required}
OUT=${2:?outfile required}
INTERVAL=${3:-45}
# 0 = sample until the process exits; N = take at most N samples. The driver
# passes 1 for its final "catch it while it is still stuck" sample.
MAX_SAMPLES=${4:-0}

OS=$(uname)
PYSPY=$(command -v py-spy 2> /dev/null || true)

dump_native() {
  if [[ "$OS" == Darwin ]]; then
    local tmp
    tmp=$(mktemp)
    if /usr/bin/sample "$PID" 3 100 -file "$tmp" > /dev/null 2>&1 \
      || sudo -n /usr/bin/sample "$PID" 3 100 -file "$tmp" > /dev/null 2>&1; then
      cat "$tmp"
      rm -f "$tmp"
      return 0
    fi
    rm -f "$tmp"
    echo "(macOS /usr/bin/sample failed)"
    return 1
  fi
  if [[ -n "$PYSPY" ]]; then
    sudo -n "$PYSPY" dump --pid "$PID" --native 2>&1 && return 0
    "$PYSPY" dump --pid "$PID" --native 2>&1 && return 0
  fi
  echo "(no native sampler available)"
  return 1
}

dump_python() {
  if [[ -n "$PYSPY" ]]; then
    sudo -n "$PYSPY" dump --pid "$PID" 2>&1 && return 0
    "$PYSPY" dump --pid "$PID" 2>&1 && return 0
    echo "(py-spy dump failed; worker faulthandler dumps cover Python frames)"
  else
    echo "(py-spy not installed; worker faulthandler dumps cover Python frames)"
  fi
}

dump_memory() {
  # 7 GB on the macOS runners vs 16 GB on Linux, so memory pressure is a live
  # suspect; record RSS and swap activity alongside every stack sample.
  ps -o rss=,vsz= -p "$PID" 2> /dev/null \
    | awk '{print "  proc_rss_kb="$1"  proc_vsz_kb="$2}'
  if [[ "$OS" == Darwin ]]; then
    vm_stat 2> /dev/null | awk -F: '
      /Pages free|Pages active|Pageouts|Swapins|Swapouts/ {
        gsub(/[ .]/, "", $2); print "  "$1"="$2 }'
    sysctl -n vm.swapusage 2> /dev/null | sed 's/^/  swapusage: /'
  else
    awk '/^(MemFree|MemAvailable|SwapTotal|SwapFree)/ {print "  "$1"="$2" kB"}' \
      /proc/meminfo 2> /dev/null
  fi
}

{
  echo "sampler starting: pid=$PID interval=${INTERVAL}s os=$OS py-spy=${PYSPY:-none}"
  echo "date_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
} >> "$OUT" 2>&1

i=0
while kill -0 "$PID" 2> /dev/null; do
  i=$((i + 1))
  {
    echo ""
    echo "########## sample $i  $(date -u +%H:%M:%S)  pid=$PID ##########"
    echo "--- memory ---"
    dump_memory
    echo "--- python frames ---"
    dump_python
    echo "--- native frames ---"
    dump_native
  } >> "$OUT" 2>&1
  if [[ "$MAX_SAMPLES" -gt 0 && "$i" -ge "$MAX_SAMPLES" ]]; then
    echo "" >> "$OUT" 2>&1
    echo "sampler stopping: reached max_samples=$MAX_SAMPLES" >> "$OUT" 2>&1
    exit 0
  fi
  sleep "$INTERVAL"
done

echo "" >> "$OUT" 2>&1
echo "sampler done after $i sample(s); pid $PID exited" >> "$OUT" 2>&1
exit 0
