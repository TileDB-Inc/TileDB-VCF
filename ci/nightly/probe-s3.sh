#!/bin/bash
# Diagnostic: record the runner's hardware and its raw latency to the S3 bucket
# used by test_large_export_correctness.
#
# Establishes the per-request baseline that any read-time comparison has to be
# interpreted against. Deliberately non-fatal: this must never fail a job.
set -u

BUCKET=tiledb-inc-demo-data
KEY=tiledbvcf-arrays/v4/vcf-samples-20/__tiledb_group.tdb
URL="https://$BUCKET.s3.us-east-1.amazonaws.com/$KEY"

echo "===== runner ====="
echo "uname:      $(uname -a)"
if [[ "$(uname)" == Darwin ]]; then
  echo "cpus:       $(sysctl -n hw.ncpu) (physical $(sysctl -n hw.physicalcpu))"
  echo "memsize:    $(sysctl -n hw.memsize)"
  echo "cpubrand:   $(sysctl -n machdep.cpu.brand_string 2>/dev/null || echo n/a)"
else
  echo "cpus:       $(nproc)"
  echo "memtotal:   $(awk '/MemTotal/{print $2}' /proc/meminfo) kB"
fi

echo "===== AWS-related environment (names only, no values) ====="
env | grep -oE '^(AWS|TILEDB)[A-Z_0-9]*' | sort || echo "(none)"

echo "===== DNS ====="
python3 - <<'PY' 2>&1 || true
import socket, time
host = "tiledb-inc-demo-data.s3.us-east-1.amazonaws.com"
t0 = time.monotonic()
try:
    infos = socket.getaddrinfo(host, 443, proto=socket.IPPROTO_TCP)
    dt = (time.monotonic() - t0) * 1000
    addrs = sorted({i[4][0] for i in infos})
    print(f"resolve {host}: {dt:.1f} ms -> {len(addrs)} addr(s): {addrs[:4]}")
except Exception as e:
    print(f"resolve failed: {e}")
PY

echo "===== 20 sequential GETs (fresh connection each) ====="
for i in $(seq 1 20); do
  curl -s -o /dev/null \
    -w "  get $i: dns=%{time_namelookup} connect=%{time_connect} tls=%{time_appconnect} ttfb=%{time_starttransfer} total=%{time_total} status=%{http_code}\n" \
    "$URL" || echo "  get $i: FAILED"
done

echo "===== 20 GETs on a reused connection ====="
# TileDB pools connections, so the reused-connection number is the one that
# actually multiplies across a read.
seq 20 | sed "s|.*|url = \"$URL\"|" > /tmp/s3-urls.txt
curl -s -o /dev/null --keepalive-time 60 -K /tmp/s3-urls.txt \
  -w "  ttfb=%{time_starttransfer} total=%{time_total} reused=%{num_connects}\n" \
  || echo "  keepalive probe FAILED"

echo "===== done ====="
exit 0
