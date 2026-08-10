#!/usr/bin/env python3
"""Diagnostic: locate the macOS stall in the S3 path of test_large_export_correctness.

What earlier runs established, and why this script looks the way it does:

  * The stall is NOT proportional to region count. macOS costs a constant ~23s
    at 50, 200 and 800 regions on libtiledb 2.30.1, and both 2.31.0 legs hung on
    the very first 50-region read. So one small size is enough.
  * It is NOT network. Both runner OSes measure ~100ms reused-connection RTT to
    the bucket, and the fastest-network leg still hung.
  * It is NOT I/O volume. macOS and Linux report identical TileDB counters on
    the same libtiledb.
  * It IS array open / fragment metadata: array_open_READ is ~79x and
    sm_load_fragment_metadata ~97x slower on macOS at identical counts.

So this measures dataset open separately from the read, and runs each
measurement in a *worker subprocess* under a hard timeout while a sampler
watches it. A hang therefore yields stacks and still lets the job finish -
the previous iteration lost the 2.31.0 stats because one hang ate the job.

Driver:  diagnose-s3-read.py
Worker:  diagnose-s3-read.py --worker <n_regions> [--stats]
"""

import argparse
import faulthandler
import gzip
import os
import subprocess
import sys
import time

URI = "s3://tiledb-inc-demo-data/tiledbvcf-arrays/v4/vcf-samples-20"
SAMPLES = ["v2-DjrIAzkP", "v2-YMaDHIoW", "v2-usVwJUmo", "v2-ZVudhauk"]
ATTRS = [
    "sample_name",
    "contig",
    "pos_start",
    "pos_end",
    "query_bed_start",
    "query_bed_end",
]

SIZES = [int(x) for x in os.environ.get("DIAGNOSE_SIZES", "50").split(",")]
READ_TIMEOUT = float(os.environ.get("DIAGNOSE_READ_TIMEOUT", "300"))
SAMPLE_INTERVAL = os.environ.get("DIAGNOSE_SAMPLE_INTERVAL", "45")

HERE = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(os.path.dirname(HERE))
BED = os.path.join(
    REPO_ROOT, "libtiledbvcf/test/inputs/E001_15_coreMarks_dense_filtered.bed.gz"
)
DIAG_DIR = os.environ.get("DIAG_DIR", os.getcwd())


def log(msg):
    print(msg, flush=True)


def load_regions(path, limit=None):
    """Return BED intervals as 'contig:start-end' strings.

    Uses regions= rather than bed_file= so no bgzip/tabix is needed; the query
    ranges TileDB-VCF derives are equivalent for our purposes.
    """
    out = []
    with gzip.open(path, "rt") as fh:
        for line in fh:
            if not line.strip() or line.startswith(("#", "track", "browser")):
                continue
            f = line.split()
            if len(f) >= 3:
                out.append(f"{f[0]}:{int(f[1]) + 1}-{f[2]}")
                if limit and len(out) >= limit:
                    break
    return out


# --------------------------------------------------------------------------
# worker: one measurement, in its own process so the driver can time it out
# --------------------------------------------------------------------------
def worker(n, want_stats):
    # Guaranteed Python-level stacks every 60s without needing root, which is
    # the one sampling method available on every platform here.
    faulthandler.enable()
    faulthandler.dump_traceback_later(60, repeat=True, exit=False)

    import tiledbvcf

    log(f"[worker] pid={os.getpid()} tiledbvcf={tiledbvcf.version} n={n}")
    try:
        import tiledb

        log(f"[worker] tiledb-py={tiledb.version()} libtiledb={tiledb.libtiledb.version()}")
    except Exception as e:
        log(f"[worker] tiledb-py unavailable: {e}")

    regions = load_regions(BED, limit=n)
    log(f"[worker] regions={len(regions)}")

    # Open and read are timed separately and flushed immediately: if the log
    # shows open_seconds but no read_seconds, the stall is in the read; if it
    # shows neither, the stall is in the open.
    log("[worker] opening dataset ...")
    t0 = time.monotonic()
    ds = tiledbvcf.Dataset(URI, mode="r", stats=want_stats)
    open_s = time.monotonic() - t0
    log(f"[worker] open_seconds={open_s:.3f}")

    log("[worker] reading ...")
    t1 = time.monotonic()
    df = ds.read(attrs=ATTRS, samples=SAMPLES, regions=regions)
    read_s = time.monotonic() - t1
    log(f"[worker] read_seconds={read_s:.3f} records={len(df)}")

    if want_stats:
        log("===== TileDB stats =====")
        try:
            log(ds.reader.get_tiledb_stats())
        except Exception as e:
            log(f"(stats unavailable: {e})")
        log("===== end stats =====")

    faulthandler.cancel_dump_traceback_later()
    log(f"[worker] done total={open_s + read_s:.3f}s")
    return 0


# --------------------------------------------------------------------------
# driver: spawn each worker, sample it, enforce a timeout
# --------------------------------------------------------------------------
def run_one(n, want_stats):
    stacks = os.path.join(DIAG_DIR, f"stacks-read-{n}.txt")
    cmd = [sys.executable, os.path.abspath(__file__), "--worker", str(n)]
    if want_stats:
        cmd.append("--stats")

    log("")
    log(f"===== measurement: {n} regions (timeout {READ_TIMEOUT:.0f}s) =====")
    t0 = time.monotonic()
    # Worker inherits stdio so its progress streams straight into the job log.
    proc = subprocess.Popen(cmd)

    sampler = None
    try:
        sampler = subprocess.Popen(
            [
                "bash",
                os.path.join(HERE, "sample-stacks.sh"),
                str(proc.pid),
                stacks,
                SAMPLE_INTERVAL,
            ]
        )
    except Exception as e:
        log(f"(sampler failed to start: {e})")

    timed_out = False
    try:
        proc.wait(timeout=READ_TIMEOUT)
    except subprocess.TimeoutExpired:
        timed_out = True
        log(f"[driver] TIMEOUT after {READ_TIMEOUT:.0f}s - sampling then killing")
        # One last sample while it is still stuck, then terminate.
        try:
            subprocess.run(
                [
                    "bash",
                    os.path.join(HERE, "sample-stacks.sh"),
                    str(proc.pid),
                    stacks,
                    "1",
                    "1",  # max_samples=1: one shot, do not loop until it exits
                ],
                timeout=60,
            )
        except Exception:
            pass
        proc.kill()
        proc.wait(timeout=60)

    if sampler is not None:
        sampler.terminate()
        try:
            sampler.wait(timeout=30)
        except Exception:
            sampler.kill()

    elapsed = time.monotonic() - t0
    verdict = "TIMEOUT" if timed_out else f"rc={proc.returncode}"
    log(f"[driver] {n} regions: {verdict} after {elapsed:.1f}s; stacks -> {stacks}")

    if os.path.exists(stacks):
        log(f"----- {stacks} (tail) -----")
        try:
            with open(stacks) as fh:
                tail = fh.read()[-6000:]
            log(tail)
        except Exception as e:
            log(f"(could not read stacks: {e})")
        log("----- end stacks -----")
    return timed_out


def driver():
    log(f"driver pid={os.getpid()} sizes={SIZES} timeout={READ_TIMEOUT}")
    log(f"DIAG_DIR={DIAG_DIR}")
    if not os.path.exists(BED):
        log(f"BED not found: {BED}")
        return 0
    for n in SIZES:
        run_one(n, want_stats=True)
    log("")
    log("Interpretation: open_seconds present but no read_seconds => stall is in")
    log("the read; neither => stall is in dataset/array open.")
    return 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--worker", type=int, default=None)
    ap.add_argument("--stats", action="store_true")
    args = ap.parse_args()
    try:
        if args.worker is not None:
            sys.exit(worker(args.worker, args.stats))
        sys.exit(driver())
    except Exception as e:
        # A diagnostic must never fail the job.
        log(f"diagnostic failed: {type(e).__name__}: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(0)
