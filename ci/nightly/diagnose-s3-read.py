#!/usr/bin/env python3
"""Diagnostic: characterise the S3 read that test_large_export_correctness does.

Reads the same array, samples and BED intervals, but at increasing region
counts under a wall-clock budget. Prints per-size timing and TileDB's own
counters, flushing as it goes so partial results survive a killed step.

Answers two things the CI logs cannot:
  1. Is the cost linear in region count (per-request overhead) or superlinear
     (algorithmic)?
  2. Do the TileDB S3 counters differ between matrix legs, or only the timings?

Deliberately non-fatal - it must never fail a job.
"""

import gzip
import os
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
SIZES = [50, 200, 800, 3200, 12800, 31520]
BUDGET_SECONDS = float(os.environ.get("DIAGNOSE_BUDGET_SECONDS", "600"))
STATS_AT = int(os.environ.get("DIAGNOSE_STATS_AT", "800"))

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
BED = os.path.join(
    REPO_ROOT,
    "libtiledbvcf/test/inputs/E001_15_coreMarks_dense_filtered.bed.gz",
)


def log(msg):
    print(msg, flush=True)


def load_regions(path):
    """Return BED intervals as 'contig:start-end' strings.

    Uses regions= rather than bed_file= so this needs no bgzip/tabix; the query
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
    return out


def main():
    import tiledbvcf

    log(f"tiledbvcf {tiledbvcf.version}")
    try:
        import tiledb

        log(f"tiledb-py {tiledb.version()}  libtiledb {tiledb.libtiledb.version()}")
    except Exception as e:  # pragma: no cover - diagnostic only
        log(f"tiledb-py unavailable: {e}")

    if not os.path.exists(BED):
        log(f"BED not found: {BED}")
        return 0
    regions = load_regions(BED)
    log(f"BED intervals available: {len(regions)}")
    log("")
    log(f"{'regions':>8} {'seconds':>10} {'records':>12} {'ms/region':>11}")
    log("-" * 45)

    spent = 0.0
    rate = None  # seconds per region, measured from the previous size
    for n in SIZES:
        if n > len(regions):
            continue
        remaining = BUDGET_SECONDS - spent
        if remaining <= 0:
            log(f"(stopping: {spent:.0f}s spent, {BUDGET_SECONDS:.0f}s budget)")
            break
        # Project this size from the previous size's rate and skip it if it
        # would not fit. On the stalling leg a single large size can otherwise
        # consume the whole job, leaving no time for pytest.
        if rate is not None and n * rate > remaining:
            log(
                f"(skipping {n}: projected {n * rate:.0f}s exceeds "
                f"{remaining:.0f}s remaining)"
            )
            continue

        want_stats = n == STATS_AT
        try:
            ds = tiledbvcf.Dataset(URI, mode="r", stats=want_stats)
            t0 = time.monotonic()
            df = ds.read(attrs=ATTRS, samples=SAMPLES, regions=regions[:n])
            dt = time.monotonic() - t0
        except Exception as e:
            log(f"{n:>8} {'ERROR':>10}  {type(e).__name__}: {e}")
            break

        spent += dt
        rate = dt / n
        log(f"{n:>8} {dt:>10.2f} {len(df):>12,} {1000.0 * rate:>11.2f}")

        if want_stats:
            log("")
            log(f"===== TileDB stats at {n} regions =====")
            try:
                log(ds.reader.get_tiledb_stats())
            except Exception as e:
                log(f"(stats unavailable: {e})")
            log("===== end stats =====")
            log("")

    log("")
    log("Read the ms/region column across matrix legs: a flat but large value")
    log("means per-request overhead; a rising value means algorithmic cost.")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:  # never fail the job on a diagnostic
        log(f"diagnostic failed: {type(e).__name__}: {e}")
        sys.exit(0)
