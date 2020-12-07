import time


def _wait_and_get_responses(job_ids, batch_client):
    """Waits on all given jobs to terminate; returns the job descriptions."""
    job_responses = {}
    for job in job_ids:
        terminated = False
        while not terminated:
            response = batch_client.describe_jobs(jobs=[job])
            job_response = response["jobs"][0]
            status = job_response["status"]
            if status in {"FAILED", "SUCCEEDED"}:
                job_responses[job] = job_response
                terminated = True
            else:
                time.sleep(1)
    return job_responses


def _get_job_stats(job_responses):
    """Computes and returns some statistics about the given jobs."""
    usage_interval_per_instance = {}
    stats = {
        "num_jobs": 0,
        "num_succeeded": 0,
        "num_retried": 0,
        "num_instances": 0,
        "max_duration_sec": 0,
        "total_duration_hr": 0,
    }

    for job_id, resp in job_responses.items():
        status = resp["status"]
        attempts = resp["attempts"]
        job_sec = 0
        for attempt in attempts:
            started_at = int(attempt["startedAt"])
            stopped_at = int(attempt["stoppedAt"])
            instance = attempt["container"]["containerInstanceArn"]
            job_sec = (stopped_at - started_at) / 1e3
            if instance not in usage_interval_per_instance:
                usage_interval_per_instance[instance] = (started_at, stopped_at)
            else:
                curr = usage_interval_per_instance[instance]
                usage_interval_per_instance[instance] = (
                    min(curr[0], started_at),
                    max(curr[1], stopped_at),
                )
        stats["max_duration_sec"] = max(stats["max_duration_sec"], job_sec)
        stats["num_retried"] += 1 if len(attempts) > 1 else 0
        stats["num_succeeded"] += 1 if status == "SUCCEEDED" else 0
        stats["num_jobs"] += 1

    stats["num_instances"] = len(usage_interval_per_instance.keys())

    # Compute aggregate stats.
    for instance, interval in usage_interval_per_instance.items():
        # The interval is a pair (earliest-job-start, latest-job-stop).
        duration_sec = (interval[1] - interval[0]) / 1e3
        duration_hr = duration_sec / 3600.0
        stats["total_duration_hr"] += duration_hr

    return stats


def print_stats_report(stats, instance_price_per_hr=1.00):
    """Prints a report from the given stats."""
    print("Finished waiting on {} jobs:".format(stats["num_jobs"]))
    print("- {} / {} jobs succeeded".format(stats["num_succeeded"], stats["num_jobs"]))
    print("- {} jobs had more than 1 attempt.".format(stats["num_retried"]))
    print("- {} instances used.".format(stats["num_instances"]))
    print(
        "- Longest job took {:.3f} seconds (includes retries).".format(
            stats["max_duration_sec"]
        )
    )

    # Note on the cost estimate. For each job attempt, we note the instance ID
    # it executed on. For every instance, we compute a "usage interval" of
    # (earliest-job-start, latest-job-stop). This interval is used to compute
    # a duration per instance, which is aggregated into a duration for the total
    # compute used.
    print(
        "- Total cost estimate ${:.2f} ({:.2f} hours @ ${:.4f} / hr)".format(
            stats["total_duration_hr"] * instance_price_per_hr,
            stats["total_duration_hr"],
            instance_price_per_hr,
        )
    )


def wait_all(job_ids, batch_client, verbose=True, job_name=None):
    """Waits for all given jobs to terminate; returns stats about the jobs."""
    if verbose:
        msg = "Waiting on {} jobs to terminate...".format(len(job_ids))
        if job_name is not None:
            msg += " [{}]".format(job_name)
        print(msg)

    # Wait for all jobs to finish.
    job_responses = _wait_and_get_responses(job_ids, batch_client)

    # Process response data.
    stats = _get_job_stats(job_responses)

    # Check for warning.
    all_succeeded = stats["num_succeeded"] == stats["num_jobs"]
    if verbose and not all_succeeded:
        print("\nWARNING: one or more jobs failed.")

    return stats
