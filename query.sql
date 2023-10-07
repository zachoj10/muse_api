select count(
        distinct case
            when list_contains(locations, 'New York City Metro Area') THEN job_id
        END
    ) AS nycma_jobs,
    count(
        distinct case
            when list_contains(locations, 'New York City Metro Area')
            or is_remote_eligible THEN job_id
        END
    ) AS nycma_remote_jobs,
    count(
        distinct case
            when list_contains(locations, 'New York City Metro Area')
            or is_remote_eligible
            or contains(locations, ', NY')
            or contains(locations, ', NJ')
            or contains(locations, ', CT') THEN job_id
        END
    ) AS tristate_remote_jobs
from jobs
where publication_at >= '2023-07-01'
    and publication_at < '2023-08-01';