select 
    count(distinct job_id)
from jobs 
where 
list_contains(locations, 'New York City Metro Area') 
-- and publication_date >= '2023-07-01'
-- and publication_date < '2023-08-01'
;