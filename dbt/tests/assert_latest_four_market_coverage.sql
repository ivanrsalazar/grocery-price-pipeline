with latest_date as (
    select max(snapshot_date) as snapshot_date
    from {{ ref('stg_kroger_products') }}
),

coverage as (
    select count(distinct location_id) as actual_markets
    from {{ ref('stg_kroger_products') }}
    join latest_date using (snapshot_date)
)

select *
from coverage
where actual_markets != (select countif(active) from {{ ref('stg_locations') }})
