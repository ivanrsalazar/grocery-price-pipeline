select
    cast(location_id as string) as location_id,
    city,
    state,
    cast(market_order as int64) as market_order,
    cast(active as bool) as active
from {{ ref('locations') }}
