select
    basket_id,
    item_name,
    search_term,
    cast(quantity as numeric) as quantity,
    pricing_method,
    nullif(cast(upc as string), '') as upc,
    cast(item_order as int64) as item_order,
    cast(standard_quantity as numeric) as standard_quantity,
    nullif(cast(standard_unit as string), '') as standard_unit,
    item_category,
    concat(basket_id, '|', cast(item_order as string)) as basket_item_key
from {{ ref('basket_items') }}
