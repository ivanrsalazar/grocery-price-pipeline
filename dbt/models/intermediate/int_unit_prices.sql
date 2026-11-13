with parsed as (
    select
        *,
        lower(coalesce(size, '')) as size_normalized,
        safe_cast(regexp_extract(lower(coalesce(size, '')), r'(\d+(?:\.\d+)?)\s*fl\.?\s*oz') as numeric) as fluid_ounce_value,
        safe_cast(regexp_extract(lower(coalesce(size, '')), r'(\d+(?:\.\d+)?)\s*(?:oz|ounce)') as numeric) as ounce_value,
        safe_cast(regexp_extract(lower(coalesce(size, '')), r'(\d+(?:\.\d+)?)\s*(?:lb|pound)') as numeric) as pound_value,
        safe_cast(regexp_extract(lower(coalesce(size, '')), r'(\d+(?:\.\d+)?)\s*(?:ct\.?|count)') as numeric) as count_value,
        safe_cast(regexp_extract(lower(coalesce(size, '')), r'(\d+(?:\.\d+)?)\s*(?:gal(?:lon)?s?)') as numeric) as gallon_value,
        safe_cast(regexp_extract(lower(coalesce(size, '')), r'(\d+(?:\.\d+)?)\s*(?:qt|quart)') as numeric) as quart_value,
        safe_cast(regexp_extract(lower(coalesce(size, '')), r'(\d+(?:\.\d+)?)\s*(?:pt|pint)') as numeric) as pint_value,
        case
            when regexp_contains(lower(coalesce(size, '')), r'fl\.?\s*oz') then 'fluid_ounce'
            when regexp_contains(lower(coalesce(size, '')), r'\bgal(?:lon)?s?\b') then 'fluid_ounce'
            when regexp_contains(lower(coalesce(size, '')), r'\bqt\b|\bquart') then 'fluid_ounce'
            when regexp_contains(lower(coalesce(size, '')), r'\bpt\b|\bpint') then 'fluid_ounce'
            when regexp_contains(lower(coalesce(size, '')), r'\blb\b|\bpound') then 'ounce'
            when regexp_contains(lower(coalesce(size, '')), r'\boz\b|\bounce') then 'ounce'
            when regexp_contains(lower(coalesce(size, '')), r'\bct\b|\bcount') then 'count'
        end as normalized_unit
    from {{ ref('stg_kroger_products') }}
    where price > 0
),

normalized as (
    select
        *,
        case
            when normalized_unit = 'fluid_ounce' and regexp_contains(size_normalized, r'\b(?:half|1/2)\s*gal') then 64
            when normalized_unit = 'fluid_ounce' and gallon_value is not null then gallon_value * 128
            when normalized_unit = 'fluid_ounce' and quart_value is not null then quart_value * 32
            when normalized_unit = 'fluid_ounce' and pint_value is not null then pint_value * 16
            when normalized_unit = 'fluid_ounce' then fluid_ounce_value
            when normalized_unit = 'ounce' and pound_value is not null then pound_value * 16
            when normalized_unit = 'ounce' then ounce_value
            when normalized_unit = 'count' then count_value
        end as normalized_quantity
    from parsed
)

select
    * except (
        size_normalized, fluid_ounce_value, ounce_value, pound_value,
        count_value, gallon_value, quart_value, pint_value
    ),
    regexp_contains(size_normalized, r'\b(?:ct\.?|pk|pack)\b\s*/') as is_ambiguous_multipack,
    safe_divide(price, normalized_quantity) as unit_price
from normalized
