{{
    config(
        materialized='incremental',
        incremnetal_strategy='delete+insert',
        transient=false,
        unique_key= "date_partition" ,
        alias='past_launch',
        schema=var('externalSchema'),
        database=var('externalDatabase')
    )
}}

with raw_source as (
    select * from {{ source('intake_spacex', 'intake_past_launch') }}
    and  date_partition = to_date( {{ (var("intakeDate")|string) [:6] }}::varchar, 'YYYYMM')
)

select
    -- Core Space X Payload Extractions
    cast(json_extract_string(payload, '$.id') as varchar) as launch_id,
    cast(json_extract_string(payload, '$.pad') as varchar) as launch_pad,
    cast(json_extract_string(payload, '$.name') as varchar) as launch_name,
    cast(json_extract_string(payload, '$.flight_number') as integer) as flight_number,
    cast(json_extract_string(payload, '$.date_utc') as timestamp) as launch_at_utc,
    cast(json_extract_string(payload, '$.success') as boolean) as is_successful

    raw_content,
    raw_content_length,
    forms,
    headers,
    images,
    input_fields,
    links,
    parsed_html,
    'past' as launch_stream,
    date_partition
from raw_source