{{ config(
    cluster_by = "_airbyte_emitted_at",
    partition_by = {"field": "_airbyte_emitted_at", "data_type": "timestamp", "granularity": "day"},
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_vk",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('vk', '_airbyte_raw_messages') }}
select
    {{ json_extract_scalar('_airbyte_data', ['_id'], ['_id']) }} as _id,
    {{ json_extract_scalar('_airbyte_data', ['type'], ['type']) }} as type,
    null AS price,
    {{ json_extract_scalar('_airbyte_data', ['bot_id'], ['bot_id']) }} as bot_id,
    null AS status,
    null AS is_paid,
    {{ json_extract_scalar('_airbyte_data', ['user_id'], ['user_id']) }} as user_id,
    null AS campaign_id,
    null AS currency,
    {{ json_extract_scalar('_airbyte_data', ['direction'], ['direction']) }} as direction,
    {{ json_extract_scalar('_airbyte_data', ['contact_id'], ['contact_id']) }} as contact_id,  
    {{ json_extract_scalar('_airbyte_data', ['created_at'], ['created_at']) }} as created_at,
    {{ json_extract_scalar('_airbyte_data', ['updated_at'], ['updated_at']) }} as updated_at,
    null AS price_country_code,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ source('vk', '_airbyte_raw_messages') }} as table_alias
-- messages
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at') }}

