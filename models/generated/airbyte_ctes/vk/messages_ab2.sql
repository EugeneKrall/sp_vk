{{ config(
    cluster_by = "_airbyte_emitted_at",
    partition_by = {"field": "_airbyte_emitted_at", "data_type": "timestamp", "granularity": "day"},
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_vk",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('messages_ab1') }}
select
    cast(_id as {{ dbt_utils.type_string() }}) as _id,
    cast(type as {{ dbt_utils.type_float() }}) as type,
    cast(bot_id as {{ dbt_utils.type_string() }}) as bot_id,
    cast(message as {{ dbt_utils.type_string() }}) as message,
    cast(user_id as {{ dbt_utils.type_float() }}) as user_id,
    cast(version as {{ dbt_utils.type_float() }}) as version,
    cast(direction as {{ dbt_utils.type_float() }}) as direction,
    cast(contact_id as {{ dbt_utils.type_string() }}) as contact_id,
    cast(created_at as {{ dbt_utils.type_string() }}) as created_at,
    cast(updated_at as {{ dbt_utils.type_string() }}) as updated_at,
    cast(message_text as {{ dbt_utils.type_string() }}) as message_text,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('messages_ab1') }}
-- messages
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at') }}

