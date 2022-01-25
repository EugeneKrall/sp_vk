{{ config(
    cluster_by = "_airbyte_emitted_at",
    partition_by = {"field": "_airbyte_emitted_at", "data_type": "timestamp", "granularity": "day"},
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_vk",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('messages_ab2') }}
select
    {{ dbt_utils.surrogate_key([
        '_id',
        'type',
        'price',
        'bot_id',
        'status',
         boolean_to_string('is_paid'),
        'user_id',
        'campaign_id',
        'currency',
        'direction',
        'contact_id',
        'created_at',
        'updated_at',
        'price_country_code',
    ]) }} as _airbyte_messages_hashid,
    tmp.*
from {{ ref('messages_ab2') }} tmp
-- messages
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at') }}

