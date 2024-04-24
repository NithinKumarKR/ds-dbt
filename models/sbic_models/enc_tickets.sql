-- models/enc_tickets.sql

{{ config(
    materialized='incremental',
    unique_key = '_id',
    incremental_stategy = 'merge',
    on_schema_change= 'append_new_columns'
) }}


WITH transformed_data AS (
    SELECT 
        _airbyte_ab_id,
        _airbyte_data->>'_id' as "_id",
        _airbyte_data->>'__v' as "__v",
        _airbyte_data->>'type' as  "type"  ,
        _airbyte_data->>'formSchemaId' as  "formSchemaId" ,
        _airbyte_data->>'status' as  "status"  ,
        _airbyte_data->>'teamId' as "teamId"   ,
        _airbyte_data->>'assignee' as  "assignee"  ,
        _airbyte_data->>'userId' as  "userId"  ,
        _airbyte_data->'userInfo' as "userInfo",
        _airbyte_data->'formDoc' as "formDoc",
        _airbyte_data->'userInfo'->'email'->>'mac' as "email_mac",
        _airbyte_data->'userInfo'->'email'->>'encryptedData' as "email_encryptedData",
        _airbyte_data->'userInfo'->'mobileNumber'->>'mac' as "mobileNumber_mac",
        _airbyte_data->'userInfo'->'mobileNumber'->>'encryptedData' as "mobileNumber_encryptedData",
        _airbyte_data->>'isDeleted' as "isDeleted",
        _airbyte_data->>'isOtpVerificationPending' as  "isOtpVerificationPending"  ,
        _airbyte_data->>'createdAt' as  "createdAt"  ,
        _airbyte_data->>'updatedAt' as  "updatedAt"  ,
        _airbyte_data->>'ticketNumber' as  "ticketNumber",
        _airbyte_emitted_at

    FROM{{source('airbyte_raw','_airbyte_raw_tickets')}}

        {% if is_incremental() %}

        where TO_TIMESTAMP(SUBSTRING(cast(_airbyte_emitted_at as varchar), 1,23),'YYYY-MM-DD HH24:MI:SS.MS') > 
        (select max(TO_TIMESTAMP(SUBSTRING(cast(_airbyte_emitted_at as varchar), 1,23),'YYYY-MM-DD HH24:MI:SS.MS'))
        from {{this}} )

        {% endif %}
)


SELECT * FROM transformed_data




WITH transformed_data AS (
    SELECT
        "Name",
        array_to_string(array_agg(laterally.key), ', ') AS "nithin_keys",
    FROM
        "nithin"."nivea"."weekday",
        LATERAL jsonb_object_keys("Abbreviation"::jsonb) AS laterally(key)
    GROUP BY
        "Name"
)
SELECT
    *
FROM
    transformed_data
