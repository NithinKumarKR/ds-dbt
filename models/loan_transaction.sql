
{{ config(
    materialization='table',
    schema='nivea',  -- Specify the desired schema
    alias='loan_transaction'  -- Specify the desired table name
) }}


WITH transformed_data AS (
    -- Select data from cello.nivea.loan
    SELECT
        "_airbyte_ab_id",
        "_airbyte_emitted_at",
        array_to_string(array_agg(laterally.key), ', ') AS "nithin_keys",
        array_to_string(
            array_agg(
                "_airbyte_data"::jsonb ->> laterally.key
            ),
            ', '
        ) AS "nithin_values",
        "_airbyte_data" as "_airbyte_data_"
    FROM
        "cello"."nivea"."loan",
        LATERAL jsonb_object_keys("_airbyte_data"::jsonb) AS laterally(key)
    GROUP BY
        "_airbyte_ab_id",
        "_airbyte_emitted_at",
        "_airbyte_data"
),
transformed_data1 AS (
    -- Select transaction details
    SELECT
        transformed_data._airbyte_ab_id,
        transformed_data._airbyte_emitted_at,
        jsonb_array_elements(jsonb_extract_path(transformed_data._airbyte_data_::jsonb, 'transactionDetails')) AS transaction
    FROM
        transformed_data,
        LATERAL jsonb_array_elements(jsonb_extract_path(transformed_data._airbyte_data_::jsonb, 'transactionDetails')) AS transaction
)
-- Extract desired fields from the transaction array element
SELECT
   transaction ->> 'mcc' AS mcc,
    transaction ->> 'amount' AS amount,
    transaction ->> 'txnType' AS txnType,
    transaction ->> 'extTxnId' AS extTxnId,
    transaction ->> 'txnAmount' AS txnAmount,
    transaction ->> 'txnOrigin' AS txnOrigin,
    transaction ->> 'subTxnType' AS subTxnType,
    transaction ->> 'description' AS description,
    transaction ->> 'merchantName' AS merchantName,
    transaction ->> 'transactionDate' AS transactionDate,
    transformed_data1._airbyte_ab_id,
    transformed_data1._airbyte_emitted_at
FROM
    transformed_data1
    group by 1,2,3,4,5,6,7,8,9,10,11,12

