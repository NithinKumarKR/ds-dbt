{% set key_query = [ '_class', 'amount', 'loanId', 'status', 'tenure', 'deleted', 'entityId', 'noOfAdvEmi', 
'createdDate', 'requestType', 'actualLoanId', 'interestRate', 'modifiedDate', 'loanProductId', 
'requestOrigin', 'rescheduledCount', 'loanRequestStatus', 'rescheduleHistory', 'lastDueFetchedTime',
 'transactionDetails', '_id_aibyte_transform', 'advInstallmentAmount', 
 'loanAgreementFileUrl', 'stmtGeneratedInstallments' 
 ] %} 

{% set transactionDetail = ['mcc', 'amount', 'txnType', 'extTxnId', 'txnAmount', 
'txnOrigin', 'subTxnType', 'description', 'transactionDate']
  %} 


WITH transformed_data AS (
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
)


SELECT
    -- Iterate over keys and assign aliases as needed
    {% for key in key_query %}
        jsonb_extract_path_text(transformed_data._airbyte_data_::jsonb, '{{key}}') AS "{{ key }}"
        {% if not loop.last %},{% endif %}
    {% endfor %}
    ,
    "_airbyte_ab_id",
    "_airbyte_emitted_at"
FROM transformed_data
