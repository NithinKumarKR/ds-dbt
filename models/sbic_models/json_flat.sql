{% set keys =[ 'employment']  %}
{% set address_keys = ['zip','city','street'] %}
{% set address_coo= ['latitude','longitude'] %}
{% set educations=['degrees','certifications']%}
{% set employment=['current_job','previous_jobs']%}

WITH transformed_data AS (
    SELECT
        "Name",
        array_to_string(array_agg(laterally.key), ', ') AS "nithin_keys",
        array_to_string(
            array_agg(
                "Abbreviation"::jsonb ->> laterally.key
            ),
            ', '
        ) AS "nithin_values",
        "Abbreviation" as "Abbreviation"
    FROM
        "nithin"."nivea"."weekday",
        LATERAL jsonb_object_keys("Abbreviation"::jsonb) AS laterally(key)
    GROUP BY
        "Name",
        "Abbreviation"
)

SELECT
    "Name"
        ,
    {% for key in keys %}
        jsonb_extract_path_text("Abbreviation"::jsonb, '{{ key }}') AS {{ key }}
        {% if not loop.last %},{% endif %}
    {% endfor %}
        ,    
    {% for key in employment %}
        jsonb_extract_path_text(
            jsonb_extract_path_text("Abbreviation"::jsonb, 'employment')::jsonb, '{{ key }}'
        ) AS "employment_{{ key }}"
        {% if not loop.last %},{% endif %}
    {% endfor %}
FROM
    transformed_data

{"_class": "com.lmsengine.entity.LoanEntity", "amount": 1200.0,
 "loanId": "S29032023176", "status": "ACTIVE", "tenure": 12, "deleted": false, 
 "entityId": "CLFKUPOQU000T0I4JFLDPCQ28", "noOfAdvEmi": 1, "createdDate": "2023-03-29T10:23:49.544Z", 
 "requestType": "ENTIRE_OUTSTANDING", "actualLoanId": "176", "interestRate": 36.0, 
"modifiedDate": "2023-03-29T10:23:49.544Z", "loanProductId": "1", "requestOrigin": "DEFAULT",
 "rescheduledCount": 0, "loanRequestStatus": "COMPLETED", "rescheduleHistory": [],
  "lastDueFetchedTime": "2023-05-31T00:00:00.000Z",
   "transactionDetails": 
   [{"amount": 1200.0, "txnType": "POS", "extTxnId": "6d31c223-9f9a-443d-9d15-2ae5910a6686", 
   "txnAmount": 3600.0, "txnOrigin": "POS", "subTxnType": "POS", "description": "Transaction Type",
    "transactionDate": "2023-03-27T00:00:00.000Z"}], 
    "_id_aibyte_transform": "e021bb48-79c1-45b3-b573-deaaab8ff922", 
    "advInstallmentAmount": 117.04,
     "loanAgreementFileUrl":
      "SBIC_BNPL/event/loan_agreement/SBIC_BNPL_loan_agreement_S29032023176.pdf", "stmtGeneratedInstallments": 3}