{% set key_query = ['pkey', 'awb_No', 
'kit_no', 'status', 'card_no', 'changed', 'changer',
 'created', 'creator', 'deleted', 'version', 'card_type', 'serial_no',
  'enc_card_no', 'expiry_date', 'holder_fkey', 'bus_kit_fkey', 'network_type', 
  'emboss_status', 'merchant_fkey', 'activation_date', 'delivery_vendor', 'is_prism_enabled',
   'account_provider_fkey', 'encrypted_hex_card_no'] %}  --Initialize an empty key_query list

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
        "cello"."nivea"."kit",
        LATERAL jsonb_object_keys("_airbyte_data"::jsonb) AS laterally(key)
    GROUP BY
        "_airbyte_ab_id",
        "_airbyte_emitted_at",
        "_airbyte_data"
)


SELECT    
    {% for key in key_query %}
        jsonb_extract_path_text(transformed_data._airbyte_data_::jsonb, '{{key}}') AS "{{ key }}"
        {% if not loop.last %},{% endif %}
    {% endfor %}
    ,        "_airbyte_ab_id",
        "_airbyte_emitted_at"

FROM transformed_data
