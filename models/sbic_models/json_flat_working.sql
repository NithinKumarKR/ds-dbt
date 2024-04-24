WITH transformed_data AS (
    SELECT
       -- "dob_encryptedData",
		{{ json_output('dob_encryptedData', 'cipherData') }} AS "cipherData", 
		{{ json_output_2('dob_encryptedData', 'cipherData','cipherText') }}  AS "cipherText",
        {{ json_output_2('dob_encryptedData', 'cipherData','authTag') }}   "authTag",
        {{ json_output_2('dob_encryptedData', 'cipherData','iv') }}  AS "iv",
        {{ json_output('dob_encryptedData', 'version') }} AS "version"
    FROM
        "nithin"."nivea"."enc_aadhar_information"
)
SELECT
    *
FROM
    transformed_data
