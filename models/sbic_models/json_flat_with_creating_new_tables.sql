{% set keys =['age', 'city', 'name', 'hobbies', 'education', 'employment']  %}
{% set address_keys = ['zip','city','street'] %}
{% set address_coo= ['latitude','longitude'] %}
{% set educations=['degrees','certifications']%}

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
    {% for key in address_keys %}
        jsonb_extract_path_text(
            jsonb_extract_path_text("Abbreviation"::jsonb, 'address')::jsonb, '{{ key }}'
        ) AS "address_{{ key }}"
        {% if not loop.last %},{% endif %}
    {% endfor %}
    ,    
    {% for key in address_coo %}
        jsonb_extract_path_text(
        jsonb_extract_path_text(
        jsonb_extract_path_text(
            "Abbreviation"::jsonb, 
            'address')::jsonb,
            'coordinates')::jsonb,'{{key}}')
         AS "address_coordinates_{{ key }}"

        {% if not loop.last %},{% endif %}
    {% endfor %}

    ,
    {% for key in keys %}
        jsonb_extract_path_text("Abbreviation"::jsonb, '{{ key }}') AS {{ key }}
        {% if not loop.last %},{% endif %}
    {% endfor %}



        ,
    {% for key in educations %}
        jsonb_extract_path_text(
            jsonb_extract_path_text("Abbreviation"::jsonb, 'education')::jsonb, '{{ key }}'
        ) AS "educations_{{ key }}"
        {% if not loop.last %},{% endif %}
    {% endfor %}
    
FROM
    transformed_data
