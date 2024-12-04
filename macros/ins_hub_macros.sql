{% macro ins_hub_macros(hub_table, source_table,  key_join, args) %}

SELECT 
    '{{ var('run_id') }}' dataflow_id,
    '{{ var('execution_date') }}'::timestamp dataflow_dttm,
    oid source_system_dk,
    md5( {% for i in args %} {{ i }} || '#' || {% endfor %}  oid) {{ key_join }},
    {% for i in args %} {{ i }} || '#' || {% endfor %}  oid hub_key
FROM 
    {{ ref( source_table  ) }}  ods
	LEFT JOIN 
	{{hub_table}} h_cl
	ON md5( {% for i in args %} ods.{{ i }} || '#' || {% endfor %}  ods.oid) = h_cl.{{ key_join }}
WHERE h_cl.{{ key_join }} IS NULL

{% endmacro %}
