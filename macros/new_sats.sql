

{% macro select_modif_sals(table_name, source_table, pks_source_table, entity_key, args_source_tab=( ), type_sat = '') %}

select 
    {% if type_sat == 'E' %}
        '{{ var('run_id') }}' dataflow_id,
        '{{ var('execution_date') }}'::timestamp dataflow_dttm,
        md5( {% for i in args_source_tab %} {{ i }}{% if not loop.last %} || '#' || {% endif %}{% endfor %} ) hashdiff_key,
        md5( {% for i in pks_source_table %} {{ i }} || '#' || {% endfor %}  oid) {{entity_key}},
        0 delete_flg,
        1 actual_flg, 
        oid source_system_dk,
        '{{ var('execution_date') }}'::timestamp valid_from_dttm
    {% else %}
        '{{ var('run_id') }}' dataflow_id,
        '{{ var('execution_date') }}'::timestamp dataflow_dttm,
        oid source_system_dk, 
        md5( {% for i in pks_source_table %} {{ i }} || '#' || {% endfor %}  oid) {{entity_key}}, 
        '{{ var('execution_date') }}'::timestamp valid_from_dttm, 
        md5( {% for i in args_source_tab %} {{ i }}{% if not loop.last %} || '#' || {% endif %}{% endfor %} ) hashdiff_key,
        1 actual_flg,
        0 delete_flg,
        {% for i in args_source_tab %}
            {{ i }}
            {% if not loop.last %}, {% endif %}
        {% endfor %}
    {% endif %}
from 
	{{ref( source_table )}}
where md5( {% for i in pks_source_table %} {{ i }} || '#' || {% endfor %}  oid) in
(
select 
	{{ entity_key }}
from
	(
	select 
		md5( {% for i in pks_source_table %} {{ i }} || '#' || {% endfor %}  oid) {{entity_key}}, 
		md5( {% for i in args_source_tab %} {{ i }}{% if not loop.last %} || '#' ||{% endif %}{% endfor %} ) hashdiff_key 
	from 
	    {{ref( source_table )}}
	except
	select 
		{{ entity_key }}, 
		hashdiff_key   
	from 
	 {{ table_name }} where actual_flg = 1 and delete_flg = 0)
		)

{% endmacro %}