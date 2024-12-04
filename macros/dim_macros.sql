{% macro ins_dim_macros
(
	sal_table,
	pit_table, 
	entity_key,
	sat_tables,
	columns_name
)  
%}

-- sal_table (source(sal_name))
-- pit_table (source(pit_name), (sat_col_name, ))
-- entity_key 'entity_key'
-- sat_tables ((source(sat_name), sat_name, (columns, ), sat_type, oid), )
-- columns_name (column_name, )

with unif_dim as
(
	select sal.{{entity_key}}, 
		source_system_dk, 
		valid_from_dttm, 
        {% for column in columns_name %}
            {{column}}
        {% if not loop.last %} , {% endif %}
        {% endfor %}
	from
	{{ sal_table }} sal
	join (
    {% for sat_t in range(0, sat_tables|length) %}
	
		select 
		{{entity_key}}, 
		source_system_dk, 
		valid_from_dttm, 
        {% for c_name in range(0, sat_tables[sat_t][2]|length) %}
           {{ sat_tables[sat_t][2][c_name] }} {{ columns_name[c_name] }}
        {% if not loop.last %} , {% endif %}
        {% endfor %}
		from {{ sat_tables[sat_t][0] }}
		where 
        {% if sat_tables[sat_t][3] == 'M' %} row_num = 1 and {% endif %}
		actual_flg = 1
        {% if not loop.last %} union all {% endif %}
		{% endfor %}
	) sat
	on sal.x_{{entity_key}} = sat.{{entity_key}}
)

select * from
(

	select 
	'{{var('run_id')}}' dataflow_id,
	'{{var('execution_date')}}'::timestamp dataflow_dttm,
	pit.{{entity_key}},
	pit.valid_from_dttm,
	pit.valid_to_dttm,
    {% for column in columns_name %}
	case 
        {% for sat_t in range(0, sat_tables|length) %}
		when unif_dim_{{sat_tables[sat_t][1]}}.valid_from_dttm != '1960-01-01' 
		then unif_dim_{{sat_tables[sat_t][1]}}.{{column}} 
        {% endfor %}
	end {{column}}
    {% if not loop.last %} , {% endif %}  
    {% endfor %}

	from 
		(
		select 
		{{entity_key}}, 
		valid_from_dttm, 
		valid_to_dttm,
        {% for p_col in pit_table[1] %} 
			{{p_col}}
		{% if not loop.last %} , {% endif %}  
        {% endfor %}

		from {{pit_table[0]}}
		--where valid_to_dttm = '5999-01-01'
		where dataflow_dttm = '{{var('execution_date')}}'::timestamp
		) pit
	
	left join 
    {% for sat_t in range(0, sat_tables|length) %} 
	
		(select * from unif_dim where source_system_dk = {{sat_tables[sat_t][4]}}) unif_dim_{{sat_tables[sat_t][1]}}
	    on pit.{{entity_key}} = unif_dim_{{sat_tables[sat_t][1]}}.{{entity_key}}
	{% if not loop.last %} left join {% endif %}  
    {% endfor %}
)

where 
{% for column in columns_name %}
    {{column}} is not null
{% if not loop.last %} or {% endif %}
{% endfor %}

{% endmacro %}