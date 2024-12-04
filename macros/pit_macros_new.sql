{% macro ins_pit_new_macros
(
	dim_name, 
	tables_field_m_name, 
	entity_key
)  
%}

with pit_new as (
	-- Узнаем унифицированные rk и проверяем, есть ли запись с таким rk в pit таблице
    select '{{ var('run_id') }}' dataflow_id, '{{ var('execution_date') }}'::timestamp dataflow_dttm, t1.{{dim_name}}_rk, 
    '{{ var('execution_date') }}'::timestamp valid_from_dttm,
    '5999-01-01 00:00:00'::timestamp valid_to_dttm,
    {% for tables in range(0,tables_field_m_name|length) %}
        coalesce(max(t{{tables+2}}.valid_from_dttm), '1960-01-01 00:00:00'::timestamp) {{tables_field_m_name[tables][1]}} 
    {% if not loop.last %} , {% endif %} {% endfor %}
    from dbt_schema."GPR_BV_A_{{dim_name}}" t1 
    left join 
    	-- Выбор из последней выгрузки актуальных неудаленных записей
        {% for tables in range(0,tables_field_m_name|length) %}
        (
            select {{entity_key}}, valid_from_dttm, source_system_dk 
            from {{tables_field_m_name[tables][0]}}
            where 
            {% if tables_field_m_name[tables][2] == 'M'%} row_num = 1 and {% endif %}
                actual_flg = 1 
                and delete_flg = 0 
                and valid_from_dttm  = 
                (select max(valid_from_dttm) from {{tables_field_m_name[tables][0]}}) 
        ) t{{tables+2}} on t1.x_{{entity_key}} = t{{tables+2}}.{{entity_key}}
        {% if not loop.last %} left join {% endif %} 
        {% endfor %}
    where t1.{{entity_key}} not in (select {{entity_key}} from dbt_schema."GPR_BV_P_{{dim_name}}")
    group by t1.{{entity_key}}
)

--Добавляем каждому rk ополнительный период от 1960 года до даты выгрузки
select * from pit_new
union all
select '{{ var('run_id') }}' dataflow_id, '{{ var('execution_date') }}'::timestamp dataflow_dttm, {{entity_key}}, 
       '1960-01-01 00:00:00'::timestamp valid_from_dttm, valid_from_dttm - interval '1 minute' valid_to_dttm, 
        {% for tables in range(0,tables_field_m_name|length) %}
            '1960-01-01 00:00:00'::timestamp {{tables_field_m_name[tables][1]}} 
        {% if not loop.last %} , {% endif %} {% endfor %}
from pit_new

{% endmacro %}