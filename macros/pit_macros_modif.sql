{% macro ins_pit_modif_macros
(
	dim_name, 
	tables_field_m_name, 
	entity_key
)  
%}

select * from
(select 
    '{{ var('run_id') }}' dataflow_id, 
    '{{ var('execution_date') }}'::timestamp dataflow_dttm, 
    t1.{{dim_name}}_rk, 
    '{{ var('execution_date') }}'::timestamp valid_from_dttm,
    '5999-01-01 00:00:00'::timestamp valid_to_dttm,
    {% for tables in range(0,tables_field_m_name|length) %}
        --подставляем новую дату актуальности, в случаях, если ее нет, - последнюю дату актуальности
        coalesce(max(t{{tables+2}}.valid_from_dttm), max(pit.{{tables_field_m_name[tables][1]}})) {{tables_field_m_name[tables][1]}} 
    {% if not loop.last %} , {% endif %} {% endfor %}
from dbt_schema."GPR_BV_A_{{dim_name}}" t1 
left join 
	-- Ищем записи из новой выгрузки
    {% for tables in range(0,tables_field_m_name|length) %}
    (
        select {{entity_key}}, case when delete_flg = 1 then '1960-01-01' ::timestamp else valid_from_dttm end valid_from_dttm 
        from {{tables_field_m_name[tables][0]}}
        where 
        {% if tables_field_m_name[tables][2] == 'M'%} row_num = 1 and {% endif %}
            actual_flg = 1  
	    and valid_from_dttm = 
	    (select max(valid_from_dttm) from {{tables_field_m_name[tables][0]}})
            and ({{entity_key}}, valid_from_dttm) not in (select {{entity_key}},  valid_from_dttm from dbt_schema."GPR_BV_P_{{dim_name}}")
    ) t{{tables+2}} on t1.x_{{entity_key}} = t{{tables+2}}.{{entity_key}}
    {% if not loop.last %} left join {% endif %} 
    {% endfor %}
--join pit для того, чтобы узнать последнюю дату изменения, тех экземпляров, которые не изменились в последней выгрузке 
join  (select * from dbt_schema."GPR_BV_P_{{dim_name}}" where valid_to_dttm = '5999-01-01' :: timestamp) pit
on pit.{{entity_key}} = t1.{{entity_key}}
--исключение экземпляров, в которых ничего не изменилось
where ({% for tables in range(0,tables_field_m_name|length) %} t{{tables+2}}.{{entity_key}} is not null {% if not loop.last %} or {%endif%}{% endfor %})
group by t1.{{entity_key}})
--исключение экземпляров, которые были удалены на всех источниках
where
({% for tbl in tables_field_m_name %} {{tbl[1]}} > '1960-01-01' :: timestamp {% if not loop.last %} or {% endif %} {% endfor %})



{% endmacro %}

