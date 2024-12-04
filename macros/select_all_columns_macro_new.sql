{% macro select_all_columns_macro_new(
    table_name, 
    source_table, 
    pks_source_table, 
    entity_key, 
    row_name, 
    args=( ),
    attrs_target=(),
    attrs_source=(),
    type_sat = ''
)
%} 

/*
Поля для случая м сата:
    attrs_target=() - tuple строк - названия столбцов таблицы м сата - бизнес атрибутов
    attrs_source=() - tuple строк - названия столбцов таблицы источника - бизнес атрибутов

они нужны для того, чтобы правильно "вычитать" (except) выборки, 
и находить удаленные записи
*/

-- макрос, который смотрит какие строки удалены
select
    {% if type_sat == 'E' %}
        '{{ var('run_id') }}' dataflow_id,
        '{{ var('execution_date') }}'::timestamp dataflow_dttm,
        hashdiff_key,
        {{ entity_key }},
        1 delete_flg,
        1 actual_flg, 
        source_system_dk,
        '{{ var('execution_date') }}'::timestamp valid_from_dttm
    {% elif type_sat == 'M' %}
        '{{ var('run_id') }}' dataflow_id,
        '{{ var('execution_date') }}'::timestamp dataflow_dttm,
        source_system_dk, 
        {{ entity_key }},
	{{ row_name }}, 
        '{{ var('execution_date') }}'::timestamp valid_from_dttm, 
        hashdiff_key,
        1 actual_flg,
        1 delete_flg,
        {% for i in args %}
            {{ i }}
            {% if not loop.last %}, {% endif %}
        {% endfor %}
    {% else %}
        '{{ var('run_id') }}' dataflow_id,
        '{{ var('execution_date') }}'::timestamp dataflow_dttm,
        source_system_dk, 
        {{ entity_key }},
        '{{ var('execution_date') }}'::timestamp valid_from_dttm, 
        hashdiff_key,
        1 actual_flg,
        1 delete_flg,
        {% for i in args %}
            {{ i }}
            {% if not loop.last %}, {% endif %}
        {% endfor %}
    {% endif %}
from 
    {{ table_name }}
where (
    {% if type_sat == 'M' %}
        {{ entity_key }}
        , 
        {% for attr in attrs_target %}
            {{ attr }}
            {% if not loop.last %}, {% endif %}
        {% endfor %}
    {% else %}
        {{ entity_key }}
    {% endif %}
    ) in
(
select 
    {% if type_sat == 'M' %}
        {{ entity_key }}
        , 
        {% for attr in attrs_target %}
            {{ attr }}
            {% if not loop.last %}, {% endif %}
        {% endfor %}
    {% else %}
        {{ entity_key }}
    {% endif %}
from
	(
	select
        -- если сателит является мульти, то нужно также сравнивать со всеми бизнес атрибутами
        {% if type_sat == 'M' %}
            {{ entity_key }}
            , 
            {% for attr in attrs_target %}
                {{ attr }}
                {% if not loop.last %}, {% endif %}
            {% endfor %}
        {% else %}
            {{ entity_key }}
        {% endif %}
	from 
		 {{ table_name }}
	 where delete_flg = 0 and actual_flg = 1
    except
    select
        -- если сателит является мульти, то нужно также сравнивать со всеми бизнес атрибутами
        {% if type_sat == 'M' %}
            md5( {% for i in pks_source_table %} {{ i }} || '#' || {% endfor %}  oid) entity_rk
            , 
            {% for attr in attrs_source %}
                {{ attr }}
                {% if not loop.last %}, {% endif %}
            {% endfor %}
        {% else %}
            md5( {% for i in pks_source_table %} {{ i }} || '#' || {% endfor %}  oid) entity_rk
        {% endif %}
	from 
		 {{ ref( source_table ) }} )
		)
	and actual_flg = 1
    and delete_flg = 0

{% endmacro %}

