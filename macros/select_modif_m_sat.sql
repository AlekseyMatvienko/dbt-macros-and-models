{% macro select_modif_m_sat(
    table_name, 
    source_table, 
    pks_entity_source, 
    entity_key, 
    logical_keys, 
    type_key_field, 
    attrs_source,
    attrs_sat
) 
%}
-- нумеруем строки внутри группы из источника
with rn_hash_diff_from_source as ( 
    select
        -- выбираем все строки из источника
        source.*
	    -- нумируем строки - поле row_num
        , row_number () over(
            -- нумируем в пределах партиции по логическим ключам, 
            -- например id client`а в случае с номерами
            partition by 
                {% for logical_key in logical_keys %} 
                    {{ logical_key }} {% if not loop.last %} , {% endif %}
                {% endfor %} 
            -- сортируем по дате dttm или по текущему времени если в сате такой записи нет
            -- также сортируем по стобцам атрибутов 
            -- TODO - REVIEW AND TESTS
            order by 
                coalesce (m_sat.dataflow_dttm::timestamp, current_timestamp)
                , {% for attr in attrs_source %} {{ attr }} {% if not loop.last %}, {% endif %}{% endfor %}
        ) as {{type_key_field}}
    from
	    {{ref( source_table )}} as source
    -- джойним источник и сателит
    left join (
        select 
            -- выбираем rk сущности
            {{entity_key}}
            -- выбираем тех поле - дату и время выполнения dttm
            , dataflow_dttm
            -- выбираем поля из таблица сата
            -- TODO
            , {% for attr in attrs_sat %} {{ attr }} {% if not loop.last %}, {% endif %}{% endfor %}
        -- таблица сателита
        from {{table_name}} 
        where actual_flg = 1 and delete_flg = 0
    ) as m_sat
        -- join по rk
        on md5( 
            {% for pk_entity in pks_entity_source %} 
                source.{{ pk_entity }} || '#' || 
            {% endfor %} source.oid
        ) = m_sat.{{entity_key}} 
        -- и по всем остальным полям
        -- TODO - REVIEW AND TESTS
        {% for attrs in zip(attrs_source, attrs_sat) %} and source.{{attrs[0]}} = m_sat.{{attrs[1]}} {% endfor %}
)
-- формируем md5 для rk и hash_diff для новых и измененных 
, form_md5 as (
    select 
        rn_hash_diff_from_source.*
        -- формируем rk на связанную сущность
        ,md5( {% for pk_entity in pks_entity_source %} {{ pk_entity }} || '#' || {% endfor %}  oid) {{entity_key}} 
        -- выводим номера строк в группе
        -- получаем значение для поля valid_from_dttm
        , '{{ var('execution_date') }}'::timestamp valid_from_dttm
        -- формируем hash_diff из всех значений всех столбцов в группе
        -- TODO
        , md5(
            -- вычисляем hash_diff
            -- TODO - REVIEW AND TESTS
            {% for attr in attrs_source %}
                string_agg( {{ attr }}::varchar, '#' ) over(
                    -- разбиваем окно на партиции по ключам разбиения
                    partition by {% for logical_key in logical_keys %} {{ logical_key }} {% if not loop.last %}, {% endif %}{% endfor %}
                    -- сортируем строки по {{type_key_field}}
                    -- TODO - REVIEW AND TESTS
                    order by {{type_key_field}}
                    rows between unbounded preceding and unbounded following
                ) || '#' ||
            {% endfor %} string_agg({{type_key_field}}::varchar, '#') over(
                -- разбиваем окно на партиции по ключам разбиения
                partition by {% for logical_key in logical_keys %} {{ logical_key }} {% if not loop.last %}, {% endif %}{% endfor %}
                -- сортируем строки по {{type_key_field}}
                -- TODO - REVIEW AND TESTS
                order by {{type_key_field}}
                rows between unbounded preceding and unbounded following
            )          
        ) hashdiff_key
    from 
	    rn_hash_diff_from_source
)
select
	'{{ var('run_id') }}' dataflow_id
    , '{{ var('execution_date') }}'::timestamp dataflow_dttm
    , oid source_system_dk
    , {{entity_key}}
    , {{type_key_field}}
    , valid_from_dttm
    , hashdiff_key
    , 1 actual_flg
    , 0 delete_flg
    , {% for attr in attrs_source %}
        {{ attr }}
        {% if not loop.last %}, {% endif %}
    {% endfor %}
from 
	form_md5
-- фильтруем rk - выбираем только новые или измененные (hash_diff) по атрибутам строки
where {{entity_key}} in (
    select 
        {{ entity_key }}
    from (
    -- вычитаем строки из источника строками таблицей м сата
        select
            {{entity_key}}
            , hashdiff_key
        from
            form_md5
        except
        select 
            {{ entity_key }}
            , hashdiff_key   
        from 
            -- выбираем только актуальные не удаленные записи
            {{ table_name }} where actual_flg = 1 and delete_flg = 0
    )
)
{% endmacro %}