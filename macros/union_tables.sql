--macros для объединения данных сателлитов в 1 место перед унификацией
{% macro union_sat_datas
(
	mass_tables, 
	pseudo_col, 
	dim
)  
%}
/* Передаваемые атрибуты:
* Массив, состоящий из всех сателлитов сущности с их логическими ключами
* Псевдоназвания колонок для использования в SAL
* Название измерения
*/

with uni_upd as
	-- Join данных сателлитов с sal 
	(select coalesce(sal.{{dim}}_rk, sats.{{dim}}_rk) {{dim}}_rk, 
	{% for col in pseudo_col %} {{col}} {% if not loop.last %} , {% endif %} {% endfor %} 
	from
		(
		--Объединение актуальных данных из последней выгрузки всех сателлитов с помощью union all
		{% for tbls in mass_tables %}
			select 
			{{ dim }}_rk,
			{% for col in range(1,tbls|length) %} 
				{{tbls[col]}} {% if col-1 >= 0 %} {{pseudo_col[col-1]}} {% endif %} 
				{% if not loop.last %},{% endif %} 
			{% endfor %} 
			from {{tbls[0]}} 
			where actual_flg = 1 
			and delete_flg = 0 
			and valid_from_dttm = '{{ var('execution_date') }}'::timestamp
			{% if not loop.last %} union all {% endif %}
		{% endfor %}
		) sats
	left join dbt_schema."GPR_BV_A_{{dim}}" sal
	on sal.x_{{dim}}_rk = sats.{{dim}}_rk)
	
select * from
({% for tbls in mass_tables %}
		select 
		'{{ var('run_id') }}' dataflow_id, 
		'{{ var('execution_date') }}'::timestamp dataflow_dttm, 
		valid_from_dttm, 
		source_system_dk, 
		actual_flg, 
		delete_flg,
		{{ dim }}_rk,
		{% for col in range(1,tbls|length) %}
			{{tbls[col]}} {% if col-1 >= 0 %} {{pseudo_col[col-1]}} {% endif %} {% if not loop.last %},{% endif %}
		{% endfor %} 
		from {{tbls[0]}} 
		where actual_flg = 1 
		and delete_flg = 0
		{% if not loop.last %} union all {% endif %}
	{% endfor %})
where 
({% for col in pseudo_col %} {{col}} 
	{% if not loop.last %} , {% endif %} {% endfor %}) in (select {% for col in pseudo_col %} {{col}} {% if not loop.last %} , {% endif %} 
{% endfor %} from uni_upd)
or
{{dim}}_rk in
	(select sal2.x_{{dim}}_rk from uni_upd
	join dbt_schema."GPR_BV_A_{{dim}}" sal2
	on uni_upd.{{dim}}_rk = sal2.{{dim}}_rk )
{% endmacro %}