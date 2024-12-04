
/* Передаваемые атрибуты:
* Массив, состоящий из всех сателлитов сущности с их логическими ключами
* Псевдоназвания колонок для использования в SAL
* Название измерения
*/

with uni_upd as
	-- Join данных сателлитов с sal 
	(select coalesce(sal.CARD_rk, sats.CARD_rk) CARD_rk, 
	 card_num_cnt   
	from
		(
		--Объединение актуальных данных из последней выгрузки всех сателлитов с помощью union all
		
			select 
			CARD_rk,
			 
				card_num_cnt  card_num_cnt  
				 
			 
			from "postgres"."dbt_schema"."GPR_RV_S_PROFILE_CARD_POST" 
			where actual_flg = 1 
			and delete_flg = 0 
			and valid_from_dttm = '2024-12-03 11:00:52.076204+00:00'::timestamp
			
		
		) sats
	left join dbt_schema."GPR_BV_A_CARD" sal
	on sal.x_CARD_rk = sats.CARD_rk)
	
select * from
(
		select 
		'manual__2024-12-03T11:00:52.076204+00:00' dataflow_id, 
		'2024-12-03 11:00:52.076204+00:00'::timestamp dataflow_dttm, 
		valid_from_dttm, 
		source_system_dk, 
		actual_flg, 
		delete_flg,
		CARD_rk,
		
			card_num_cnt  card_num_cnt  
		 
		from "postgres"."dbt_schema"."GPR_RV_S_PROFILE_CARD_POST" 
		where actual_flg = 1 
		and delete_flg = 0
		
	)
where 
( card_num_cnt 
	 ) in (select  card_num_cnt  
 from uni_upd)
or
CARD_rk in
	(select sal2.x_CARD_rk from uni_upd
	join dbt_schema."GPR_BV_A_CARD" sal2
	on uni_upd.CARD_rk = sal2.CARD_rk )


--depends on "postgres"."dbt_schema"."GPR_RV_S_PROFILE_CARD_POST"