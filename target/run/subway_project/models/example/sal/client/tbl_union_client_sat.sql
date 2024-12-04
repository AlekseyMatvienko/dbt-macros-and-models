
  create view "postgres"."dbt_schema"."tbl_union_client_sat__dbt_tmp"
    
    
  as (
    
/* Передаваемые атрибуты:
* Массив, состоящий из всех сателлитов сущности с их логическими ключами
* Псевдоназвания колонок для использования в SAL
* Название измерения
*/

with uni_upd as
	-- Join данных сателлитов с sal 
	(select coalesce(sal.CLIENT_rk, sats.CLIENT_rk) CLIENT_rk, 
	 name_desc  ,   birthday_dt   
	from
		(
		--Объединение актуальных данных из последней выгрузки всех сателлитов с помощью union all
		
			select 
			CLIENT_rk,
			 
				fio_desc  name_desc  
				, 
			 
				birthday_dt  birthday_dt  
				 
			 
			from "postgres"."dbt_schema"."GPR_RV_M_CLIENT_PROFILE_POST" 
			where actual_flg = 1 
			and delete_flg = 0 
			and valid_from_dttm = '2024-12-03 11:00:52.076204+00:00'::timestamp
			 union all 
		
			select 
			CLIENT_rk,
			 
				name_desc  name_desc  
				, 
			 
				birthday_dt  birthday_dt  
				 
			 
			from "postgres"."dbt_schema"."GPR_RV_M_CLIENT_SUBWAY_STAR" 
			where actual_flg = 1 
			and delete_flg = 0 
			and valid_from_dttm = '2024-12-03 11:00:52.076204+00:00'::timestamp
			 union all 
		
			select 
			CLIENT_rk,
			 
				name_desc  name_desc  
				, 
			 
				birthdate_dt  birthday_dt  
				 
			 
			from "postgres"."dbt_schema"."GPR_RV_S_CLIENT_CLIENT_FIREBIRD" 
			where actual_flg = 1 
			and delete_flg = 0 
			and valid_from_dttm = '2024-12-03 11:00:52.076204+00:00'::timestamp
			
		
		) sats
	left join dbt_schema."GPR_BV_A_CLIENT" sal
	on sal.x_CLIENT_rk = sats.CLIENT_rk)
	
select * from
(
		select 
		'manual__2024-12-03T11:00:52.076204+00:00' dataflow_id, 
		'2024-12-03 11:00:52.076204+00:00'::timestamp dataflow_dttm, 
		valid_from_dttm, 
		source_system_dk, 
		actual_flg, 
		delete_flg,
		CLIENT_rk,
		
			fio_desc  name_desc  ,
		
			birthday_dt  birthday_dt  
		 
		from "postgres"."dbt_schema"."GPR_RV_M_CLIENT_PROFILE_POST" 
		where actual_flg = 1 
		and delete_flg = 0
		 union all 
	
		select 
		'manual__2024-12-03T11:00:52.076204+00:00' dataflow_id, 
		'2024-12-03 11:00:52.076204+00:00'::timestamp dataflow_dttm, 
		valid_from_dttm, 
		source_system_dk, 
		actual_flg, 
		delete_flg,
		CLIENT_rk,
		
			name_desc  name_desc  ,
		
			birthday_dt  birthday_dt  
		 
		from "postgres"."dbt_schema"."GPR_RV_M_CLIENT_SUBWAY_STAR" 
		where actual_flg = 1 
		and delete_flg = 0
		 union all 
	
		select 
		'manual__2024-12-03T11:00:52.076204+00:00' dataflow_id, 
		'2024-12-03 11:00:52.076204+00:00'::timestamp dataflow_dttm, 
		valid_from_dttm, 
		source_system_dk, 
		actual_flg, 
		delete_flg,
		CLIENT_rk,
		
			name_desc  name_desc  ,
		
			birthdate_dt  birthday_dt  
		 
		from "postgres"."dbt_schema"."GPR_RV_S_CLIENT_CLIENT_FIREBIRD" 
		where actual_flg = 1 
		and delete_flg = 0
		
	)
where 
( name_desc 
	 ,   birthday_dt 
	 ) in (select  name_desc  ,  
 birthday_dt  
 from uni_upd)
or
CLIENT_rk in
	(select sal2.x_CLIENT_rk from uni_upd
	join dbt_schema."GPR_BV_A_CLIENT" sal2
	on uni_upd.CLIENT_rk = sal2.CLIENT_rk )


--depends on "postgres"."dbt_schema"."GPR_RV_M_CLIENT_PROFILE_POST"
--depends on "postgres"."dbt_schema"."GPR_RV_M_CLIENT_SUBWAY_STAR"
--depends on "postgres"."dbt_schema"."GPR_RV_S_CLIENT_CLIENT_FIREBIRD"
  );