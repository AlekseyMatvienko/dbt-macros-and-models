
  create view "postgres"."dbt_schema"."ins_to_pit_client__dbt_tmp"
    
    
  as (
    select dataflow_id, dataflow_dttm,
       client_rk, valid_from_dttm, valid_to_dttm,
       client_subway_star_vf_dttm,
       profile_client_post_vf_dttm,
       client_phones_vf_dttm 
    from (
    

select * from
(select 
    'manual__2024-12-03T11:00:52.076204+00:00' dataflow_id, 
    '2024-12-03 11:00:52.076204+00:00'::timestamp dataflow_dttm, 
    t1.CLIENT_rk, 
    '2024-12-03 11:00:52.076204+00:00'::timestamp valid_from_dttm,
    '5999-01-01 00:00:00'::timestamp valid_to_dttm,
    
        --подставляем новую дату актуальности, в случаях, если ее нет, - последнюю дату актуальности
        coalesce(max(t2.valid_from_dttm), max(pit.client_subway_star_vf_dttm)) client_subway_star_vf_dttm 
     ,  
        --подставляем новую дату актуальности, в случаях, если ее нет, - последнюю дату актуальности
        coalesce(max(t3.valid_from_dttm), max(pit.profile_client_post_vf_dttm)) profile_client_post_vf_dttm 
     ,  
        --подставляем новую дату актуальности, в случаях, если ее нет, - последнюю дату актуальности
        coalesce(max(t4.valid_from_dttm), max(pit.client_phones_vf_dttm)) client_phones_vf_dttm 
     
from dbt_schema."GPR_BV_A_CLIENT" t1 
left join 
	-- Ищем записи из новой выгрузки
    
    (
        select client_rk, case when delete_flg = 1 then '1960-01-01' ::timestamp else valid_from_dttm end valid_from_dttm 
        from "postgres"."dbt_schema"."GPR_RV_M_CLIENT_SUBWAY_STAR"
        where 
         row_num = 1 and 
            actual_flg = 1  
	    and valid_from_dttm = 
	    (select max(valid_from_dttm) from "postgres"."dbt_schema"."GPR_RV_M_CLIENT_SUBWAY_STAR")
            and (client_rk, valid_from_dttm) not in (select client_rk,  valid_from_dttm from dbt_schema."GPR_BV_P_CLIENT")
    ) t2 on t1.x_client_rk = t2.client_rk
     left join  
    
    (
        select client_rk, case when delete_flg = 1 then '1960-01-01' ::timestamp else valid_from_dttm end valid_from_dttm 
        from "postgres"."dbt_schema"."GPR_RV_M_CLIENT_PROFILE_POST"
        where 
         row_num = 1 and 
            actual_flg = 1  
	    and valid_from_dttm = 
	    (select max(valid_from_dttm) from "postgres"."dbt_schema"."GPR_RV_M_CLIENT_PROFILE_POST")
            and (client_rk, valid_from_dttm) not in (select client_rk,  valid_from_dttm from dbt_schema."GPR_BV_P_CLIENT")
    ) t3 on t1.x_client_rk = t3.client_rk
     left join  
    
    (
        select client_rk, case when delete_flg = 1 then '1960-01-01' ::timestamp else valid_from_dttm end valid_from_dttm 
        from "postgres"."dbt_schema"."GPR_RV_S_CLIENT_CLIENT_FIREBIRD"
        where 
        
            actual_flg = 1  
	    and valid_from_dttm = 
	    (select max(valid_from_dttm) from "postgres"."dbt_schema"."GPR_RV_S_CLIENT_CLIENT_FIREBIRD")
            and (client_rk, valid_from_dttm) not in (select client_rk,  valid_from_dttm from dbt_schema."GPR_BV_P_CLIENT")
    ) t4 on t1.x_client_rk = t4.client_rk
     
    
--join pit для того, чтобы узнать последнюю дату изменения, тех экземпляров, которые не изменились в последней выгрузке 
join  (select * from dbt_schema."GPR_BV_P_CLIENT" where valid_to_dttm = '5999-01-01' :: timestamp) pit
on pit.client_rk = t1.client_rk
--исключение экземпляров, в которых ничего не изменилось
where ( t2.client_rk is not null  or  t3.client_rk is not null  or  t4.client_rk is not null )
group by t1.client_rk)
--исключение экземпляров, которые были удалены на всех источниках
where
( client_subway_star_vf_dttm > '1960-01-01' :: timestamp  or   profile_client_post_vf_dttm > '1960-01-01' :: timestamp  or   client_phones_vf_dttm > '1960-01-01' :: timestamp  )




    union all
    select * from "postgres"."dbt_schema"."ins_new_to_pit_client"
    )

--depends on "postgres"."dbt_schema"."ins_new_to_pit_client"
--depends on "postgres"."dbt_schema"."GPR_RV_M_CLIENT_SUBWAY_STAR"
--depends on "postgres"."dbt_schema"."GPR_RV_M_CLIENT_PROFILE_POST"
--depends on "postgres"."dbt_schema"."GPR_RV_S_CLIENT_CLIENT_FIREBIRD"
  );