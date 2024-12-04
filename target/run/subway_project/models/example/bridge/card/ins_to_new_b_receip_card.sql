
  create view "postgres"."dbt_schema"."ins_to_new_b_receip_card__dbt_tmp"
    
    
  as (
    -- фильтруем бридж самая актуальная информация 
with cut_bridge as (

select 
	*
from 
	dbt_schema."GPR_BV_B_RECEIPT_CARD_POST"
where 
	(card_tech_key, valid_from_dttm)
	in 
	(select 
		card_tech_key, 
		max(valid_from_dttm) mx_valid 
	from 
		dbt_schema."GPR_BV_B_RECEIPT_CARD_POST" 
	group by card_tech_key )
)

 ,
 
 -- фильтруем транзакцинный линк самая актуальная информация 
 cut_transaction as (
 
		select 
			client_rk,
			md5(card_cnt || '#' || '3055104511' ) card_hash ,
			card_cnt,
			max(dataflow_dttm) dataflow_dttm 
		from 
			dbt_schema."GPR_RV_T_RECEIPT_POST" group by client_rk, card_cnt
 )
 


select 
    'manual__2024-11-22T11:45:23.346621+00:00' dataflow_id , 
    '2024-11-22 11:45:23.346621+00:00'::timestamp dataflow_dttm, 
	card_hash card_tech_key,
	'2024-11-22 11:45:23.346621+00:00'::timestamp valid_from_dttm , 
	s_cl.name_desc client_name_desc,
	case 
		when phone_cnt = 1 then s_cl.phone_desc
		else null
	end  client_phone_desc,
	s_cl.city_desc client_city_desc,
	s_cl.birthday_dt client_birthday_dt,
	s_cl.age_cnt client_age_cnt,
	t_nw.card_cnt client_card_cnt
from 
	(select 
		ct.card_hash,
		ct.card_cnt,
		ct.client_rk
	from 
		cut_transaction ct
	left join
		cut_bridge cd
	on ct.card_hash = cd.card_tech_key 
	where cd.card_tech_key is null ) t_nw
join 
	(
	select
		a_cl.client_rk ,
		m_cl.client_rk not_unif_client_rk,
		m_cl.name_desc,
		m_cl.phone_desc,
		m_cl.city_desc,
		m_cl.birthday_dt,
		m_cl.age_cnt,
		count( phone_desc) over(partition by a_cl.client_rk) phone_cnt  
	from 
		dbt_schema."GPR_RV_M_CLIENT_SUBWAY_STAR" m_cl 
	join 
		dbt_schema."GPR_BV_A_CLIENT" a_cl 
	on m_cl.client_rk = a_cl.x_client_rk 
	where actual_flg = 1 and delete_flg = 0 
	)  s_cl
on t_nw.client_rk = s_cl.not_unif_client_rk
  );