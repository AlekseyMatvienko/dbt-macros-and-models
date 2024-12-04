
  create view "postgres"."dbt_schema"."ins_upd_to_dim_card__dbt_tmp"
    
    
  as (
    

-- sal_table (source(sal_name))
-- pit_table (source(pit_name), (sat_col_name, ))
-- entity_key 'entity_key'
-- sat_tables ((source(sat_name), sat_name, (columns, ), sat_type, oid), )
-- columns_name (column_name, )

with unif_dim as
(
	select sal.card_rk, 
		source_system_dk, 
		valid_from_dttm, 
        
            card_num_cnt
         , 
        
            card_service_name_desc
         , 
        
            discount_procent_cnt
        
        
	from
	"postgres"."dbt_schema"."GPR_BV_A_CARD" sal
	join (
    
	
		select 
		card_rk, 
		source_system_dk, 
		valid_from_dttm, 
        
           card_num_cnt card_num_cnt
         , 
        
           card_service_name_desc card_service_name_desc
         , 
        
           discount_procent_cnt discount_procent_cnt
        
        
		from "postgres"."dbt_schema"."GPR_RV_S_PROFILE_CARD_POST"
		where 
        
		actual_flg = 1
        
		
	) sat
	on sal.x_card_rk = sat.card_rk
)

select * from
(

	select 
	'manual__2024-12-03T11:00:52.076204+00:00' dataflow_id,
	'2024-12-03 11:00:52.076204+00:00'::timestamp dataflow_dttm,
	pit.card_rk,
	pit.valid_from_dttm,
	pit.valid_to_dttm,
    
	case 
        
		when unif_dim_GPR_RV_S_PROFILE_CARD_POST.valid_from_dttm != '1960-01-01' 
		then unif_dim_GPR_RV_S_PROFILE_CARD_POST.card_num_cnt 
        
	end card_num_cnt
     ,   
    
	case 
        
		when unif_dim_GPR_RV_S_PROFILE_CARD_POST.valid_from_dttm != '1960-01-01' 
		then unif_dim_GPR_RV_S_PROFILE_CARD_POST.card_service_name_desc 
        
	end card_service_name_desc
     ,   
    
	case 
        
		when unif_dim_GPR_RV_S_PROFILE_CARD_POST.valid_from_dttm != '1960-01-01' 
		then unif_dim_GPR_RV_S_PROFILE_CARD_POST.discount_procent_cnt 
        
	end discount_procent_cnt
      
    

	from 
		(
		select 
		card_rk, 
		valid_from_dttm, 
		valid_to_dttm,
         
			profile_card_post_vf_dttm
		  
        

		from "postgres"."dbt_schema"."GPR_BV_P_CARD"
		--where valid_to_dttm = '5999-01-01'
		where dataflow_dttm = '2024-12-03 11:00:52.076204+00:00'::timestamp
		) pit
	
	left join 
     
	
		(select * from unif_dim where source_system_dk = 107023) unif_dim_GPR_RV_S_PROFILE_CARD_POST
	    on pit.card_rk = unif_dim_GPR_RV_S_PROFILE_CARD_POST.card_rk
	  
    
)

where 

    card_num_cnt is not null
 or 

    card_service_name_desc is not null
 or 

    discount_procent_cnt is not null






--depends on "postgres"."dbt_schema"."GPR_RV_S_PROFILE_CARD_POST"
--depends on "postgres"."dbt_schema"."GPR_BV_A_CARD"
--depends on "postgres"."dbt_schema"."GPR_BV_P_CARD"
  );