
  create view "postgres"."dbt_schema"."ins_upd_to_dim_client__dbt_tmp"
    
    
  as (
    

-- sal_table (source(sal_name))
-- pit_table (source(pit_name), (sat_col_name, ))
-- entity_key 'entity_key'
-- sat_tables ((source(sat_name), sat_name, (columns, ), sat_type, oid), )
-- columns_name (column_name, )

with unif_dim as
(
	select sal.client_rk, 
		source_system_dk, 
		valid_from_dttm, 
        
            client_name_desc
         , 
        
            client_phone_desc
         , 
        
            client_city_desc
         , 
        
            client_birthday_dt
        
        
	from
	"postgres"."dbt_schema"."GPR_BV_A_CLIENT" sal
	join (
    
	
		select 
		client_rk, 
		source_system_dk, 
		valid_from_dttm, 
        
           name_desc client_name_desc
         , 
        
           phone_desc client_phone_desc
         , 
        
           city_desc client_city_desc
         , 
        
           birthday_dt client_birthday_dt
        
        
		from "postgres"."dbt_schema"."GPR_RV_M_CLIENT_SUBWAY_STAR"
		where 
         row_num = 1 and 
		actual_flg = 1
         union all 
		
	
		select 
		client_rk, 
		source_system_dk, 
		valid_from_dttm, 
        
           fio_desc client_name_desc
         , 
        
           phone_desc client_phone_desc
         , 
        
           null client_city_desc
         , 
        
           birthday_dt client_birthday_dt
        
        
		from "postgres"."dbt_schema"."GPR_RV_M_CLIENT_PROFILE_POST"
		where 
         row_num = 1 and 
		actual_flg = 1
         union all 
		
	
		select 
		client_rk, 
		source_system_dk, 
		valid_from_dttm, 
        
           name_desc client_name_desc
         , 
        
           null client_phone_desc
         , 
        
           null client_city_desc
         , 
        
           birthdate_dt client_birthday_dt
        
        
		from "postgres"."dbt_schema"."GPR_RV_S_CLIENT_CLIENT_FIREBIRD"
		where 
        
		actual_flg = 1
        
		
	) sat
	on sal.x_client_rk = sat.client_rk
)

select * from
(

	select 
	'manual__2024-12-03T11:00:52.076204+00:00' dataflow_id,
	'2024-12-03 11:00:52.076204+00:00'::timestamp dataflow_dttm,
	pit.client_rk,
	pit.valid_from_dttm,
	pit.valid_to_dttm,
    
	case 
        
		when unif_dim_GPR_RV_M_CLIENT_SUBWAY_STAR.valid_from_dttm != '1960-01-01' 
		then unif_dim_GPR_RV_M_CLIENT_SUBWAY_STAR.client_name_desc 
        
		when unif_dim_GPR_RV_M_CLIENT_PROFILE_POST.valid_from_dttm != '1960-01-01' 
		then unif_dim_GPR_RV_M_CLIENT_PROFILE_POST.client_name_desc 
        
		when unif_dim_GPR_RV_S_CLIENT_CLIENT_FIREBIRD.valid_from_dttm != '1960-01-01' 
		then unif_dim_GPR_RV_S_CLIENT_CLIENT_FIREBIRD.client_name_desc 
        
	end client_name_desc
     ,   
    
	case 
        
		when unif_dim_GPR_RV_M_CLIENT_SUBWAY_STAR.valid_from_dttm != '1960-01-01' 
		then unif_dim_GPR_RV_M_CLIENT_SUBWAY_STAR.client_phone_desc 
        
		when unif_dim_GPR_RV_M_CLIENT_PROFILE_POST.valid_from_dttm != '1960-01-01' 
		then unif_dim_GPR_RV_M_CLIENT_PROFILE_POST.client_phone_desc 
        
		when unif_dim_GPR_RV_S_CLIENT_CLIENT_FIREBIRD.valid_from_dttm != '1960-01-01' 
		then unif_dim_GPR_RV_S_CLIENT_CLIENT_FIREBIRD.client_phone_desc 
        
	end client_phone_desc
     ,   
    
	case 
        
		when unif_dim_GPR_RV_M_CLIENT_SUBWAY_STAR.valid_from_dttm != '1960-01-01' 
		then unif_dim_GPR_RV_M_CLIENT_SUBWAY_STAR.client_city_desc 
        
		when unif_dim_GPR_RV_M_CLIENT_PROFILE_POST.valid_from_dttm != '1960-01-01' 
		then unif_dim_GPR_RV_M_CLIENT_PROFILE_POST.client_city_desc 
        
		when unif_dim_GPR_RV_S_CLIENT_CLIENT_FIREBIRD.valid_from_dttm != '1960-01-01' 
		then unif_dim_GPR_RV_S_CLIENT_CLIENT_FIREBIRD.client_city_desc 
        
	end client_city_desc
     ,   
    
	case 
        
		when unif_dim_GPR_RV_M_CLIENT_SUBWAY_STAR.valid_from_dttm != '1960-01-01' 
		then unif_dim_GPR_RV_M_CLIENT_SUBWAY_STAR.client_birthday_dt 
        
		when unif_dim_GPR_RV_M_CLIENT_PROFILE_POST.valid_from_dttm != '1960-01-01' 
		then unif_dim_GPR_RV_M_CLIENT_PROFILE_POST.client_birthday_dt 
        
		when unif_dim_GPR_RV_S_CLIENT_CLIENT_FIREBIRD.valid_from_dttm != '1960-01-01' 
		then unif_dim_GPR_RV_S_CLIENT_CLIENT_FIREBIRD.client_birthday_dt 
        
	end client_birthday_dt
      
    

	from 
		(
		select 
		client_rk, 
		valid_from_dttm, 
		valid_to_dttm,
         
			client_subway_star_vf_dttm
		 ,   
         
			profile_client_post_vf_dttm
		  
        

		from "postgres"."dbt_schema"."GPR_BV_P_CLIENT"
		--where valid_to_dttm = '5999-01-01'
		where dataflow_dttm = '2024-12-03 11:00:52.076204+00:00'::timestamp
		) pit
	
	left join 
     
	
		(select * from unif_dim where source_system_dk = 3515641477) unif_dim_GPR_RV_M_CLIENT_SUBWAY_STAR
	    on pit.client_rk = unif_dim_GPR_RV_M_CLIENT_SUBWAY_STAR.client_rk
	 left join   
     
	
		(select * from unif_dim where source_system_dk = 107023) unif_dim_GPR_RV_M_CLIENT_PROFILE_POST
	    on pit.client_rk = unif_dim_GPR_RV_M_CLIENT_PROFILE_POST.client_rk
	 left join   
     
	
		(select * from unif_dim where source_system_dk = 1) unif_dim_GPR_RV_S_CLIENT_CLIENT_FIREBIRD
	    on pit.client_rk = unif_dim_GPR_RV_S_CLIENT_CLIENT_FIREBIRD.client_rk
	  
    
)

where 

    client_name_desc is not null
 or 

    client_phone_desc is not null
 or 

    client_city_desc is not null
 or 

    client_birthday_dt is not null






--depends on "postgres"."dbt_schema"."GPR_RV_M_CLIENT_SUBWAY_STAR"
--depends on "postgres"."dbt_schema"."GPR_RV_M_CLIENT_PROFILE_POST"
--depends on "postgres"."dbt_schema"."GPR_RV_S_CLIENT_CLIENT_FIREBIRD"
--depends on "postgres"."dbt_schema"."GPR_BV_A_CLIENT"
--depends on "postgres"."dbt_schema"."GPR_BV_P_CLIENT"
  );