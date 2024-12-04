-- вставляем новые rk в объект hub клиента
{{ ins_hub_macros( '"dbt_schema"."GPR_RV_H_CLIENT"', 'firebird_cut_client_ods', "client_rk", ("id", )  )  }}