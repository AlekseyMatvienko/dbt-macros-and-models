{{ config(materialized='table') }}

select * from {{ source('dbt_schema', 'ods_receipt_post') }}  where execution_date = '{{ var('execution_date') }}'

--depends on {{ source('dbt_schema', 'ods_receipt_post') }} 