{{
    config(
        materialized = 'table',
        enabled = false
    )
}}
select {{ env_var('DBT_FIRST_ENV_VAR') }}::string as env_var_value

union all

select {{ env_var('DBT_ENV_SECRET_FIRST_ENV_VAR') }}::string as env_var_value