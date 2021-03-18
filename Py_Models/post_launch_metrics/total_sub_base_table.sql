create or replace table {database}.{schema}.sub_period_in_uuid_test (
      hbo_uuid varchar (255) not null
    , provider varchar (255) not null
    , is_trial integer
    , platform_name varchar (255) not null
    , sub_session_id integer not null
    , last_update_timestamp timestamp not null
    , subscription_start_timestamp  timestamp not null
    , subscription_expire_timestamp  timestamp not null
    , constraint id_plt_session primary key (hbo_uuid, provider, platform_name, sub_session_id)
    , constraint id_plt_session_unique unique (hbo_uuid, provider, platform_name, sub_session_id)
) as (with base as (
        select
               product_code
             , provider
             , subscription_start_date
             , case
                   when (provider in ('google', 'samsung') and
                         datediff(day, subscription_expire_date, current_timestamp) <= 2) then subscription_cancel_date
                   else subscription_expire_date
                        end as subscription_expire_date
             , next_start
             , case when free_paid_ind = 'trial' then 1 else 0 end
                 as is_trial
             , is_cancel
             , provider_user_id
        from max_prod.bi_analytics.fact_common_receipt
    ),

         crs_sub_balance as (
             select
                    uuid as hbo_uuid
                  , provider
                  , case when provider in ('dtc', 'google', 'samsung', 'apple')
                            and to_date(subscription_start_date) >= to_date('2020-05-27') then 'hboMax'
                        when provider = 'amazon' and to_date(subscription_start_date) >= to_date('2020-11-17') then 'hboMax'
                        when provider = 'roku' and to_date(subscription_start_date) >= to_date('2020-12-17') then 'hboMax'
                        when to_date(subscription_start_date) < to_date('2020-05-27') then 'hboNow'
                            else null end as platform_name
                  , is_trial
                  , provider_user_id
                  , subscription_start_date
                  , case
                        when not is_cancel and next_start is not null
                            then next_start
                        else subscription_expire_date
                 end     as subscription_expire_date
            from base as a
                    left join enterprise_data.identity.idgraph_vertex as b
                        on a.provider_user_id = b.name
             where 1 = 1
               and product_code in ('hboMax', 'hboNow')
               and provider in ('dtc', 'google', 'samsung', 'apple','amazon','roku')
               and uuid is not null
               and provider_user_id is not null
               and platform_name is not null
         ),

         previous_expire_table as (
             select
                    *
                  , lag(subscription_expire_date, 1)
                        over (partition by hbo_uuid, provider, platform_name, is_trial order by subscription_start_date asc)
                            as preivous_expire_date
                  -- if the gap is less than a day, then regard it as a continous session
                  , case
                  -- only takes the autorenewal into account for now
                        when subscription_start_date = preivous_expire_date then 0
                        else 1
                            end as cont_sub_session_ind
             from crs_sub_balance
         ),


-- create sub session ind for combination
         sub_session_table as (
             select
                    *
                  , sum(cont_sub_session_ind)
                        over (partition by hbo_uuid, provider, platform_name, is_trial order by subscription_start_date asc)
                            as sub_session_ind
             from previous_expire_table
         ),

-- do combination based on the sub session
         session_connection_table as (
             select
                    hbo_uuid
                  , provider
                  , is_trial
                  , platform_name
                  , sub_session_ind
                  , current_timestamp() as last_update_timestamp
                  , min(subscription_start_date)  as subscription_start_timestamp
                  , max(subscription_expire_date) as subscription_expire_timestamp
             from sub_session_table
             group by 1, 2, 3, 4, 5
         )

    select *
    from session_connection_table
    order by hbo_uuid, subscription_start_timestamp
);
