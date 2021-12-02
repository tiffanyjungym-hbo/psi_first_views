use schema {schema};
--
-- Existing title prediction table
--
create or replace table {database}.{schema}.title_percent_metric_pred(
        title_name varchar (255) not null
        , match_id varchar (255) not null
        , days_since_first_offered integer not null
        , percent_actives float
        , percent_view float
        , data_type varchar (255) not null
    ) as  (
    with pred_active as (
        select
                match_id
              , percent_view_pred as percent_actives
              , 'prediction' as data_type
          from (
              select
                  title_name
                  , match_id
                  , platform_name
                  , percent_view_pred
                  , pred_day
                  , row_number() over (partition by title_name, platform_name order by pred_day desc) as row_num
              from {database}.{schema}.pct_actives_scoring_pipeline)
          where 1=1
              and row_num = 1
              and platform_name = 1 -- max new title only
              and pred_day > 0
    ),

    pred_view as (
        select
              t.title_name
            , t.match_id
            , percent_view_pred*100 as percent_view
        from (
            select
                title_name
                , match_id
                , platform_name
                , percent_view_pred
                , pred_day
                , row_number() over (partition by title_name, platform_name order by pred_day desc) as row_num
            from {database}.{schema}.title_retail_percent_view_d28_prediction_new) as p
        left join (select distinct
                  title_name
                , match_id
                , earliest_offered_timestamp
                , platform_name
              from {database}.{schema}.title_retail_funnel_metrics) as t
            on p.match_id = t.match_id
                and p.platform_name = (case when t.platform_name = 'hboMax' then 1 else 0 end)
                and p.title_name = p.title_name
        where 1=1
            and row_num = 1
            and t.platform_name = 'hboMax'
        order by percent_view_pred desc
    )

    select distinct
        title_name
        , coalesce(a.match_id, v.match_id) as match_id
        , 28 AS days_since_first_offered
        , percent_actives
        , percent_view
        , 'prediction' as data_type
    from pred_active as a
    full outer join pred_view as v
        on a.match_id = v.match_id
    order by percent_actives desc
);

create or replace table {database}.{schema}.title_percent_metric_actual(
    title_name varchar (255) not null
    , match_id varchar (255) not null
    , days_since_first_offered integer not null
    , percent_actives float
    , percent_view float
    , data_type varchar (255) not null
    ) as  (
        select
            title_name
            , coalesce(a.match_id, v.match_id) as match_id
            , days_since_first_offered
            , pct_actives as percent_active
            , retail_viewed_count_percent*100 as percent_view
            , 'actual' as data_type
        from {database}.{schema}.pct_actives_metric_values_pipeline as a
        full outer join {database}.{schema}.title_retail_funnel_metrics as v
            on a.match_id = v.match_id
                and a.days_on_hbo_max = v.days_since_first_offered
        where 1=1
            and platform_name = 'hboMax'
        order by title_name, days_since_first_offered
)

