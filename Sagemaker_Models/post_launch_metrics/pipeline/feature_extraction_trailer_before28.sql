USE SCHEMA workspace;

COPY INTO {stage}/input_percent_view/prelaunch_trailer_feature_before28.csv
FROM (
    with base as (
        select distinct
            match_id_platform
            , cumulative_day_num
            , retail_trailer_view_metric
        from (
            select distinct
                  match_id_platform
                , cumulative_day_num
                , retail_trailer_view_metric
                , row_number() over (partition by match_id_platform order by last_update_timestamp desc) as row_num
            from {database}.{schema}.trailer_retail_view_percent as before28
        where 1=1
            and row_num = 1
        ),

    inal as (
        select distinct
            concat(case when f.platform_name = 'hboNow' then 0
                else 1 end, '-', f.match_id) as match_id_platform
            , ifnull(cumulative_day_num, -1) as trailer_metric_before28_cumday
            , ifnull(retail_trailer_view_metric, -1) as trailer_metric_before28
        from {database}.{schema}.title_retail_funnel_metrics as f
        left join base
            on base.match_id_platform = concat(case when f.platform_name = 'hboNow' then 0
                else 1 end, '-', f.match_id)
        where 1=1
        )

    select * from final
    order by trailer_metric_before28 desc
) file_format = (type='csv' compression = 'NONE') single = true OVERWRITE = TRUE header = TRUE;



