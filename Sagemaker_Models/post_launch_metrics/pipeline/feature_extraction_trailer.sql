USE SCHEMA workspace;

COPY INTO {stage}/input_percent_view/trailer_feature.csv
FROM (
    with base as (
        select distinct
            match_id_platform
            , cumulative_day_num
            , total_trailer_num
            , retail_trailer_view_metric
        from (
            select distinct
                  match_id_platform
                , cumulative_day_num
                , total_trailer_num
                , retail_trailer_view_metric
                , row_number() over (partition by match_id_platform order by nday_before desc) as row_num
            from {database}.{schema}.trailer_retail_view_percent_d28) as p
        where 1=1
            and row_num = 1
        ),

    final as (
        select distinct
              concat(case when f.platform_name = 'hboNow' then 0
                            else 1 end, '-', f.match_id) as match_id_platform
            , case when cumulative_day_num is null then -1 else cumulative_day_num
                end as cumulative_day_num
            , case when total_trailer_num is null then -1 else total_trailer_num
                end as total_trailer_num
            , case when retail_trailer_view_metric is null then -1 else retail_trailer_view_metric
                end as retail_trailer_view_metric
            , case when retail_trailer_view_metric/cumulative_day_num is null then -1 else
                retail_trailer_view_metric/cumulative_day_num end
                    as avg_trail_metric_per_day
        from base as b
        right join {database}.{schema}.title_retail_funnel_metrics as f
            on b.match_id_platform = concat(case when f.platform_name = 'hboNow' then 0
                            else 1 end, '-', f.match_id)
    )

    select * from final
    order by retail_trailer_view_metric desc
) file_format = (type='csv' compression = 'NONE') single = true OVERWRITE = TRUE header = TRUE;



