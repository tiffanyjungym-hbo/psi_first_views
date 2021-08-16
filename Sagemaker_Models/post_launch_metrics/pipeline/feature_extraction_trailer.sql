USE SCHEMA workspace;

COPY INTO {stage}/input_percent_view/trailer_feature.csv
FROM (
    with base as (
        select
            match_id_platform
            , retail_trailer_view_metric
        from (
            select
                match_id_platform
                , retail_trailer_view_metric
                , row_number() over (partition by match_id_platform order by nday_before desc) as day_num
            from {database}.{schema}.trailer_retail_view_percent as t
        )
        where day_num = 1
        order by retail_trailer_view_metric desc
    ),

    final as (
        select distinct
            concat(case when f.platform_name = 'hboNow' then 0
                        else 1 end, '-', f.match_id) as match_id_platform
            , ifnull(t.retail_trailer_view_metric, -1) as retail_trailer_view_metric
        from base as t
        right join {database}.{schema}.title_retail_funnel_metrics as f
            on t.match_id_platform = concat(case when f.platform_name = 'hboNow' then 0
                    else 1 end, '-', f.match_id)
        order by retail_trailer_view_metric desc
    )

    select * from final
) file_format = (type='csv' compression = 'NONE') single = true OVERWRITE = TRUE header = TRUE;



