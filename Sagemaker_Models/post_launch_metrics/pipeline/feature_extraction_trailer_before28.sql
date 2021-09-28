USE SCHEMA workspace;

COPY INTO {stage}/input_percent_view/prelaunch_trailer_feature_before28.csv
FROM (
    with final as (
        select distinct
            concat(case when f.platform_name = 'hboNow' then 0
                else 1 end, '-', f.match_id) as match_id_platform
            , ifnull(cumulative_day_num, -1) as trailer_metric_before28_cumday
            , ifnull(retail_trailer_view_metric, -1) as trailer_metric_before28
        from max_dev.workspace.title_retail_funnel_metrics as f
        left join max_dev.workspace.trailer_retail_view_percent as before28
            on before28.match_id_platform = concat(case when f.platform_name = 'hboNow' then 0
                else 1 end, '-', f.match_id)
        where 1=1
        )

    select * from final
    order by trailer_metric_before28 desc
) file_format = (type='csv' compression = 'NONE') single = true OVERWRITE = TRUE header = TRUE;



