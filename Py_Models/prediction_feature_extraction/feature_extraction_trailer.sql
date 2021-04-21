COPY INTO {stage}/
FROM (
    with final as (select distinct
          concat(case when f.platform_name = 'hboNow' then 0
                    else 1 end, '-', f.match_id) as match_id_platform
        , ifnull(retail_trailer_view_metric, -1) as retail_trailer_view_metric
    from {database}.{schema}.{trailer_table} as t
    right join {database}.{schema}.{funnel_metrics_table} as f
        on t.match_id_platform = concat(case when f.platform_name = 'hboNow' then 0
                    else 1 end, '-', f.match_id)
    order by retail_trailer_view_metric desc)

    select * from final
) file_format = (type='csv') OVERWRITE = TRUE header = TRUE;

;



