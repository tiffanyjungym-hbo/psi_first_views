USE SCHEMA workspace;

COPY INTO {stage}/hourspct/trailer_feature.csv
FROM (
    with final as (select distinct
          f.match_id
        , ifnull(t.retail_trailer_view_metric, -1) as retail_trailer_view_metric
    from {database}.{schema}.trailer_view_percent_test as t
    right join {database}.{schema}.title_retail_funnel_metrics as f
        on t.match_id = f.match_id
    order by retail_trailer_view_metric desc)

    select * from final
) file_format = (type='csv' compression = 'NONE') single = true OVERWRITE = TRUE header = TRUE;



