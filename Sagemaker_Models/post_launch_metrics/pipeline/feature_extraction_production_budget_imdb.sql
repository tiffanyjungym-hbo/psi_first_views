USE SCHEMA workspace;

COPY INTO {stage}/input_percent_view/production_budget_imdb_feature.csv
FROM (
    with base as (
        select distinct
            case when content_category = 'series'
                        then ifnull(asset.season_number, 1)
                        else asset.season_number
                            end as season_number_adj
            , coalesce(concat(series_title_long, ' S', season_number_adj), asset_title_long) as title_name
            , coalesce(concat(asset.series_id, '-', season_number_adj), asset.viewable_id) as match_id
            , content_category
            , amount/1000000 as budget
        from enterprise_data.catalog.imdb_boxoffice_title_budget as b
        join enterprise_data.catalog.wm_catalog as c
            on b.title_id = c.imdb_title_id
        join max_prod.catalog.asset_dim as asset
            on asset.viewable_id = c.viewable_id
        where 1=1
            and asset.asset_type in ('FEATURE', 'ELEMENT')
            and content_category is not null
            and title_name is not null
            and first_offered_date_max is not null
    ),

    final as (
        select distinct
            f.title_name
            , f.match_id
            , f.content_category
            , earliest_offered_timestamp
            , total_production_budget
        from (
                select
                    title_name
                    , match_id
                    , content_category
                    , sum(budget) as total_production_budget
                from base
                group by 1,2,3
            )  as b
        right join {database}.{schema}.title_retail_funnel_metrics as f
            on b.match_id = f.match_id
        where 1=1
        order by earliest_offered_timestamp desc
    )

    select
        title_name
        , match_id
        --, content_category
        --, earliest_offered_timestamp
        , case when total_production_budget is null then -1
            else total_production_budget end as total_production_budget_imdb
    from final
    order by total_production_budget desc
) file_format = (type = 'csv' 
        compression = 'NONE' 
        field_optionally_enclosed_by = '"') single = true OVERWRITE = TRUE header = TRUE;