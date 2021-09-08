USE SCHEMA workspace;

COPY INTO {stage}/input_percent_view/production_budget_imdb_feature.csv
FROM (
    with base as (
        select distinct
            case when content_category = 'series'
                        then ifnull(asset.season_number, 1)
                        else asset.season_number
                            end as season_number_adj
            , coalesce(concat(asset.series_id, '-', season_number_adj), asset.viewable_id) as match_id
            , asset.viewable_id
            , max(amount/1000000) as budget
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
            and asset.viewable_id is not null
        -- group by in viewable_id level since same viewable_id could have more than 1 imdb id
        group by 1, 2, 3
    ),

    total_budget as (
        select distinct
              title_name
            , concat(case when f.platform_name = 'hboNow' then 0
                    else 1 end, '-', f.match_id) as match_id_platform
            --, content_category
            --, earliest_offered_timestamp
            , total_production_budget
        from (
                select
                      match_id
                    , sum(budget) as total_production_budget
                from base
                group by 1
            )  as b
        right join {database}.{schema}.title_retail_funnel_metrics as f
            on b.match_id = f.match_id
        where 1=1
    ),

    final as (
        select
            match_id_platform
            , case when total_production_budget is null then -1
                else total_production_budget end as total_production_budget_imdb
        from total_budget)

    select *
    from final
    order by total_production_budget_imdb desc
) file_format = (type = 'csv' 
        compression = 'NONE' 
        field_optionally_enclosed_by = '"') single = true OVERWRITE = TRUE header = TRUE;