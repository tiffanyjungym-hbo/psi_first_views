create or replace table {database}.{schema}.title_wikipedia_page_view
    (
          title_name varchar (255) not null
        , date varchar (255) not null
        , days_since_first_offered integer
        , match_id_platform varchar(255)
        , imdb_id varchar(255)
        , earliest_offered_timestamp timestamp
        , total_page_views integer
    ) as (
        with imdb_match_id_mapping as (
            select distinct
                case
                    when f.title_name = 'BARRY LYNDON' then 'Barry Lyndon'
                    when f.title_name = 'In The Heights' then 'In the Heights'
                    else f.title_name end as title_name
                , concat(case when f.platform_name = 'hboNow' then 0
                                else 1 end, '-', f.match_id) as match_id_platform
                , imdb_title_id as imdb_id
                , earliest_offered_timestamp
            from max_prod.catalog.asset_dim as asset
            join {database}.{schema}.title_retail_funnel_metrics as f
                on f.match_id = asset.viewable_id
            join enterprise_data.catalog.wm_catalog as c
                on f.match_id = c.viewable_id
            where 1=1
                and asset.content_category in ('movies', 'special')
                and length(imdb_title_id) in (9,10)
                and earliest_offered_timestamp >= dateadd(day, 63, '2018-01-01')
            union
            select distinct
                f.title_name -- imdb put the seasons of a same series under same page
                , concat(case when f.platform_name = 'hboNow' then 0
                                else 1 end, '-', f.match_id) as match_id_platform
                , imdb_series_id as imdb_id
                , earliest_offered_timestamp
            from max_prod.catalog.asset_dim as asset
            join {database}.{schema}.title_retail_funnel_metrics as f
                on substr(f.match_id, 1, 21) = asset.series_id
            join enterprise_data.catalog.wm_catalog as c
                on asset.viewable_id = c.viewable_id
            where 1=1
                and asset.content_category in ('series')
                and length(imdb_series_id) in (9,10)
                and earliest_offered_timestamp >= dateadd(day, 63, '2018-01-01')
            order by title_name
        ),

        no_dup_titles_imdb_id as (
            select
                  match_id_platform
                , count(distinct imw.imdb_id) as imdb_id_count
            from {database}.ckg.id_mapping_imdb_wikipedia imw
            join imdb_match_id_mapping as map
                on imw.imdb_id = map.imdb_id
            group by 1
            having imdb_id_count < 2
        ),

        no_dup_titles_match_platform_id as (
            select
                  imw.imdb_id
                , count(distinct match_id_platform) as match_platform_id_count
            from {database}.ckg.id_mapping_imdb_wikipedia imw
            join imdb_match_id_mapping as map
                on imw.imdb_id = map.imdb_id
            group by 1
            having match_platform_id_count < 2
        )

        select distinct
            title_name
            , date
            , datediff(day, earliest_offered_timestamp, date) as days_since_first_offered
            , map.match_id_platform
            , imw.imdb_id
            , earliest_offered_timestamp
            , sum(page_views) as total_page_views
        from {database}.ckg.wikipedia_page_views wpv
        join {database}.ckg.id_mapping_imdb_wikipedia imw
            on wpv.wikipedia_article = imw.wikipedia_article
        join imdb_match_id_mapping as map
            on imw.imdb_id = map.imdb_id
        join no_dup_titles_imdb_id as d
            on map.match_id_platform = d.match_id_platform
        join no_dup_titles_match_platform_id as md
            on map.imdb_id = md.imdb_id
        where 1=1
            and date between dateadd(day, -64, earliest_offered_timestamp)
                and dateadd(day, 28, earliest_offered_timestamp)
        group by 1,2,3,4,5,6
        order by title_name, days_since_first_offered
)