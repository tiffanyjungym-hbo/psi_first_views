-- window_start: YYYY-MM-DD string for start of window, inclusive
-- window_end: YYYY-MM-DD string for end of window, exclusive
-- window_days: number of units(days/weeks) in the window
-- database: db of table
-- schema: schema of table

insert into {database}.{schema}.cds_historical_hourspct (
select
    match_id
    ,datediff(day, hbo_offer_date, window_end) as window
    ,view_share_user_prop as score
from
    (
    select
        case when catalog.content_category = 'series'
                then concat(catalog.series_id, '-', ifnull(catalog.season_number, 1))
                else catalog.viewable_id
                end as match_id
        ,100 * ratio_to_report(sum(STREAM_ELAPSED_PLAY_SECONDS)) over () as view_share_user_prop
    FROM
        MAX_PROD.DATASCIENCE_STAGE.USER_STREAM_DETAIL_ENRICHED as user_stream
    JOIN MAX_PROD.CATALOG.ASSET_DIM  AS catalog
        ON (catalog.VIEWABLE_ID = user_stream.VIEWABLE_ID) 
        AND catalog.asset_type = 'FEATURE'
    WHERE
        user_stream.stream_min_timestamp_est >= '{window_start}'
        AND user_stream.stream_min_timestamp_est < '{window_end}'
        AND user_stream.stream_min_timestamp_est >= '2014-10-15'
        AND user_stream.stream_elapsed_play_seconds >= 120
    GROUP BY
        match_id
    ) vote_table
join
    (
    SELECT
        case when content_category = 'series'
            then concat(series_id, '-', ifnull(season_number, 1))
            else viewable_id
            end  as catalog_match_id
        ,mode( case when series_title_long is not null
                    then concat (series_title_long
                                ,' (Season '
                                , ifnull(season_number, 1)
                                ,')')
                    else asset_title_long
                    end
            ) as match_title
        ,to_date(min(first_offered_date_hbo)) AS hbo_offer_date
        ,dateadd(day, {window_days}, hbo_offer_date) AS window_end
    FROM
        MAX_PROD.CATALOG.ASSET_DIM
    WHERE
        asset_type = 'FEATURE'
        and first_offered_date_hbo is not null
        and first_offered_date_hbo <= dateadd(day, 1, to_timestamp_ntz('{window_start}'))
    group by catalog_match_id
    ) catalog_table
    on vote_table.match_id = catalog_table.catalog_match_id
where
    hbo_offer_date = '{window_start}'
    and match_id not in (select match_id from {database}.{schema}.CDS_HISTORICAL_HOURSPCT where window = {window_days})
);
