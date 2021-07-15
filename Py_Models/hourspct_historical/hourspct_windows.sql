-- run_date: YYYY-MM-DD string for run date
-- window_days: number of days as length of window
-- database: db of table
-- schema: schema of table

WITH series_agg as (
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
        ,(CASE WHEN to_date(min(first_offered_date_hbo)) < to_timestamp_ntz('2014-10-15') THEN to_timestamp_ntz('2014-10-15')
               ELSE to_date(min(first_offered_date_hbo)) END) AS hbo_offer_date
        ,dateadd(day, {window_days}, hbo_offer_date) AS window_end
    FROM
        MAX_PROD.CATALOG.ASSET_DIM
    WHERE
        asset_type = 'FEATURE'
        and first_offered_date_hbo is not null
    group by catalog_match_id
)
SELECT *
FROM series_agg
WHERE window_end < '{run_date}'
AND hbo_offer_date > (select max(hbo_offer_date) from {database}.{schema}.CDS_HISTORICAL_HOURSPCT a join series_agg b on a.match_id=b.catalog_match_id where window = {window_days});