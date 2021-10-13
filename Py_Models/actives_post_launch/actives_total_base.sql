-- date: YYYY-MM-DD string for start of window
-- database: db of table
-- schema: schema of table

insert into {database}.{schema}.actives_base_first_view (
        WITH date AS
            (
            select seq_date as date
            from "MAX_PROD"."STAGING"."DATE_RANGE"
            where seq_date <= CURRENT_DATE()
            and seq_date >= '2020-05-27'
            and seq_date = '{date}'
            ),

         denom_first_viewed as (
              select
                hbo_uuid,
                dr.date as start_date,
                min(LOCAL_REQUEST_DATE) as request_date
                from viewership.max_user_stream_distinct_user_dates_heartbeat udh
                join date dr ON udh.LOCAL_REQUEST_DATE between dr.date and dateadd(day, 28, dr.date)
                group by 1,2
              ),

          denom_subs_count as (
                select
                    start_date, request_date as end_date,
                    count(hbo_uuid) as subs_count
              from denom_first_viewed
               group by 1,2
            )

            select
              start_date, end_date,
              subs_count as daily_viewing_subs_denom,
              sum(subs_count) over(partition by start_date order by end_date rows between unbounded preceding and current row) as cumulative_viewing_subs_denom
            from denom_subs_count
            group by 1, 2, 3