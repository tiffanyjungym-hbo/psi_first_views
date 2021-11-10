use schema {schema};

-- Create table for the metric
create or replace table {database}.{schema}.pct_actives_metric_values (
      match_id string
    , title string not null
    , days_on_hbo_max int
    , pct_actives float
);

copy into {database}.{schema}.pct_actives_metric_values
    from(
        select
              $1
            , $2
            , $3
            , $4
        from {stage}/pct_actives_prediction/pct_actives_metric_values.csv
        )
    file_format = (type = csv null_if=(''))
    on_error = 'CONTINUE';
