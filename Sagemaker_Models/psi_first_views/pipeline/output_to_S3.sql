use schema {schema};

-- current set of predictions
create or replace table {database}.{schema}.cds_predicted_hourspct (
    match_id string not null
    , predicted_hourspct float
    , prediction_date string
    , window_days int
);

-- historical record of predictions
create table if not exists {database}.{schema}.cds_predicted_hourspct_historical (
    match_id string not null
    , predicted_hourspct float
    , prediction_date string
    , window_days int
);

copy into {database}.{schema}.cds_predicted_hourspct
    from(
        select 
              $1
            , $2
            , $3
            , $4
        from {stage}/hourspct/new_hourspct_prediction.csv
        )
    file_format = (type = csv null_if=(''))
    on_error = 'CONTINUE';

copy into {database}.{schema}.cds_predicted_hourspct_historical 
    from(
        select 
              $1
            , $2
            , $3
            , $4
        from {stage}/hourspct/new_hourspct_prediction.csv
        )
    file_format = (type = csv null_if=(''))
    on_error = 'CONTINUE';
