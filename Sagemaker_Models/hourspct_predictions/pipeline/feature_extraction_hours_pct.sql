USE SCHEMA workspace;

COPY INTO {stage}/hourspct/hourspct_feature.csv
FROM (
    select * 
    from max_dev.workspace.cds_historical_hourspct
    PIVOT(max(hourspct) for window in (7,14,21,28,56,91,182,364))
        AS p (match_id, hourspct7d, hourspct14d,hourspct21d,hourspct28d,hourspct8w,hourspct13w,hourspct26w,hourspct52w)
) file_format = (type='csv' compression = 'NONE') single = true OVERWRITE = TRUE header = TRUE;
