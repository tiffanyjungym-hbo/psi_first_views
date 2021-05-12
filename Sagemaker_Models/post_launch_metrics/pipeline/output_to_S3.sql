use schema workspace;

-- Last Prediction table
create or replace table title_retail_percent_view_last_prediction (
    PARENT_ID string,
    WM_ID string,
    DAY_PART string,
    DISTINCT_USER_COUNT int,
    TOTAL_COMPLETED_VIEWS int,
    JOBRUNID string,
    TIMESTAMP timestamp
); 

-- Create Temp Table & Load Today's Output
CREATE TEMP TABLE output_weekly_temp(
    PARENT_ID string,
    WM_ID string,
    DAY_PART string,
    DISTINCT_USER_COUNT int,
    TOTAL_COMPLETED_VIEWS int
);

-- Hard Code Temp Table
COPY INTO output_weekly_temp
  FROM(
            SELECT 
                $1:PARENT_ID,
                $1:WM_ID,
                $1:DAY_PART,
                $1:DISTINCT_USER_COUNT,
                $1:TOTAL_COMPLETED_VIEWS
            FROM {stage}/popularity/weekly/output_{perc}/{yr}/{mo}/{dy}/
        )
  file_format = (type = json null_if=(''))
  ON_ERROR = 'CONTINUE';

-- Audit Table-------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS FULL_AUDIT(
    JOBRUNID string,
    JOBNAME string,
    JOB_RUN_DATE date,
    TIMESTAMP timestamp,
    STATUS string
); 

-- Create Temp Table & Load Today's Audit
CREATE temp TABLE FULL_AUDIT_weekly_temp(
    JOBRUNID string,
    JOBNAME string,
    JOB_RUN_DATE date,
    TIMESTAMP timestamp,
    STATUS string
); 

COPY INTO FULL_AUDIT_weekly_temp
  FROM(
           SELECT 
               $1:jobrunid,
               $1:jobname,
               $1:job_run_date,
               $1:timestamp,
               $1:status
           FROM {stage}/popularity/audit_metrics/audit/
        )
  file_format = (type = json null_if=(''))
  ON_ERROR = 'CONTINUE';

INSERT INTO FULL_AUDIT
WITH a AS(
SELECT * FROM FULL_AUDIT_weekly_temp WHERE jobrunid NOT IN (SELECT jobrunid FROM FULL_AUDIT)
)
SELECT * FROM a;