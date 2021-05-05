"""
"""
import pandas as pd

from io import StringIO

df_clean_and_generate_tags_based_on_genres = pd.read_csv(
    StringIO("""
        TITLE_NAME,MATCH_ID,SERIES_ID,SEASON_NUMBER,CONTENT_CATEGORY,SINGLE_EPISODE_IND,PROGRAM_TYPE,EARLIEST_OFFERED_TIMESTAMP,EARLIEST_PUBLIC_TIMESTAMP,LATEST_OFFERED_TIMESTAMP,IN_SEQUANTIAL_RELEASING_PERIOD,FIRST_DEBUT,DAY01_PERCENT_VIEWED,DAY02_PERCENT_VIEWED,DAY03_PERCENT_VIEWED,DAY05_PERCENT_VIEWED,DAY07_PERCENT_VIEWED,DAY14_PERCENT_VIEWED,DAY21_PERCENT_VIEWED,DESCRIPTIVE_GENRE_DESC_AGG,WM_ENTERPRISE_GENRES_AGG,SCRIPTED_FLAG,SPORTS_FLAG,KIDS_FLAG,INTERNATIONAL_FLAG,LATINO_FLAG,LICENSOR_AGG,TARGET_DAY28_PERCENT_VIEWED,DAYOFWEEK_EARLIEST_DATE,MONTH_EARLIEST_DATE,DAY01_PERCENT_VIEWED_LAST_SEASON,DAY02_PERCENT_VIEWED_LAST_SEASON,DAY03_PERCENT_VIEWED_LAST_SEASON,DAY05_PERCENT_VIEWED_LAST_SEASON,DAY07_PERCENT_VIEWED_LAST_SEASON,DAY14_PERCENT_VIEWED_LAST_SEASON,DAY21_PERCENT_VIEWED_LAST_SEASON
Friends S2,GXdbR_gOXWJuAuwEAACVH-2,GXdbR_gOXWJuAuwEAACVH,2,series,0,acquired,2020-05-27 07:00:00.000000000,2020-05-27 07:00:00.000000000,2020-05-27 07:00:00.000000000,0,1,0.017941,0.033436,0.041857,0.053933,0.056383,0.054825,0.049166,comedy|teens,comedy | romance,1,0,1,0,0,"turner entertainment networks, inc., warner bros. domestic television distribution",0.043866,Wed,5,0.179311,0.171849,0.162898,0.157564,0.138957,0.107294,0.087771
        """)
)
