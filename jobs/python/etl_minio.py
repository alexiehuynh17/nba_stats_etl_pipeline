import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import os
import logging
from minio import Minio
from io_minio import load_minio_data

# Create spark session
# spark = (SparkSession
#          .builder
#          .config("spark.jars", "/opt/airflow/tmps/jars/postgresql-42.7.3.jar,/opt/airflow/tmps/jars/hadoop-aws-3.3.4.jar, /opt/airflow/tmps/jars/aws-java-sdk-bundle-1.12.262.jar, /opt/airflow/tmps/jars/guava-23.0.jar") 
#          .getOrCreate()
#          )
# spark = (SparkSession
#          .builder
#          .config("spark.jars", "/opt/airflow/tmps/jars/postgresql-42.7.3.jar") 
#          .getOrCreate()
#          )

# sc = spark.sparkContext

# # Set the MinIO access key, secret key, endpoint, and other configurations
# sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", os.getenv("MINIO_ROOT_USER"))
# sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD"))
# sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://186.20.0.11:9000")
# sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
# sc._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
# sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
# # sc._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
# sc._jsc.hadoopConfiguration().set('fs.s3.aws.credentials.provider', 'com.amazonaws.auth.EnvironmentVariableCredentialsProvider')

# df = (spark.read
#     .format("csv")
#     .option("header", True)
#     .load(f"s3a://{os.getenv('MINIO_BUCKET_NAME')}/nba_player_information.csv")
#     )

# df.printSchema()


spark = (SparkSession
         .builder
         .config("spark.jars", "/opt/airflow/tmps/jars/postgresql-42.7.3.jar") 
         .getOrCreate()
         )

sc = spark.sparkContext


nba_player_leader  = load_minio_data("nba_player_leader.csv")
nba_team_leader  = load_minio_data("nba_team_leader.csv")

nba_player_information = load_minio_data("nba_player_information.csv")
nba_player_box_scores_rs = load_minio_data("player_box_scores_rs.csv")
nba_team_box_scores_rs = load_minio_data("team_box_scores_rs.csv")

postgres_db = "jdbc:postgresql://postgres:5432/da_nba"
postgres_user = "airflow"
postgres_pwd = "airflow"

print("######################################")
print("EXTRACT CSV FILES")
print("######################################")

df_nba_player_leader = (
    spark.read
    .format("csv")
    .option("header", True)
    .load(nba_player_leader)
)

df_nba_team_leader = (
    spark.read
    .format("csv")
    .option("header", True)
    .load(nba_team_leader)
)

df_nba_player_information = (
    spark.read
    .format("csv")
    .option("header", True)
    .load(nba_player_information)
)


df_nba_player_box_scores_rs = (
    spark.read
    .format("csv")
    .option("header", True)
    .load(nba_player_box_scores_rs)
)

df_nba_team_box_scores_rs = (
    spark.read
    .format("csv")
    .option("header", True)
    .load(nba_team_box_scores_rs)
)

# df_nba_player_leader.printSchema()
# df_nba_player_information.printSchema()
# df_nba_player_box_scores_rs.printSchema()



print("######################################")
print("TRANSFORM CSV FILES")
print("######################################")

spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
tmp_df = df_nba_player_information.withColumn("BIRTHDATE", 
                                              to_date(unix_timestamp(df_nba_player_information["BIRTHDATE"], 
                                                                     "MMMM dd, yyyy").cast("timestamp")))
tmp_df = tmp_df.withColumn("RPG", tmp_df.RPG.cast('float'))
tmp_df = tmp_df.withColumn("PPG", tmp_df.PPG.cast('float'))
tmp_df = tmp_df.withColumn("APG", tmp_df.APG.cast('float'))
tmp_df = tmp_df.withColumn("PIE", tmp_df.PIE.cast('float'))
tmp_df = tmp_df.withColumn("RPG", when(isnull(tmp_df.RPG), 0).otherwise(tmp_df.RPG))
tmp_df = tmp_df.withColumn("PPG", when(isnull(tmp_df.PPG), 0).otherwise(tmp_df.PPG))
tmp_df = tmp_df.withColumn("APG", when(isnull(tmp_df.APG), 0).otherwise(tmp_df.APG))
tmp_df = tmp_df.withColumn("PIE", when(isnull(tmp_df.PIE), 0).otherwise(tmp_df.PIE))


tmp_df = tmp_df.withColumn("EXPERIENCE", regexp_replace("EXPERIENCE", " ?Years?", ""))
tmp_df = tmp_df.withColumn("EXPERIENCE", regexp_replace("EXPERIENCE", " ?Rookie?", "0"))
tmp_df = tmp_df.withColumn("AGE", regexp_replace("AGE", " ?years?", ""))
tmp_df = tmp_df.withColumn("AGE", tmp_df.AGE.cast('int'))
tmp_df = tmp_df.withColumn("HEIGHT", regexp_extract("HEIGHT", r'\((\d+\.\d+)m\)', 1))
tmp_df = tmp_df.withColumn("WEIGHT", regexp_extract("WEIGHT", r'(\d+)kg', 1))
tmp_df = tmp_df.withColumn("HEIGHT", tmp_df.HEIGHT.cast('float'))
tmp_df = tmp_df.withColumn("WEIGHT", tmp_df.WEIGHT.cast('float'))

tmp_df = tmp_df.withColumn("WEIGHT", when(isnull(tmp_df.WEIGHT), 0).otherwise(tmp_df.WEIGHT))
tmp_df = tmp_df.withColumn("HEIGHT", when(isnull(tmp_df.HEIGHT), 0).otherwise(tmp_df.HEIGHT))


tmp_df = tmp_df.withColumn("EXPERIENCE", tmp_df.EXPERIENCE.cast('float'))
tmp_df = tmp_df.withColumn("POSITION", regexp_extract("POSITION", "[^|]+$", 0))
tmp_df = tmp_df.withColumnRenamed("HEIGHT", "HEIGHT(m)")
tmp_df = tmp_df.withColumnRenamed("WEIGHT", "WEIGHT(kg)")
tmp_df = tmp_df.withColumnRenamed("LAST ATTENDED", "LAST_ATTENDED")

ioc_to_country = {
    "USA": "United States"
}

broadcasted_mapping = spark.sparkContext.broadcast(ioc_to_country)

def ioc_code_to_country(ioc_code):
    return broadcasted_mapping.value.get(ioc_code, ioc_code)

ioc_code_to_country_udf = udf(ioc_code_to_country, StringType())

tmp_df  = tmp_df.withColumn("COUNTRY", ioc_code_to_country_udf(tmp_df["COUNTRY"]))

player_df = df_nba_player_leader.select("PLAYER_ID", "PLAYER")
joined_df = tmp_df.join(player_df, on="PLAYER_ID", how="inner")
df_nba_player_information_transform = joined_df.withColumnRenamed("PLAYER", "PLAYER_NAME")
# df_nba_player_information_transform.show()

unique_player_id = df_nba_player_information_transform.select("PLAYER_ID").distinct()
unique_player_ids_list = [row.PLAYER_ID for row in unique_player_id.collect()]

player_box_filtered_df = df_nba_player_box_scores_rs.filter(df_nba_player_box_scores_rs["PLAYER_ID"].isin(unique_player_ids_list))

# player_box_filtered_df.show(10)   

df_with_date = player_box_filtered_df.withColumn("GAME_DATE", 
                                                 to_date(player_box_filtered_df["GAME_DATE"],
                                                          "yyyy-MM-dd"))
tmp_df = df_with_date.withColumn("MIN", df_with_date.MIN.cast('int'))
tmp_df = tmp_df.withColumn("FGM", tmp_df.FGM.cast('int'))
tmp_df = tmp_df.withColumn("FGA", tmp_df.FGA.cast('int'))
tmp_df = tmp_df.withColumn("FG_PCT", tmp_df.FG_PCT.cast('float'))
tmp_df = tmp_df.withColumn("FG3M", tmp_df.FG3M.cast('int'))
tmp_df = tmp_df.withColumn("FG3A", tmp_df.FG3A.cast('int'))
tmp_df = tmp_df.withColumn("FG3_PCT", tmp_df.FG3_PCT.cast('float'))
tmp_df = tmp_df.withColumn("FTM", tmp_df.FTM.cast('int'))
tmp_df = tmp_df.withColumn("FTA", tmp_df.FTA.cast('int'))
tmp_df = tmp_df.withColumn("FT_PCT", tmp_df.FT_PCT.cast('float'))
tmp_df = tmp_df.withColumn("OREB", tmp_df.OREB.cast('int'))
tmp_df = tmp_df.withColumn("DREB", tmp_df.DREB.cast('int'))
tmp_df = tmp_df.withColumn("REB", tmp_df.REB.cast('int'))
tmp_df = tmp_df.withColumn("AST", tmp_df.AST.cast('int'))
tmp_df = tmp_df.withColumn("STL", tmp_df.STL.cast('int'))
tmp_df = tmp_df.withColumn("BLK", tmp_df.BLK.cast('int'))
tmp_df = tmp_df.withColumn("TOV", tmp_df.TOV.cast('int'))
tmp_df = tmp_df.withColumn("PF", tmp_df.PF.cast('int'))
tmp_df = tmp_df.withColumn("PTS", tmp_df.PTS.cast('int'))
tmp_df = tmp_df.withColumn("PLUS_MINUS", tmp_df.PLUS_MINUS.cast('int'))
tmp_df = tmp_df.withColumn("FANTASY_PTS", tmp_df.FANTASY_PTS.cast('int'))
columns_to_drop = ["SEASON_ID", "PLAYER_NAME", "TEAM_NAME", "VIDEO_AVAILABLE"]
df_nba_player_box_scores_rs_transform = tmp_df.drop(*columns_to_drop)
# df_nba_player_box_scores_rs_transform.printSchema()

# df_nba_player_box_scores_rs_transform.show(10)

tmp_df = df_nba_player_leader.withColumn("MIN", df_nba_player_leader.MIN.cast('int'))
tmp_df = tmp_df.withColumn("RANK", tmp_df.RANK.cast('int'))
tmp_df = tmp_df.withColumn("FGM", tmp_df.FGM.cast('int'))
tmp_df = tmp_df.withColumn("FGA", tmp_df.FGA.cast('int'))
tmp_df = tmp_df.withColumn("FG_PCT", tmp_df.FG_PCT.cast('float'))
tmp_df = tmp_df.withColumn("FG3M", tmp_df.FG3M.cast('int'))
tmp_df = tmp_df.withColumn("FG3A", tmp_df.FG3A.cast('int'))
tmp_df = tmp_df.withColumn("FG3_PCT", tmp_df.FG3_PCT.cast('float'))
tmp_df = tmp_df.withColumn("FTM", tmp_df.FTM.cast('int'))
tmp_df = tmp_df.withColumn("FTA", tmp_df.FTA.cast('int'))
tmp_df = tmp_df.withColumn("FT_PCT", tmp_df.FT_PCT.cast('float'))
tmp_df = tmp_df.withColumn("OREB", tmp_df.OREB.cast('int'))
tmp_df = tmp_df.withColumn("DREB", tmp_df.DREB.cast('int'))
tmp_df = tmp_df.withColumn("REB", tmp_df.REB.cast('int'))
tmp_df = tmp_df.withColumn("AST", tmp_df.AST.cast('int'))
tmp_df = tmp_df.withColumn("STL", tmp_df.STL.cast('int'))
tmp_df = tmp_df.withColumn("BLK", tmp_df.BLK.cast('int'))
tmp_df = tmp_df.withColumn("TOV", tmp_df.TOV.cast('int'))
tmp_df = tmp_df.withColumn("PF", tmp_df.PF.cast('int'))
tmp_df = tmp_df.withColumn("PTS", tmp_df.PTS.cast('int'))
tmp_df = tmp_df.withColumn("EFF", tmp_df.EFF.cast('int'))
tmp_df = tmp_df.withColumn("AST_TOV", tmp_df.AST_TOV.cast('float'))
tmp_df = tmp_df.withColumn("STL_TOV", tmp_df.STL_TOV.cast('float'))
columns_to_drop = ["PLAYER", "TEAM"]
df_nba_player_leader_transform = tmp_df.drop(*columns_to_drop)
# df_nba_player_leader_transform.printSchema()


# df_nba_player_leader_transform.show(10)

tmp_df = df_nba_team_leader.withColumn("MIN", df_nba_team_leader.MIN.cast('int'))
tmp_df = tmp_df.withColumn("GP", tmp_df.GP.cast('int'))
tmp_df = tmp_df.withColumn("W", tmp_df.W.cast('int'))
tmp_df = tmp_df.withColumn("L", tmp_df.L.cast('int'))
tmp_df = tmp_df.withColumn("W_PCT", tmp_df.W_PCT.cast('float'))
tmp_df = tmp_df.withColumn("FGM", tmp_df.FGM.cast('int'))
tmp_df = tmp_df.withColumn("FGA", tmp_df.FGA.cast('int'))
tmp_df = tmp_df.withColumn("FG_PCT", tmp_df.FG_PCT.cast('float'))
tmp_df = tmp_df.withColumn("FG3M", tmp_df.FG3M.cast('int'))
tmp_df = tmp_df.withColumn("FG3A", tmp_df.FG3A.cast('int'))
tmp_df = tmp_df.withColumn("FG3_PCT", tmp_df.FG3_PCT.cast('float'))
tmp_df = tmp_df.withColumn("FTM", tmp_df.FTM.cast('int'))
tmp_df = tmp_df.withColumn("FTA", tmp_df.FTA.cast('int'))
tmp_df = tmp_df.withColumn("FT_PCT", tmp_df.FT_PCT.cast('float'))
tmp_df = tmp_df.withColumn("OREB", tmp_df.OREB.cast('int'))
tmp_df = tmp_df.withColumn("DREB", tmp_df.DREB.cast('int'))
tmp_df = tmp_df.withColumn("REB", tmp_df.REB.cast('int'))
tmp_df = tmp_df.withColumn("AST", tmp_df.AST.cast('int'))
tmp_df = tmp_df.withColumn("TOV", tmp_df.TOV.cast('int'))
tmp_df = tmp_df.withColumn("STL", tmp_df.STL.cast('int'))
tmp_df = tmp_df.withColumn("BLK", tmp_df.BLK.cast('int'))
tmp_df = tmp_df.withColumn("BLKA", tmp_df.BLKA.cast('int'))
tmp_df = tmp_df.withColumn("PF", tmp_df.PF.cast('int'))
tmp_df = tmp_df.withColumn("PFD", tmp_df.PFD.cast('int'))
tmp_df = tmp_df.withColumn("PTS", tmp_df.PTS.cast('int'))
tmp_df = tmp_df.withColumn("PLUS_MINUS", tmp_df.PLUS_MINUS.cast('int'))


columns_to_drop = ["GP_RANK", "W_RANK", "L_RANK", "W_PCT_RANK", "MIN_RANK", "FGM_RANK", "FGA_RANK", "FG_PCT_RANK",
                "FG3M_RANK", "FG3A_RANK", "FG3_PCT_RANK", "FTM_RANK", "FTA_RANK", "FT_PCT_RANK", "OREB_RANK",
                "DREB_RANK", "REB_RANK", "AST_RANK", "TOV_RANK", "STL_RANK", "BLK_RANK", "BLKA_RANK", "PF_RANK",
                "PFD_RANK", "PTS_RANK", "PLUS_MINUS_RANK"]

df_nba_team_leader_transform = tmp_df.drop(*columns_to_drop)
# df_nba_team_leader_transform.show(10)


tmp_df = df_nba_team_box_scores_rs.withColumn("GAME_DATE", 
                                                 to_date(df_nba_team_box_scores_rs["GAME_DATE"],
                                                          "yyyy-MM-dd"))
tmp_df = tmp_df.withColumn("MIN", tmp_df.MIN.cast('int'))
tmp_df = tmp_df.withColumn("FGM", tmp_df.FGM.cast('int'))
tmp_df = tmp_df.withColumn("FGA", tmp_df.FGA.cast('int'))
tmp_df = tmp_df.withColumn("FG_PCT", tmp_df.FG_PCT.cast('float'))
tmp_df = tmp_df.withColumn("FG3M", tmp_df.FG3M.cast('int'))
tmp_df = tmp_df.withColumn("FG3A", tmp_df.FG3A.cast('int'))
tmp_df = tmp_df.withColumn("FG3_PCT", tmp_df.FG3_PCT.cast('float'))
tmp_df = tmp_df.withColumn("FTM", tmp_df.FTM.cast('int'))
tmp_df = tmp_df.withColumn("FTA", tmp_df.FTA.cast('int'))
tmp_df = tmp_df.withColumn("FT_PCT", tmp_df.FT_PCT.cast('float'))
tmp_df = tmp_df.withColumn("OREB", tmp_df.OREB.cast('int'))
tmp_df = tmp_df.withColumn("DREB", tmp_df.DREB.cast('int'))
tmp_df = tmp_df.withColumn("REB", tmp_df.REB.cast('int'))
tmp_df = tmp_df.withColumn("AST", tmp_df.AST.cast('int'))
tmp_df = tmp_df.withColumn("STL", tmp_df.STL.cast('int'))
tmp_df = tmp_df.withColumn("BLK", tmp_df.BLK.cast('int'))
tmp_df = tmp_df.withColumn("TOV", tmp_df.TOV.cast('int'))
tmp_df = tmp_df.withColumn("PF", tmp_df.PF.cast('int'))
tmp_df = tmp_df.withColumn("PTS", tmp_df.PTS.cast('int'))
tmp_df = tmp_df.withColumn("PLUS_MINUS", tmp_df.PLUS_MINUS.cast('int'))
columns_to_drop = ["SEASON_ID", "TEAM_NAME", "VIDEO_AVAILABLE"]
df_nba_team_box_scores_rs_transform = tmp_df.drop(*columns_to_drop)

df_nba_team_box_scores_rs_transform.printSchema()


print("######################################")
print("LOADING TO POSTGRES TABLES")
print("######################################")


(
    df_nba_player_information_transform.write
    .format("jdbc")
    .option("url", postgres_db)
    .option("dbtable", "public.nba_player_information")
    .option("user", postgres_user)
    .option("password", postgres_pwd)
    .mode("overwrite")
    .save()
)

(
    df_nba_player_box_scores_rs_transform.write
    .format("jdbc")
    .option("url", postgres_db)
    .option("dbtable", "public.nba_player_box_scores_rs")
    .option("user", postgres_user)
    .option("password", postgres_pwd)
    .mode("overwrite")
    .save()
)

(
    df_nba_player_leader_transform.write
    .format("jdbc")
    .option("url", postgres_db)
    .option("dbtable", "public.nba_player_leader")
    .option("user", postgres_user)
    .option("password", postgres_pwd)
    .mode("overwrite")
    .save()
)

(
    df_nba_team_leader_transform.write
    .format("jdbc")
    .option("url", postgres_db)
    .option("dbtable", "public.nba_team_leader")
    .option("user", postgres_user)
    .option("password", postgres_pwd)
    .mode("overwrite")
    .save()
)

(
    df_nba_team_box_scores_rs_transform.write
    .format("jdbc")
    .option("url", postgres_db)
    .option("dbtable", "public.nba_team_box_scores_rs")
    .option("user", postgres_user)
    .option("password", postgres_pwd)
    .mode("overwrite")
    .save()
)

os.remove(nba_player_leader)
os.remove(nba_team_box_scores_rs)
os.remove(nba_player_information)
os.remove(nba_player_box_scores_rs)
os.remove(nba_team_leader)