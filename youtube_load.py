import pandas as pd
import pymysql
import numpy as np

def youtube_load():

  conn = pymysql.connect(host='hostname',port=port_id,user='DB_user_id',password='DB_pass',database='global_youtube_music_details',charset='utf8mb4')

  def insert_database(query,data_to_insert):
     try:
       with conn.cursor() as cur:
         cur.executemany(query, data_to_insert)
         conn.commit()
       print(f"Inserted {cur.rowcount} rows successfully")
     except Exception as e:
       print("Error:", e)
       return []

  def fetch_data(query, data=None):
     try:
        with conn.cursor() as cur:
            cur.execute(query, data)
            rows=cur.fetchall()
            return rows
     except Exception as e:
        print("Error:", e)
        return []

  video_metrics=pd.read_csv('/home/ashu/airflow-docker/output/video_metrics.csv')
  video_details=pd.read_csv('/home/ashu/airflow-docker/output/video_details.csv')
  video_regions=pd.read_csv('/home/ashu/airflow-docker/output/video_regions.csv')
  tags_table=pd.read_csv('/home/ashu/airflow-docker/output/tags_table.csv')

  video_details_copy=video_details.copy()

  video_details_copy=video_details_copy.drop('country_name',axis=1)

  video_details_copy=video_details_copy.drop_duplicates(subset='video_id')

  #inserting video_details df
  insert_query="INSERT INTO video_details (video_id,published_at,channel_id,song_title,song_description,song_thumbnail,channel_title) VALUES (%s, %s, %s, %s, %s, %s, %s)"

  data_to_insert=list(video_details_copy[['video_id','published_at','channel_id','song_title',
    'song_description','song_thumbnail','channel_title']].itertuples(index=False, name=None))

  insert_database(insert_query,data_to_insert)

  #inserting video_regions df

  fetch_query="SELECT video_id, video_details_id FROM video_details"

  rows=fetch_data(fetch_query)

  video_id_pk_df=pd.DataFrame(rows,columns=["video_id","video_details_id"])

  video_regions_copy=video_regions.copy()

  video_regions_copy=video_regions_copy.merge(video_id_pk_df,on='video_id',how='left')

  video_regions_copy=video_regions_copy.drop("video_id",axis=1)

  video_regions_copy=video_regions_copy.dropna(subset=["video_details_id"])

  video_regions_copy["video_details_id"]=video_regions_copy["video_details_id"].astype("Int64")

  insert_query_two="INSERT INTO video_regions(country_name,country_code,continent_region,video_details_id) VALUES (%s, %s, %s, %s)"
  
  data_to_insert_two=list(video_regions_copy[['country_name','country_code','continent_region','video_details_id']].itertuples(index=False, name=None))

  insert_database(insert_query_two,data_to_insert_two)

  #map primary_key to the respictive video_id

  tags_table_copy=tags_table.copy()

  tags_table_copy=tags_table_copy.merge(video_id_pk_df,on='video_id',how='left')

  tags_table_copy.replace([np.nan], None, inplace=True)

  tags_table_copy=tags_table_copy.drop("video_id",axis=1)

  tags_table_copy=tags_table_copy.dropna(subset=["video_details_id"])

  tags_table_copy["video_details_id"]=tags_table_copy["video_details_id"].astype("Int64")

  #datdframe insertion to database
  insert_query_three="INSERT INTO tags_table(tag, video_details_id) VALUES (%s, %s)"

  data_to_insert_three=list(tags_table_copy[['tags','video_details_id']].itertuples(index=False, name=None))

  insert_database(insert_query_three,data_to_insert_three)

  video_metrics_copy=video_metrics.copy()

  video_metrics_copy=video_metrics_copy.drop('country_name',axis=1)

  video_metrics_copy=video_metrics_copy.drop_duplicates(subset='video_id')

  video_metrics_copy=video_metrics_copy.merge(video_id_pk_df,on='video_id',how='inner')

  video_metrics_copy=video_metrics_copy.dropna(subset=["video_details_id"])

  video_metrics_copy["video_details_id"]=video_metrics_copy["video_details_id"].astype("Int64")

  fetch_query_regions="SELECT video_region_id, video_details_id FROM video_regions"

  rows_regions=fetch_data(fetch_query_regions)

  video_id_pk_regions=pd.DataFrame(rows_regions,columns=["video_region_id","video_details_id"])

  video_metrics_copy=video_metrics_copy.merge(video_id_pk_regions,on="video_details_id",how='left')

  video_metrics_copy=video_metrics_copy.dropna(subset=["video_region_id"])

  video_metrics_copy["video_region_id"]=video_metrics_copy["video_region_id"].astype("Int64")

  video_metrics_copy=video_metrics_copy.drop("video_id",axis=1)

  video_metrics_copy.rename(columns={"viewCount": "view_count","likeCount": "like_count","favoriteCount":"favourite_count","commentCount":"comment_count"}, inplace=True)

  insert_query_four="INSERT INTO video_metrics(view_count,like_count,favourite_count,comment_count,video_details_id,video_region_id) VALUES (%s, %s, %s, %s, %s, %s)"

  data_to_insert_four=list(video_metrics_copy[['view_count','like_count','favourite_count','comment_count','video_details_id','video_region_id']].itertuples(index=False, name=None))

  insert_database(insert_query_four,data_to_insert_four)

  conn.close()

  print("All inserted")
