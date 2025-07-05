import pandas as pd
import numpy as np
import pycountry
import json

def youtube_transform():

   #function for converting country_code to country_name
   def country_name(code):
      country=pycountry.countries.get(alpha_2=code) or pycountry.countries.get(alpha_3=code)
      return country.name if country else None
   
   with open('/home/ashu/airflow-docker/output/youtube_raw.json','r') as f:
      raw=json.load(f)

   global_youtube_raw=pd.DataFrame(raw)

   global_youtube_raw.replace(["", " ", "None", "none", "NaN", "nan", None], np.nan, inplace=True)

   global_youtube_raw=global_youtube_raw.dropna(how="all")

   global_youtube_raw["likeCount"]=global_youtube_raw["likeCount"].fillna(0)

   global_youtube_raw["viewCount"]=global_youtube_raw["viewCount"].astype(int)
   global_youtube_raw["likeCount"]=global_youtube_raw["likeCount"].astype(int)
   global_youtube_raw["favoriteCount"]=global_youtube_raw["favoriteCount"].astype(int)
   global_youtube_raw["commentCount"]=global_youtube_raw["commentCount"].astype(int)
   global_youtube_raw["published_at"]=pd.to_datetime(global_youtube_raw["published_at"],errors='coerce')
   #print(global_youtube_raw["viewCount"])
   global_youtube_raw["country_name"]=global_youtube_raw["country"].apply(country_name)

   global_youtube_raw.rename(columns={"country": "country_code","continent": "continent_region"}, inplace=True)
   
   video_id_temp=global_youtube_raw.iloc[0:,0:1]

   country_name_temp=global_youtube_raw.iloc[0:,14:]

   regions_temp=global_youtube_raw.iloc[0:,12:14]

   video_details_temp=global_youtube_raw.iloc[0:,1:7]

   video_metrics_temp=global_youtube_raw.iloc[0:,7:11]

   video_id_country_name_temp=pd.concat([video_id_temp,country_name_temp],axis=1)

   video_regions=pd.concat([video_id_country_name_temp,regions_temp],axis=1)

   video_metrics=pd.concat([video_id_country_name_temp,video_metrics_temp],axis=1)

   video_details=pd.concat([video_id_country_name_temp,video_details_temp],axis=1)

   video_tags_temp=global_youtube_raw.iloc[0:,11:12]

   video_tags_df=pd.concat([video_id_temp,video_tags_temp],axis=1)

   video_tags_df=video_tags_df.drop_duplicates(subset='video_id')

   video_tags_raw=video_tags_df.to_dict(orient="records")

   video_tag_list=[]

   for video in video_tags_raw:
    tags=video.get("tags")
    video_id=video.get("video_id")
    if isinstance(tags, list):
        for tag in tags:
            video_tag_list.append({
                "video_id": video_id,
                "tags": tag
            })
    elif video_id==None or video_id=="":
       video_tag_list.append({})
    else:
        video_tag_list.append({
            "video_id": video_id,
            "tags": ""
        })

   tags_table=pd.DataFrame(video_tag_list)

   tags_table.replace(["", " ", "None", "none", "NaN", "nan", None], np.nan, inplace=True)

   tags_table=tags_table.dropna(how="all")

   video_metrics=video_metrics.where(pd.notnull(video_metrics),None)

   video_details=video_details.where(pd.notnull(video_details),None)

   video_regions=video_regions.where(pd.notnull(video_regions),None)

   tags_table=tags_table.where(pd.notnull(tags_table),None)

   video_details["published_at"] = video_details["published_at"].dt.tz_localize(None)

   video_metrics.to_csv('/home/ashu/airflow-docker/output/video_metrics.csv',index=False)
   video_details.to_csv('/home/ashu/airflow-docker/output/video_details.csv',index=False)
   video_regions.to_csv('/home/ashu/airflow-docker/output/video_regions.csv',index=False)
   tags_table.to_csv('/home/ashu/airflow-docker/output/tags_table.csv',index=False)

   print("✅ Transformation complete. CSVs written.")
   return ["/home/ashu/airflow-docker/output/video_metrics.csv",
    "/home/ashu/airflow-docker/output/video_details.csv",
    "/home/ashu/airflow-docker/output/video_regions.csv",
    "/home/ashu/airflow-docker/output/tags_table.csv"]