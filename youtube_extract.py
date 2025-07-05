import requests
import json

def youtube_extract():
      
   fresh=[]
   api_key="Your_API_Key"
   continent_regions = {
    "Asia": ["IN", "ID", "JP", "KR", "SG", "TH", "VN", "PH", "MY", "BD", "PK", "LK", "NP"],
    "Europe": ["GB", "DE", "FR", "IT", "ES", "PL", "SE", "NO", "FI", "NL", "BE"],
    "North America": ["US", "CA", "MX"],
    "South America": ["BR", "AR", "CL", "CO", "PE"],
    "Africa": ["ZA", "NG", "EG", "KE", "GH", "DZ", "MA"],
    "Oceania": ["AU", "NZ", "FJ"],
    "Middle East": ["AE", "SA", "IR", "IQ", "IL", "TR"]
    }
      
   for continent in continent_regions:
       for country in continent_regions[continent]:
             url=f"https://www.googleapis.com/youtube/v3/videos?part=snippet,statistics&chart=mostPopular&videoCategoryId=10&regionCode={country}&maxResults=10&key={api_key}"
             response=requests.get(url)
             data=response.json()
             #print(data["items"][6]["snippet"]["thumbnails"])
             try:
               for i in data["items"]:
                  tempo={}
                  tempo["video_id"]=i["id"]
                  tempo["published_at"]=i["snippet"]["publishedAt"]
                  tempo["channel_id"]=i["snippet"]["channelId"]
                  tempo["song_title"]=i["snippet"]["localized"]["title"]
                  tempo["song_description"]=i["snippet"]["localized"]["description"]
                  try:
                    tempo["song_thumbnail"]=i["snippet"]["thumbnails"]["maxres"]["url"]
                  except KeyError:
                    tempo["song_thumbnail"]=i["snippet"]["thumbnails"]["standard"]["url"]
                  tempo["channel_title"]=i["snippet"]["channelTitle"]
                  tempo["viewCount"]=i["statistics"]["viewCount"]
                  try:
                    tempo["likeCount"]=i["statistics"]["likeCount"]
                  except KeyError:
                    tempo["likeCount"]=None
                  tempo["favoriteCount"]=i["statistics"]["favoriteCount"]
                  tempo["commentCount"]=i["statistics"]["commentCount"]
                  try:
                    tempo["tags"]=i["snippet"]["tags"]
                  except KeyError:
                    tempo["tags"]=""
                  tempo["country"]=country
                  tempo["continent"]=continent
                  fresh.append(tempo)
             except KeyError:
                fresh.append({})
   
   with open('/home/ashu/airflow-docker/output/youtube_raw.json','w') as f:
      json.dump(fresh,f)
   print("✅ YouTube data extracted to youtube_raw.json")
   return "/home/ashu/airflow-docker/output/youtube_raw.json"
