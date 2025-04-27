import pandas as pd
import glob
import os
from pymongo import MongoClient


# MongoDB 연결
client = MongoClient("mongodb+srv://gjs4565:gn91040827@tlogdest.nacbs.mongodb.net/")
db = client["tlog"] # "tlog" db 선택 
destinations_col = db["destinations"]   #"destinations" 컬렉션 선택
tags_col = db["tags"]   # "tags" 컬렉션 선택

csv_folder_path = "/Users/gojungsu/Desktop/pystudy/csvs"

csv_files = glob.glob(os.path.join(csv_folder_path,"*.csv"))

df = pd.read_csv("/workspaces/python/destination.csv") # 여행지 정보 df 변수 저장

for csv_file in csv_files:
    df = pd.read_csv(csv_file)

    bulk_destinations = []
    
    for index, row in df.iterrows():
        tag_names = row["tagNames"].split(",")
        tag_weights = list(map(int,row["weight"].split(",")))
    
        tags = []
        for tag_name, weight in zip(tag_names,tag_weights):
            tag_doc = tags_col.find_one({"name":tag_name}) # mongoDB에서 해당 태그 찾기
            if tag_doc:
                tags.append({"_id":tag_doc["_id"], "weight":weight})
    
        # 여행지 데이터 mongoDB 삽입
        destination_data = {
            "name": row["name"],
            "address": row["address"],
            "city": row["city"],
            "location": {"longitude": row["longitude"], "latitude": row["latitude"]},
            "rating": row["rating"],
            "hasParking": row["hasParking"],
            "petFriendly": row["petFriendly"],
            "tags": tags,
        }
        bulk_destinations.append(destination_data)
    
    # 한 파일당 insert    
    if bulk_destinations:
        destinations_col.insert_many(bulk_destinations)
        
    
print("여행지 데이터 저장 완료")
