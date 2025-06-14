import pandas as pd
import glob
import os
from pymongo import MongoClient


# MongoDB 연결
# 이 자리에 temp 파일에 있는 코드 넣을 것

db = client["tlog"] # "tlog" db 선택 
destinations_col = db["destinations"]   #"destinations" 컬렉션 선택
tags_col = db["tags"]   # "tags" 컬렉션 선택

csv_folder_path = "/Users/gojungsu/Desktop/pystudy/csvs"

csv_files = glob.glob(os.path.join(csv_folder_path,"*.csv"))


for csv_file in csv_files:
    df = pd.read_csv(csv_file)

    bulk_destinations = []
    
    for index, row in df.iterrows():
        tag_names_raw = row["tagNames"]

        if isinstance(tag_names_raw, str) and "," in tag_names_raw:
            tag_names = tag_names_raw.split(",")
        else:
            tag_names = [tag_names_raw]

        weight = row["weight"]
        if pd.isna(weight):
            tag_weights = []
        elif isinstance(weight, str) and "," in weight:
            tag_weights = list(map(int, weight.split(",")))
        else:
            tag_weights = [int(weight)]
    
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
            "district": row["district"],
            "location": {"longitude": row["longitude"], "latitude": row["latitude"]},
            "ratingSum": row.get("ratingSum", 0),
            "reviewCount": row.get("reviewCount", 0),
            "averageRating": row.get("averageRating", 0.0),
            "imageUrl": row["image"],
            "description": row.get("description", ""),
            "hasParking": row["hasParking"],
            "petFriendly": row["petFriendly"],
            "tags": tags,
        }
        bulk_destinations.append(destination_data)
    
    # 한 파일당 insert    
    if bulk_destinations:
        destinations_col.insert_many(bulk_destinations)
        
    
print("여행지 데이터 저장 완료")
