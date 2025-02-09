import pandas as pd
from pymongo import MongoClient

# ✅ 1. MongoDB 연결
client = MongoClient("mongodb+srv://gjs4565:gn91040827@tlogdest.nacbs.mongodb.net/")
db = client["tlog"] # "tlog" db 선택 
destinations_col = db["destinations"]   #"destinations" 컬렉션 선택
tags_col = db["tags"]   # "tags" 컬렉션 선택

df = pd.read_csv("/workspaces/python/destination.csv") # 여행지 정보 df 변수 저장

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
    
    destinations_col.insert_one(destination_data)
    
print("여행지 데이터 저장 완료")
