from pymongo import MongoClient, UpdateOne
import firebase_admin
from firebase_admin import credentials
from google.cloud import storage
from PIL import Image
import requests
from io import BytesIO
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
# firebase 초기화
# temp파일에서 그대로 넣을 것 

# MongoDB 연결
# temp 파일에서 그대로 넣을 것

# bulk 업데이트를 위한 리스트
bulk_updates = []
fail_logs = []

def to_webp_and_upload(doc):
    try:
        #imaegUrl 필드에서 외부 이미지 URL 가져와서 다운로드 (10초 타임아웃)
        res = requests.get(doc["imageUrl"], timeout=10)
        #이미지를 메모리에서 바로 열고 .bmp,.png,.jpg 상관없이 .webp 변환을 위해 rgb 포맷으로 통일
        img = Image.open(BytesIO(res.content)).convert("RGB")

        city = doc.get("city", "etc") # None이면 "etc"로 대체됨 
        district = doc.get("district", "etc")
        doc_id = str(doc["_id"]) # _id를 문자열로 변환(파일 이름 사용)

        #.webp 변환
        path = f"images/{city}/{district}/{doc_id}.webp" # 도시/구 폴더 구조
        blob = bucket.blob(path) # Firebase Storage 안의 특정 위치(blob)에 접근할 객체 생성

        buffer = BytesIO() 
        img.save(buffer, format="WEBP", quality=85) # 이미지를 메모리에 .webp로 저장
        buffer.seek(0) # 파일 포인터를 맨 앞으로 되돌려야 업로드가 정상 작동함 
        blob.upload_from_file(buffer, content_type="image/webp") # Firebase Storage에 .webp 파일 업로드
        blob.make_public() # 업로드한 파일을 퍼블릭으로 공개 (프론트에서 바로 접근 가능)

        # 퍼블릭 URL 생성
        encoded_path = urllib.parse.quote(path, safe="") # URL에 들어갈 수 없는 문자들(예: 한글, /)을 인코딩
        # Firebase Storage 파일의 CDN URL 생성 , ?alt=media가 붙어야 실제 이미지 파일로 직접 접근됨 
        public_url = f"https://firebasestorage.googleapis.com/v0/b/{bucket.name}/o/{encoded_path}?alt=media"
       
        # 업데이트용 쿼리 반환
        print(f"{doc['name']} 완료")
        return UpdateOne({ "_id": doc["_id"] }, { "$set": { "imageUrl": public_url } })

    except Exception as e:
        print(f"{doc.get('name', '이름없음')} 실패: {e}")
        fail_logs.append((doc.get("name", "이름없음"), doc["imageUrl"], str(e)))
        return None

# 아직 처리되지 않은 항목만 필터링 (imageUrl이 외부 주소일 때만)
unprocessed_docs = list(destinations_col.find({
    "imageUrl": { "$regex": "^http://" }
}))

# 병렬 처리 (5~10개)
with ThreadPoolExecutor(max_workers=6) as executor:
    futures = [executor.submit(to_webp_and_upload, doc) for doc in unprocessed_docs]

    for future in as_completed(futures):
        update_op = future.result()
        if update_op:
            bulk_updates.append(update_op)

# 한 번에 업데이트
if bulk_updates:
    result = destinations_col.bulk_write(bulk_updates)
    print(f"MongoDB 업데이트 완료: {result.modified_count}건 변경됨")
    
if fail_logs:
    with open("upload_failures.log", "w") as f:
        for name, url, error in fail_logs:
            f.write(f"{name}\t{url}\t{error}\n")
    print(f"실패 항목 {len(fail_logs)}건 기록됨: upload_failures.log")    