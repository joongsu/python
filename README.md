# Tlog 여행지 데이터 처리 스크립트

이 스크립트는 MongoDB에 저장된 여행지 이미지들을 Firebase Storage로 업로드하고, CSV로 정리된 여행지 정보를 MongoDB에 저장하는 자동화 파이프라인입니다.

---
1. 여행지 CSV 데이터를 MongoDB에 저장
특정 경로 내부의 .csv 파일 모두 읽어 destinations 컬렉션에 저장

2. 외부 이미지 .webp 변환 및 Firebase 업로드(to_webp_and_upload)
- imageUrl 필드가 외부 Url (http://) 인 것을 찾아 .webp 포멧으로 변환
- Firebase Storage에 업로드 (도시/구 폴더 구조)
imageUrl 을 Firebase 퍼블릭 Url로 갱신 