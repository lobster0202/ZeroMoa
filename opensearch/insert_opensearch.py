import os
import pandas as pd
from dotenv import load_dotenv
from opensearchpy import OpenSearch
from create_opensearch import CreateOpensearch
from glob import glob
import urllib3
import ast
import time
from datetime import datetime
import logging
import json
from opensearchpy.helpers import bulk
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 로그 설정
log_dir = 'logs'
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

log_file = os.path.join(log_dir, f'error_log_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
logging.basicConfig(
    level=logging.INFO,  # INFO 레벨로 변경하여 성공적인 인덱싱도 기록
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        # logging.StreamHandler()  # 콘솔에도 출력
    ]
)
logger = logging.getLogger(__name__)

# 전체 작업 시작 시간
total_start_time = time.time()
start_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
logger.info(f"작업 시작 시간: {start_datetime}")

# 환경 변수 로드
load_dotenv()
host = "localhost"
OPENSEARCH_KEY = os.getenv("OPENSEARCH_KEY")

# OpenSearch 클라이언트 설정
client = OpenSearch(
    hosts=[{'host': host, 'port': 9200}],
    http_auth=('admin', OPENSEARCH_KEY),
    use_ssl=True,
    verify_certs=False,
    timeout=30,
    max_retries=10,
    retry_on_timeout=True
)

# 인덱스 이름 통일
INDEX_NAME = "product"
create_opensearch = CreateOpensearch()
create_opensearch.create_index(client, INDEX_NAME)

# crawl_data 폴더의 모든 CSV 파일 경로 가져오기
file_paths = glob(r'C:\dev\ZeroMoa\ZeroMoa\crawl_data\*.csv')  # 절대 경로로 수정

if not file_paths:
    print("처리할 CSV 파일을 찾을 수 없습니다.")
    exit()

print(f"\n총 {len(file_paths)}개의 CSV 파일이 발견되었습니다.")
print("\n처리할 파일 목록:")
for i, file_path in enumerate(file_paths, 1):
    print(f"{i}. {os.path.basename(file_path)}")
print("\n")

# 현재 인덱스의 마지막 문서 번호 확인
try:
    search_response = client.search(
        index=INDEX_NAME,
        body={
            "sort": [{"product_no": "desc"}],
            "size": 1
        }
    )
    current_id = search_response['hits']['hits'][0]['_source']['product_no'] + 1 if search_response['hits']['hits'] else 1
except Exception as e:
    print(f"마지막 문서 번호 확인 중 오류 발생: {str(e)}")
    current_id = 1

print(f"시작 문서 번호: {current_id}")

# 처리 완료된 파일 목록을 저장할 리스트
completed_files = []

for file_path in file_paths:
    file_start_time = time.time()
    print(f"\n파일 '{os.path.basename(file_path)}' 처리 시작")
    
    try:
        df = pd.read_csv(file_path, encoding='utf-8')
        print(f"파일 '{os.path.basename(file_path)}'에서 {len(df)} 개의 행을 읽었습니다.")
        
        # 벌크 작업을 위한 문서 리스트
        bulk_docs = []
        
        for idx, row in df.iterrows():
            try:
                document = {
                    "_index": INDEX_NAME,
                    "_id": str(current_id),
                    "_source": {
                        "product_no": current_id,
                        "product_name": row['Name'],
                        "product_spec": row['Spec'] if pd.notna(row['Spec']) else "",
                        "spec_emb": None
                    }
                }

                if 'embedding' in df.columns and not pd.isna(row['embedding']):
                    try:
                        embedding_data = json.loads(row['embedding'])
                        if isinstance(embedding_data, list) and \
                           all(isinstance(x, (int, float)) for x in embedding_data) and \
                           len(embedding_data) == 4096:
                            document['_source']['spec_emb'] = embedding_data
                    except Exception as e:
                        logger.error(f"파일: {file_path}, 행 {idx}: embedding 변환 실패 - {str(e)}")

                bulk_docs.append(document)
                current_id += 1

                # 2500개 문서마다 벌크 삽입 실행
                if len(bulk_docs) >= 2500:
                    success, failed = bulk(client, bulk_docs)
                    logger.info(f"{success}개 문서 벌크 인덱싱 완료")
                    bulk_docs = []

            except Exception as e:
                logger.error(f"문서 준비 중 오류 발생: {str(e)}")

        # 남은 문서들 처리
        if bulk_docs:
            success, failed = bulk(client, bulk_docs)
            logger.info(f"{success}개 문서 벌크 인덱싱 완료")

        completed_files.append(os.path.basename(file_path))

    except Exception as e:
        logger.error(f"파일 처리 중 오류 발생: {str(e)}")

    file_end_time = time.time()
    file_elapsed_time = file_end_time - file_start_time
    print(f"파일 처리 완료 (소요 시간: {file_elapsed_time:.2f}초)")

# 전체 작업 종료 시간과 요약
total_end_time = time.time()
end_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
total_elapsed_time = total_end_time - total_start_time

# 분과 초로 변환
total_minutes = int(total_elapsed_time // 60)  # 총 분
total_seconds = int(total_elapsed_time % 60)   # 남은 초

summary = (
    "\n=== 작업 완료 요약 ===\n"
    f"시작 시간: {start_datetime}\n"
    f"종료 시간: {end_datetime}\n"
    f"총 소요 시간: {total_minutes}분 {total_seconds}초\n\n"
    f"처리 완료된 파일 목록:\n"
)
for i, file_name in enumerate(completed_files, 1):
    summary += f"{i}. {file_name}\n"

logger.info(summary)
print(summary)

# 최종 문서 수 확인
try:
    count = client.count(index=INDEX_NAME)
    final_msg = f"총 {count['count']}개의 문서가 {INDEX_NAME} 인덱스에 저장되었습니다."
    logger.info(final_msg)
    print(final_msg)
except Exception as e:
    error_msg = f"문서 수 확인 중 오류 발생: {str(e)}"
    logger.error(error_msg)
    print(error_msg)