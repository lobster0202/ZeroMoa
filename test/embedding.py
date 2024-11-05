import os
from dotenv import load_dotenv
from openai import OpenAI
import pandas as pd
from glob import glob
import numpy as np

load_dotenv()

embedding_api_key = os.getenv("UPSTAGE_API_KEY")

embedding_client = OpenAI(
    api_key=embedding_api_key,
    base_url="https://api.upstage.ai/v1/solar"
)

# crawl_data 폴더의 모든 CSV 파일 경로 가져오기
file_paths = glob('crawl_data/*.csv')

def get_embedding(text):
    try:
        response = embedding_client.embeddings.create(
            model="solar-embedding-1-large-passage",
            input=text
        )
        return response.data[0].embedding
    except Exception as e:
        print(f"임베딩 생성 중 오류 발생: {e}")
        return None

for file_path in file_paths:
    print(f"\n파일 '{file_path}' 처리 시작")
    
    try:
        df = pd.read_csv(file_path, encoding='utf-8')
        
        if 'Spec' not in df.columns:
            print(f"'{file_path}'에 'Spec' 컬럼이 없습니다.")
            continue
            
        print("임베딩 생성 중...")
        embeddings = []
        for spec in df['Spec']:
            if pd.isna(spec):
                embeddings.append(None)
            else:
                embedding = get_embedding(str(spec))
                embeddings.append(embedding)
        
        # 임베딩을 DataFrame에 추가
        df['embedding'] = embeddings
        
        # 기존 파일 덮어쓰기
        df.to_csv(file_path, index=False)
        print(f"임베딩 완료: {file_path}")
        
    except Exception as e:
        print(f"파일 처리 중 오류 발생: {e}")
        continue

print("\n모든 파일 처리 완료")

    