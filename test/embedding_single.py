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
file = "crawl_data/정제_중복제거_프로폴리스_꿀.csv"

# 파일 존재 확인
if not os.path.exists(file):
    print(f"파일을 찾을 수 없습니다: {file}")
    exit()

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


print(f"\n파일 '{file}' 처리 시작")

try:
    df = pd.read_csv(file, encoding='utf-8')
    
    if 'Spec' not in df.columns:
        print(f"'{file}'에 'Spec' 컬럼이 없습니다.")
        exit()
    
    print("임베딩 생성 중...")
    embeddings = []
    for idx, spec in enumerate(df['Spec']):
        if pd.isna(spec):
            embeddings.append(None)
        else:
            # 이미 임베딩이 있는지 확인
            if 'embedding' in df.columns and not pd.isna(df.loc[idx, 'embedding']):
                embeddings.append(eval(df.loc[idx, 'embedding']))
                print(f"기존 임베딩 사용 - 행 {idx+1}")
            else:
                embedding = get_embedding(str(spec))
                embeddings.append(embedding)
                print(f"새 임베딩 생성 - 행 {idx+1}")
    
    # 임베딩을 DataFrame에 추가
    df['embedding'] = embeddings
    
    # 기존 파일 덮어쓰기
    df.to_csv(file, index=False)
    print(f"임베딩 완료: {file}")
    
except Exception as e:
    print(f"파일 처리 중 오류 발생: {e}")
    

print("\n모든 파일 처리 완료")

    