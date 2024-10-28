import pandas as pd
import re
import os
from typing import Tuple, Set, Optional
import sqlite3

def get_category_hierarchy():
    # 카테고리 계층 구조 정의
    category_hierarchy = {
        '음료': {
            'keywords': ['음료', '드링크', '워터', '베버리지'],
            'subcategories': {
                '탄산': ['탄산음료', '사이다', '콜라', '탄산수', '환타', '스파클링', '소다'],
                '주스': ['주스', '과채주스', '과일주스', '과채음료', '착즙', 'ABC주스'],
                '유제품': ['우유', '두유', '요구르트', '밀크', '두유', '치즈', '연유'],
                '차': ['녹차', '홍차', '보이차', '우롱차', '캐모마일', '페퍼민트'],
                '커피': ['커피', '아메리카노', '카페라떼', '에스프레소', '카푸치노'],
                '에너지 드링크': ['에너지드링크', '레드불', '핫식스', '몬스터'],
                '주류': ['와인', '맥주', '소주', '위스키', '진', '보드카', '사케'],
                '기타': ['비타민음료', '이온음료', '식혜', '수정과', '곡물음료']
            }
        },
        '제과': {
            'keywords': ['제과', '과자류', '스낵류'],
            'subcategories': {
                '과자': ['과자', '스낵', '비스킷', '크래커', '새우깡', '감자칩'],
                '사탕': ['사탕', '캔디', '카라멜', '롤리팝', '드롭스', '민트'],
                '젤리': ['젤리', '구미', '마시멜로', '젤리빈'],
                '초콜릿': ['초콜릿', '초코', '핫초코', '가나초', '트러플'],
                '떡': ['떡', '떡류', '인절미', '찰떡', '약과', '한과'],
                '시리얼': ['시리얼', '그래놀라', '뮤즐리', '콘푸로스트'],
                '빵': ['빵', '베이커리', '크로와상', '베이글', '식빵'],
                '기타': ['잼', '스프레드', '시럽', '토핑']
            }
        },
        '아이스크림': {
            'keywords': ['아이스크림', '아이스', '빙과'],
            'subcategories': {
                '바': ['바', '바(막대)', '아이스바', '스크류바', '죠스바', '메로나'],
                '샌드': ['샌드', '아이스크림샌드', '시모나', '모나카', '와플샌드'],
                '컵': ['컵', '파인트', '하프갤런', '설레임', '체리마루'],
                '기타': ['콘', '튜브', '젤라또', '소프트아이스크림', '빙수']
            }
        }
    }
    return category_hierarchy

def extract_serving_size(text: str) -> Tuple[Optional[int], Optional[str]]:
    if pd.isna(text) or '[영양정보]' not in str(text):
        return None, None
    
    pattern = r'1회 제공량\s*(\d+)\s*(ml|g)'
    match = re.search(pattern, str(text))
    
    if match:
        amount = int(match.group(1))
        unit = match.group(2)
        return amount, unit
    return None, None

def categorize_text(text: str, category_hierarchy: dict) -> Set[str]:
    categories = set()
    
    for main_category, data in category_hierarchy.items():
        if any(keyword.lower() in text.lower() for keyword in data['keywords']):
            categories.add(main_category)
        
        sub_parts = re.findall(r'\((.*?)\)', text)
        
        for sub_part in sub_parts:
            if sub_part:
                sub_part = sub_part.lower()
                if any(keyword.lower() in sub_part for keyword in data['keywords']):
                    categories.add(main_category)
                
                for sub_category, keywords in data['subcategories'].items():
                    if any(keyword.lower() in sub_part for keyword in keywords):
                        categories.add(main_category)
                        categories.add(sub_category)
    
    return categories

def create_database():
    conn = sqlite3.connect('products.db')
    cursor = conn.cursor()
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS products (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        serving_amount INTEGER,
        serving_unit TEXT,
        category TEXT
    )
    ''')
    
    conn.commit()
    return conn

def process_files():
    conn = create_database()
    cursor = conn.cursor()
    category_hierarchy = get_category_hierarchy()
    
    folder_path = "crawl_data"
    for file_name in os.listdir(folder_path):
        if file_name.endswith('.csv'):
            try:
                file_path = os.path.join(folder_path, file_name)
                df = pd.read_csv(file_path)
                
                for _, row in df.iterrows():
                    name = row['Name']
                    spec = str(row['Spec'])
                    
                    # 1회 제공량 추출
                    serving_amount, serving_unit = extract_serving_size(spec)
                    
                    # 카테고리 추출
                    categories = categorize_text(spec, category_hierarchy)
                    
                    # 데이터베이스에 저장
                    for category in categories:
                        cursor.execute('''
                        INSERT INTO products (name, serving_amount, serving_unit, category)
                        VALUES (?, ?, ?, ?)
                        ''', (name, serving_amount, serving_unit, category))
                
                conn.commit()
                print(f"파일 처리 완료: {file_name}")
                
            except Exception as e:
                print(f"파일 처리 중 오류 발생 ({file_name}): {e}")
    
    # 처리 결과 출력
    cursor.execute('''
    SELECT category, COUNT(*) as count, 
           COUNT(serving_amount) as serving_info_count
    FROM products 
    GROUP BY category
    ''')
    
    print("\n=== 카테고리별 통계 ===")
    for category, count, serving_count in cursor.fetchall():
        print(f"- {category}: 총 {count}개 제품 (1회 제공량 정보 있음: {serving_count}개)")
    
    conn.close()

if __name__ == "__main__":
    process_files()
