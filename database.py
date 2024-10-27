import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
import os
import re
from typing import Tuple, Set, Optional

def get_db_connection():
    return psycopg2.connect(
        user="zeromoa",
        password="root",
        host="localhost",
        port="5432"
    )

def get_category_hierarchy():
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

def get_category_no(category_name, cursor):
    try:
        cursor.execute(
            "SELECT category_no FROM product_category WHERE category_name = %s",
            (category_name,)
        )
        result = cursor.fetchone()
        return result[0] if result else None
    except Exception as e:
        print(f"카테고리 번호 조회 실패 ({category_name}): {str(e)}")
        return None

def extract_nutrition_info(description: str) -> dict:
    nutrition_info = {}
    if "[영양정보]" in description:
        try:
            nutrition_text = description.split("[영양정보]")[1].split("/")
            for item in nutrition_text:
                if ":" in item:
                    try:
                        # maxsplit=1을 사용하여 첫 번째 콜론만 기준으로 분리
                        parts = item.split(":", 1)
                        if len(parts) == 2:
                            key, value = parts
                            key = key.strip()
                            value = value.strip()
                            if "kcal" in value.lower():
                                # 쉼표 제거 및 숫자만 추출
                                kcal_match = re.search(r'(\d+(?:,\d+)?)', value)
                                if kcal_match:
                                    kcal_str = kcal_match.group(1).replace(',', '')
                                    nutrition_info["energy_kcal"] = float(kcal_str)
                    except ValueError:
                        continue  # 개별 항목 파싱 실패 시 다음 항목으로
        except Exception as e:
            print(f"영양정보 추출 중 오류: {str(e)}")
    return nutrition_info

def process_product_data(row, cursor, category_hierarchy):
    try:
        product_name = row['Name']
        description = str(row['Spec'])
        image_path = row['ImageURL']
        
        # 카테고리 추출
        categories = categorize_text(description, category_hierarchy)
        category_no = None
        if categories:
            category_name = list(categories)[0]
            category_no = get_category_no(category_name, cursor)
        
        # 1회 제공량 추출
        serving_size, serving_unit = extract_serving_size(description)
        
        # 영양정보 추출
        nutrition_info = extract_nutrition_info(description)
        
        return {
            'product_name': product_name,
            'imageurl': image_path,  # 이 키는 SQL 쿼리의 %(imageurl)s와 매칭됨
            'serving_size': serving_size,
            'serving_unit': serving_unit,
            'category_no': category_no,
            'company_name': None,
            'energy_kcal': nutrition_info.get('energy_kcal')
        }
    except Exception as e:
        print(f"제품 데이터 처리 중 오류: {str(e)}")
        return None

def insert_products(file_path):
    conn = get_db_connection()
    cursor = conn.cursor()
    category_hierarchy = get_category_hierarchy()
    
    try:
        print(f"\n파일 '{file_path}' 처리 시작")
        df = pd.read_csv(file_path, encoding='utf-8')
        
        if len(df.columns) == 3:
            df.columns = ['Name', 'Spec', 'ImageURL']
        
        success_count = 0
        error_count = 0
        
        for _, row in df.iterrows():
            try:
                product_data = process_product_data(row, cursor, category_hierarchy)
                if product_data:
                    cursor.execute("""
                        INSERT INTO product(
                            product_name, imageurl, serving_size, serving_unit, 
                            category_no, company_name, energy_kcal
                        ) VALUES (%(product_name)s, %(imageurl)s, %(serving_size)s, 
                                 %(serving_unit)s, %(category_no)s, %(company_name)s, 
                                 %(energy_kcal)s)
                    """, product_data)
                    
                    conn.commit()
                    success_count += 1
                else:
                    error_count += 1
                    
            except Exception as e:
                print(f"데이터 처리 중 오류 발생: {str(e)}")
                conn.rollback()
                error_count += 1
        
        print(f"파일 처리 완료: 성공 {success_count}건, 실패 {error_count}건")
        
    except Exception as e:
        print(f"파일 처리 중 오류 발생: {str(e)}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def process_all_files():
    folder_path = "crawl_data"
    for file_name in os.listdir(folder_path):
        if file_name.endswith('.csv'):
            file_path = os.path.join(folder_path, file_name)
            insert_products(file_path)

if __name__ == "__main__":
    process_all_files()
