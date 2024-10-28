import pandas as pd
import glob
import re
import os

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

def categorize_text(text, category_hierarchy):
    categories = set()
    serving_size = None  # 1회 제공량을 저장할 변수
    
    # 1회 제공량 추출
    if '[영양정보]' in text:
        serving_match = re.search(r'1회 제공량\s*(\d+)(\w+)', text)
        if serving_match:
            amount = serving_match.group(1)
            unit = serving_match.group(2)
            serving_size = f"{amount}{unit}"  # 예: "500ml", "250ml" 등
    
    # ... 기존의 카테고리 분류 로직 ...
    for main_category, data in category_hierarchy.items():
        # 메인 키워드 검사
        if any(keyword.lower() in text.lower() for keyword in data['keywords']):
            categories.add(main_category)
        
        # 괄호 안의 내용 추출
        sub_parts = re.findall(r'\((.*?)\)', text)
        
        for sub_part in sub_parts:
            # 괄호 안의 내용도 검사
            if sub_part:
                sub_part = sub_part.lower()
                if any(keyword.lower() in sub_part for keyword in data['keywords']):
                    categories.add(main_category)
                
                for sub_category, keywords in data['subcategories'].items():
                    if any(keyword.lower() in sub_part for keyword in keywords):
                        categories.add(main_category)
                        categories.add(sub_category)
    
    return categories, serving_size

def process_csv_files():
    csv_files = glob.glob('crawl_data/*.csv')
    category_hierarchy = get_category_hierarchy()
    category_data = []  # 카테고리와 serving_size를 함께 저장할 리스트
    
    for file_path in csv_files:
        try:
            df = pd.read_csv(file_path, encoding='utf-8')
            
            if 'Spec' in df.columns:
                for text in df['Spec']:
                    categories, serving_size = categorize_text(str(text), category_hierarchy)
                    
                    # 각 카테고리별로 데이터 저장
                    for category in categories:
                        category_data.append({
                            'Category': category,
                            'Serving_Size': serving_size
                        })
        
        except Exception as e:
            print(f"Error processing {file_path}: {str(e)}")
    
    # 데이터프레임 생성 및 카테고리별 집계
    results_df = pd.DataFrame(category_data)
    category_counts = results_df['Category'].value_counts().reset_index()
    category_counts.columns = ['Category', 'Count']
    
    # Serving_Size 정보 추가
    category_counts['Serving_Size'] = results_df.groupby('Category')['Serving_Size'].agg(lambda x: ', '.join(str(i) for i in set(x) if pd.notna(i))).values
    
    # 결과 저장
    category_counts.to_csv('카테고리_분석결과_상세.csv', index=False, encoding='utf-8-sig')
    
    print("\n=== 카테고리별 출현 빈도 ===")
    for _, row in category_counts.iterrows():
        print(f"- {row['Category']}: {row['Count']}회")
    
    print("\n분석 결과가 '카테고리_분석결과_상세.csv' 파일로 저장되었습니다.")
    
    return dict(zip(category_counts['Category'], category_counts['Count']))

# 실행
counts = process_csv_files()
