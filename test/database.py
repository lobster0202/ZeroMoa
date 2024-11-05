import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
import os
import re
from typing import Tuple, Set, Optional
from nutrition import get_nutrition_fields

FIELD_MAPPING = {
    "열량": "energy_kcal",
    "수분": "water_g",
    "단백질": "protein_g",
    "지방": "fat_g",
    "회분": "ash_g",
    "탄수화물": "carbohydrate_g",
    "당류": "sugar_g",
    "식이섬유": "dietary_fiber_g",
    "칼슘": "calcium_mg",
    "철": "iron_mg",
    "인": "phosphorus_mg",
    "칼륨": "potassium_mg",
    "나트륨": "sodium_mg",
    "비타민 A": "vitamin_a_μg_rae",
    "레티놀": "retinol_μg",
    "베타카로틴": "beta_carotene_μg",
    "티아민": "thiamine_mg",
    "리보플라빈": "riboflavin_mg",
    "니아신": "niacin_mg",
    "비타민 C": "vitamin_c_mg",
    "비타민 D": "vitamin_d_μg",
    "콜레스테롤": "cholesterol_mg",
    "포화지방산": "saturated_fatty_acids_g",
    "트랜스지방산": "trans_fatty_acids_g",
    "니코틴산": "nicotinic_acid_mg",
    "니코틴아마이드": "nicotinamide_mg",
    "비오틴 / 바이오틴": "biotin_μg",
    "비타민 B6": "vitamin_b6_mg",
    "비타민 B12": "vitamin_b12_μg",
    "엽산": "folate_μg_dfe",
    "콜린": "choline_mg",
    "판토텐산": "pantothenic_acid_mg",
    "비타민 D2": "vitamin_d2_μg",
    "비타민 D3": "vitamin_d3_μg",
    "비타민 E": "vitamin_e_mg_alpha_te",
    "알파 토코페롤": "alpha_tocopherol_mg",
    "베타 토코페롤": "beta_tocopherol_mg",
    "감마 토코페롤": "gamma_tocopherol_mg",
    "델타 토코페롤": "delta_tocopherol_mg",
    "알파 토코트리에놀": "alpha_tocotrienol_mg",
    "베타 토코트리에놀": "beta_tocotrienol_mg",
    "감마 토코트리에놀": "gamma_tocotrienol_mg",
    "델타 토코트리에놀": "delta_tocotrienol_mg",
    "비타민 K": "vitamin_k_μg",
    "비타민 K1": "vitamin_k1_μg",
    "비타민 K2": "vitamin_k2_μg",
    "갈락토오스": "galactose_g",
    "과당": "fructose_g",
    "당알콜": "sugar_alcohol_g",
    "맥아당": "maltose_g",
    "알룰로오스": "allulose_g",
    "에리스리톨": "erythritol_g",
    "유당": "lactose_g",
    "자당": "sucrose_g",
    "타가토스": "tagatose_g",
    "포도당": "glucose_g",
    "불포화지방": "unsaturated_fat_g",
    "EPA + DHA": "epa_dha_mg",
    "가돌레산": "gadoleic_acid_mg",
    "감마 리놀렌산": "gamma_linolenic_acid_mg",
    "네르본산": "nervonic_acid_mg",
    "도코사디에노산": "docosadienoic_acid_mg",
    "도코사펜타에노산": "docosapentaenoic_acid_mg",
    "도코사헥사에노산": "docosahexaenoic_acid_mg",
    "디호모리놀렌산": "dihomo_gamma_linolenic_acid_mg",
    "라우르산": "lauric_acid_mg",
    "리그노세르산": "lignoceric_acid_mg",
    "리놀레산": "linoleic_acid_g",
    "미리스톨레산": "myristoleic_acid_mg",
    "미리스트산": "myristic_acid_mg",
    "박센산": "paullinic_acid_mg",
    "베헨산": "behenic_acid_mg",
    "부티르산": "butyric_acid_mg",
    "스테아르산": "stearic_acid_mg",
    "스테아리돈산": "stearidonic_acid_mg",
    "아라키돈산": "arachidonic_acid_mg",
    "아라키드산": "arachidic_acid_mg",
    "알파리놀렌산": "alpha_linolenic_acid_g",
    "에루크산": "erucic_acid_mg",
    "에이코사디에노산": "eicosadienoic_acid_mg",
    "에이코사트리에노산": "eicosatrienoic_acid_mg",
    "에이코사펜타에노산": "eicosapentaenoic_acid_mg",
    "오메가3 지방산": "omega_3_fatty_acid_g",
    "오메가6 지방산": "omega_6_fatty_acid_g",
    "올레산": "oleic_acid_mg",
    "카프로산": "caproic_acid_mg",
    "카프르산": "capric_acid_mg",
    "카프릴산": "caprylic_acid_mg",
    "트라이데칸산": "tridecanoic_acid_mg",
    "트랜스 리놀레산": "trans_linoleic_acid_mg",
    "트랜스 리놀렌산": "trans_linolenic_acid_mg",
    "트랜스 올레산": "trans_oleic_acid_mg",
    "트리코산산": "tricosanoic_acid_mg",
    "팔미톨레산": "palmitoleic_acid_mg",
    "팔미트산": "palmitic_acid_mg",
    "펜타데칸산": "pentadecanoic_acid_mg",
    "헨에이코산산": "heneicosanoic_acid_mg",
    "헵타데센산": "heptadecenoic_acid_mg",
    "헵타데칼산": "heptadecanoic_acid_mg",
    "구리": "copper_μg",
    "마그네슘": "magnesium_mg",
    "망간": "manganese_mg",
    "몰리브덴": "molybdenum_μg",
    "불소": "fluoride_mg",
    "셀레늄": "selenium_μg",
    "아연": "zinc_mg",
    "염소": "chlorine_mg",
    "요오드": "iodine_μg",
    "크롬": "chromium_μg",
    "아미노산": "amino_acid_mg",
    "필수아미노산": "essential_amino_acid_mg",
    "비필수아미노산": "non_essential_amino_acid_mg",
    "글루탐산": "glutamic_acid_mg",
    "글리신": "glycine_mg",
    "라이신": "lysine_mg",
    "류신 / 루신": "leucine_mg",
    "메티오닌": "methionine_mg",
    "발린": "valine_mg",
    "세린": "serine_mg",
    "시스테인": "cysteine_mg",
    "아르기닌": "arginine_mg",
    "아스파르트산": "aspartic_acid_mg",
    "알라닌": "alanine_mg",
    "이소류신 / 이소루신": "isoleucine_mg",
    "타우린": "taurine_mg"  ,
    "트레오닌": "threonine_mg",
    "트립토판": "tryptophan_mg",
    "티로신": "tyrosine_mg",
    "페닐알라닌": "phenylalanine_mg",
    "프롤린": "proline_mg",
    "히스티딘": "histidine_mg",
    "카페인": "caffeine_mg",
    "카페인함량": "caffeine_mg",  # 카페인과 동일하게 처리
    "고카페인": "caffeine_mg",    # 카페인과 동일하게 처리
    "HCA": "hca_mg",
    "DHA+EPA": "epa_dha_mg",     # 기존 epa_dha_mg와 동일하게 처리
    "자일로올리고당": "xylooligosaccharide_mg",
    "락추로스": "lactulose_mg",
    "락토페린": "lactoferrin_mg",
    "알룰로스": "allulose_g",     # 기존 allulose_g와 동일하게 처리
    "저분자 콜라겐펩타이드": "low_molecular_collagen_peptide_mg",
    "카르니틴": "carnitine_mg",
    "카테킨": "catechin_mg"
}

def get_db_connection():
    return psycopg2.connect(
        user="ZeroMoa",
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

def convert_nutrition_value(value_str: str, from_unit: str, to_unit: str) -> float:
    """영양소 값을 다른 단위로 변환하는 함수"""
    conversion_factors = {
        'g_to_mg': 1000,
        'g_to_μg': 1000000,
        'mg_to_g': 0.001,
        'mg_to_μg': 1000,
        'μg_to_g': 0.000001,
        'μg_to_mg': 0.001
    }
    
    try:
        # 숫자 추출
        value = float(''.join(filter(lambda x: x.isdigit() or x == '.', value_str)))
        
        # 같은 단위면 변환 불필요
        if from_unit == to_unit:
            return value
        
        # 단위 변환
        conversion_key = f'{from_unit}_to_{to_unit}'
        if conversion_key in conversion_factors:
            return value * conversion_factors[conversion_key]
        
        return None
    except:
        return None

def get_base_unit(field_name: str) -> str:
    """영양소 필드의 기준 단위를 반환하는 함수"""
    english_name = FIELD_MAPPING.get(field_name)
    if english_name:
        if '_g' in english_name:
            return 'g'
        elif '_mg' in english_name:
            return 'mg'
        elif '_μg' in english_name:
            return 'μg'
    return None

def extract_nutrition_info(description: str) -> dict:
    FIELD_MAPPING = {
        "열량": "energy_kcal",
        "수분": "water_g",
        "단백질": "protein_g",
        "지방": "fat_g",
        "회분": "ash_g",
        "탄수화물": "carbohydrate_g",
        "당류": "sugar_g",
        "식이섬유": "dietary_fiber_g",
        "칼슘": "calcium_mg",
        "철": "iron_mg",
        "인": "phosphorus_mg",
        "칼륨": "potassium_mg",
        "나트륨": "sodium_mg",
        "비타민 A": "vitamin_a_μg_rae",
        "레티놀": "retinol_μg",
        "베타카로틴": "beta_carotene_μg",
        "티아민": "thiamine_mg",
        "리보플라빈": "riboflavin_mg",
        "니아신": "niacin_mg",
        "비타민 C": "vitamin_c_mg",
        "비타민 D": "vitamin_d_μg",
        "콜레스테롤": "cholesterol_mg",
        "포화지방산": "saturated_fatty_acids_g",
        "트랜스지방산": "trans_fatty_acids_g",
        "니코틴산": "nicotinic_acid_mg",
        "니코틴아마이드": "nicotinamide_mg",
        "비오틴 / 바이오틴": "biotin_μg",
        "비타민 B6": "vitamin_b6_mg",
        "비타민 B12": "vitamin_b12_μg",
        "엽산": "folate_μg_dfe",
        "콜린": "choline_mg",
        "판토텐산": "pantothenic_acid_mg",
        "비타민 D2": "vitamin_d2_μg",
        "비타민 D3": "vitamin_d3_μg",
        "비타민 E": "vitamin_e_mg_alpha_te",
        "알파 토코페롤": "alpha_tocopherol_mg",
        "베타 토코페롤": "beta_tocopherol_mg",
        "감마 토코페롤": "gamma_tocopherol_mg",
        "델타 토코페롤": "delta_tocopherol_mg",
        "알파 토코트리에놀": "alpha_tocotrienol_mg",
        "베타 토코트리에놀": "beta_tocotrienol_mg",
        "감마 토코트리에놀": "gamma_tocotrienol_mg",
        "델타 토코트리에놀": "delta_tocotrienol_mg",
        "비타민 K": "vitamin_k_μg",
        "비타민 K1": "vitamin_k1_μg",
        "비타민 K2": "vitamin_k2_μg",
        "갈락토오스": "galactose_g",
        "과당": "fructose_g",
        "당알콜": "sugar_alcohol_g",
        "맥아당": "maltose_g",
        "알룰로오스": "allulose_g",
        "에리스리톨": "erythritol_g",
        "유당": "lactose_g",
        "자당": "sucrose_g",
        "타가토스": "tagatose_g",
        "포도당": "glucose_g",
        "불포화지방": "unsaturated_fat_g",
        "EPA + DHA": "epa_dha_mg",
        "가돌레산": "gadoleic_acid_mg",
        "감마 리놀렌산": "gamma_linolenic_acid_mg",
        "네르본산": "nervonic_acid_mg",
        "도코사디에노산": "docosadienoic_acid_mg",
        "도코사펜타에노산": "docosapentaenoic_acid_mg",
        "도코사헥사에노산": "docosahexaenoic_acid_mg",
        "디호모리놀렌산": "dihomo_gamma_linolenic_acid_mg",
        "라우르산": "lauric_acid_mg",
        "리그노세르산": "lignoceric_acid_mg",
        "리놀레산": "linoleic_acid_g",
        "미리스톨레산": "myristoleic_acid_mg",
        "미리스트산": "myristic_acid_mg",
        "박센산": "paullinic_acid_mg",
        "베헨산": "behenic_acid_mg",
        "부티르산": "butyric_acid_mg",
        "스테아르산": "stearic_acid_mg",
        "스테아리돈산": "stearidonic_acid_mg",
        "아라키돈산": "arachidonic_acid_mg",
        "아라키드산": "arachidic_acid_mg",
        "알파리놀렌산": "alpha_linolenic_acid_g",
        "에루크산": "erucic_acid_mg",
        "에이코사디에노산": "eicosadienoic_acid_mg",
        "에이코사트리에노산": "eicosatrienoic_acid_mg",
        "에이코사펜타에노산": "eicosapentaenoic_acid_mg",
        "오메가3 지방산": "omega_3_fatty_acid_g",
        "오메가6 지방산": "omega_6_fatty_acid_g",
        "올레산": "oleic_acid_mg",
        "카프로산": "caproic_acid_mg",
        "카프르산": "capric_acid_mg",
        "카프릴산": "caprylic_acid_mg",
        "트라이데칸산": "tridecanoic_acid_mg",
        "트랜스 리놀레산": "trans_linoleic_acid_mg",
        "트랜스 리놀렌산": "trans_linolenic_acid_mg",
        "트랜스 올레산": "trans_oleic_acid_mg",
        "트리코산산": "tricosanoic_acid_mg",
        "팔미톨레산": "palmitoleic_acid_mg",
        "팔미트산": "palmitic_acid_mg",
        "펜타데칸산": "pentadecanoic_acid_mg",
        "헨에이코산산": "heneicosanoic_acid_mg",
        "헵타데센산": "heptadecenoic_acid_mg",
        "헵타데칼산": "heptadecanoic_acid_mg",
        "구리": "copper_μg",
        "마그네슘": "magnesium_mg",
        "망간": "manganese_mg",
        "몰리브덴": "molybdenum_μg",
        "불소": "fluoride_mg",
        "셀레늄": "selenium_μg",
        "아연": "zinc_mg",
        "염소": "chlorine_mg",
        "요오드": "iodine_μg",
        "크롬": "chromium_μg",
        "아미노산": "amino_acid_mg",
        "필수아미노산": "essential_amino_acid_mg",
        "비필수아미노산": "non_essential_amino_acid_mg",
        "글루탐산": "glutamic_acid_mg",
        "글리신": "glycine_mg",
        "라이신": "lysine_mg",
        "류신 / 루신": "leucine_mg",
        "메티오닌": "methionine_mg",
        "발린": "valine_mg",
        "세린": "serine_mg",
        "시스테인": "cysteine_mg",
        "아르기닌": "arginine_mg",
        "아스파르트산": "aspartic_acid_mg",
        "알라닌": "alanine_mg",
        "이소류신 / 이소루신": "isoleucine_mg",
        "타우린": "taurine_mg"  ,
        "트레오닌": "threonine_mg",
        "트립토판": "tryptophan_mg",
        "티로신": "tyrosine_mg",
        "페닐알라닌": "phenylalanine_mg",
        "프롤린": "proline_mg",
        "히스티딘": "histidine_mg",
        "카페인": "caffeine_mg",
        "카페인함량": "caffeine_mg",  # 카페인과 동일하게 처리
        "고카페인": "caffeine_mg",    # 카페인과 동일하게 처리
        "HCA": "hca_mg",
        "DHA+EPA": "epa_dha_mg",     # 기존 epa_dha_mg와 동일하게 처리
        "자일로올리고당": "xylooligosaccharide_mg",
        "락추로스": "lactulose_mg",
        "락토페린": "lactoferrin_mg",
        "알룰로스": "allulose_g",     # 기존 allulose_g와 동일하게 처리
        "저분자 콜라겐펩타이드": "low_molecular_collagen_peptide_mg",
        "카르니틴": "carnitine_mg",
        "카테킨": "catechin_mg"
    }
    
    nutrition_info = {}
    if "[영양정보]" in description:
        try:
            nutrition_text = description.split("[영양정보]")[1].split("/")
            for item in nutrition_text:
                if ":" in item:
                    key, value = [x.strip() for x in item.split(":", 1)]
                    
                    # FIELD_MAPPING에서 매칭되는 영양소 찾기
                    if key in FIELD_MAPPING:
                        field_name = FIELD_MAPPING[key]
                        
                        # 현재 값의 단위 추출
                        current_unit = None
                        for unit in ['mg', 'μg', 'g']:
                            if unit in value:
                                current_unit = unit
                                break
                        
                        if current_unit:
                            # 기준 단위 확인
                            base_unit = get_base_unit(key)
                            if base_unit:
                                # 숫자 값 추출 및 단위 변환
                                converted_value = convert_nutrition_value(value, current_unit, base_unit)
                                if converted_value is not None:
                                    nutrition_info[field_name] = converted_value
                                    
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
        
        # 기본 제품 정보
        product_data = {
            'product_name': product_name,
            'imageurl': image_path,
            'serving_size': serving_size,
            'serving_unit': serving_unit,
            'category_no': category_no,
            'company_name': None,
        }
        
        # 영양정보 추가
        product_data.update(nutrition_info)
        
        return product_data
        
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
                    # 든 영양소 정보를 포함하여 product 테이블에 한 번에 삽입
                    columns = ', '.join(product_data.keys())
                    placeholders = ', '.join(['%s'] * len(product_data))
                    values = list(product_data.values())
                    
                    cursor.execute(f"""
                        INSERT INTO product(
                            {columns}
                        ) VALUES (
                            {placeholders}
                        )
                    """, values)
                    
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
