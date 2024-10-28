import pandas as pd
import os
from pathlib import Path
import re

# FIELD_MAPPING을 전역 변수로 이동
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

def get_nutrition_fields():
    known_fields = set(FIELD_MAPPING.keys())
    unknown_fields = set()
    
    crawl_data_path = Path('crawl_data')
    print(f"\n1. 검사할 폴더 경로: {crawl_data_path}")
    
    # 전체 처리된 파일 수와 영양정보가 발견된 횟수를 추적
    total_files = 0
    files_with_nutrition = 0
    
    # 찾고자 하는 새로운 영양소들과 그들의 단위를 추적하기 위한 딕셔너리
    NEW_NUTRIENTS = {
        "카페인": set(),
        "카페인함량": set(),
        "고카페인": set(),
        "HCA": set(),
        "DHA+EPA": set(),
        "자일로올리고당": set(),
        "락추로스": set(),
        "락토페": set(),
        "알룰로스": set(),
        "저분자 콜라겐펩타이드": set(),
        "카르니틴": set(),
        "카테킨": set()
    }

    for csv_file in crawl_data_path.glob('*.csv'):
        total_files += 1
        print(f"\n2. 처리 중인 파일: {csv_file.name}")
        
        try:
            df = pd.read_csv(csv_file, encoding='utf-8')
            print(f"   - 파일 크기: {len(df)} 행 x {len(df.columns)} 열")
            
            nutrition_info_found = False
            for _, row in df.iterrows():
                for col in df.columns:
                    value = str(row[col])
                    if '[영양정보]' in value:
                        if not nutrition_info_found:
                            files_with_nutrition += 1
                            nutrition_info_found = True
                            
                        print(f"\n3. 영양정보 발견: {value[:100]}...")  # 처음 100자�� 출력
                        
                        nutrition_info = value.split('[영양정보]')[1]
                        nutrients = nutrition_info.split('/')
                        
                        for nutrient in nutrients:
                            if ':' in nutrient:
                                name, value = nutrient.split(':', 1)
                                name = name.strip()
                                value = value.strip()
                                
                                # 새로운 영양소 확인 및 단위 추출
                                for target in NEW_NUTRIENTS.keys():
                                    if target in name:
                                        # 단위 추출 시도
                                        unit = ''
                                        value = value.strip()
                                        # 일반적인 단위 패턴 확인
                                        for unit_pattern in ['mg', 'μg', 'g', 'kcal', '%', 'ml']:
                                            if unit_pattern in value:
                                                unit = unit_pattern
                                                break
                                        if unit:
                                            NEW_NUTRIENTS[target].add(unit)
                                            print(f"발견된 영양소: {name} - 값: {value} (단위: {unit})")
            
            # 컬럼명 확인
            nutrition_cols = [col for col in df.columns if any(unit in col for unit in ['(g)', '(mg)', '(μg)', '(kcal)'])]
            if nutrition_cols:
                print("\n5. 영양소 관련 컬럼 발견:", nutrition_cols)
                
            for col in nutrition_cols:
                field_name = col.split('(')[0].strip()
                if field_name not in known_fields:
                    unknown_fields.add(field_name)
                    print(f"6. 컬럼에서 새로운 영양소 발견: {field_name}")
                    
        except Exception as e:
            print(f"❌ 에러 발생: {csv_file.name} - {str(e)}")
    
    print(f"\n=== 처리 완료 ===")
    print(f"총 처리된 파일 수: {total_files}")
    print(f"영양정보가 발견된 파일 수: {files_with_nutrition}")
    print(f"발견된 새로운 영양소 수: {len(unknown_fields)}")
    
    if unknown_fields:
        print("\n새롭게 발견된 영양소들:")
        for field in sorted(unknown_fields):
            print(f"- {field}")
            
        unknown_df = pd.DataFrame({
            'field_name': sorted(list(unknown_fields)),
            'suggested_english_name': '',
            'unit': ''
        })
        unknown_df.to_csv('unknown_nutrition_fields.csv', index=False, encoding='utf-8')
        print("\nunknown_nutrition_fields.csv 파일이 생성되었습니다.")
    else:
        print("\n새로운 영양소 필드가 발견되지 않았습니다.")
    
    print("\n=== 새로운 영양소 단위 분석 결과 ===")
    for nutrient, units in NEW_NUTRIENTS.items():
        if units:
            print(f"{nutrient}: {', '.join(units)}")
        else:
            print(f"{nutrient}: 단위를 찾을 수 없음")

    # 결과를 CSV 파일로 저장
    results = []
    for nutrient, units in NEW_NUTRIENTS.items():
        results.append({
            'nutrient': nutrient,
            'units': ', '.join(units) if units else '단위 없음',
        })
    
    results_df = pd.DataFrame(results)
    results_df.to_csv('nutrient_units_analysis.csv', index=False, encoding='utf-8')
    print("\nnutrient_units_analysis.csv 파일이 생성되었습니다.")

    return NEW_NUTRIENTS

def analyze_nutrition_units():
    # 이제 FIELD_MAPPING을 전역 변수로 사용할 수 있습니다
    BASE_UNITS = {
        'g': {'mg': 0.001, 'μg': 0.000001},
        'mg': {'g': 1000, 'μg': 0.001},
        'μg': {'g': 1000000, 'mg': 1000}
    }
    
    # FIELD_MAPPING에서 기본 단위 추출
    field_base_units = {}
    for korean, english in FIELD_MAPPING.items():
        if '_g' in english:
            field_base_units[korean] = 'g'
        elif '_mg' in english:
            field_base_units[korean] = 'mg'
        elif '_μg' in english:
            field_base_units[korean] = 'μg'
    
    # 발견된 단위를 저장할 딕셔너리
    found_units = {field: set() for field in FIELD_MAPPING.keys()}
    
    crawl_data_path = Path('crawl_data')
    
    # CSV 파일들을 순회하며 단위 분석
    for csv_file in crawl_data_path.glob('*.csv'):
        try:
            df = pd.read_csv(csv_file, encoding='utf-8')
            
            for _, row in df.iterrows():
                for col in df.columns:
                    value = str(row[col])
                    if '[영양정보]' in value:
                        nutrition_info = value.split('[영양정보]')[1]
                        nutrients = nutrition_info.split('/')
                        
                        for nutrient in nutrients:
                            if ':' in nutrient:
                                name, value = nutrient.split(':', 1)
                                name = name.strip()
                                value = value.strip()
                                
                                # 단위 추출
                                for unit in ['mg', 'μg', 'g']:
                                    if unit in value and name in found_units:
                                        found_units[name].add(unit)
        
        except Exception as e:
            print(f"파일 처리 중 오류 발생: {csv_file.name} - {str(e)}")
    
    # 결과 분석 및 저장
    unit_analysis = []
    for field, units in found_units.items():
        if units and field in field_base_units:
            base_unit = field_base_units[field]
            unit_info = {
                'field_name': field,
                'english_name': FIELD_MAPPING[field],
                'base_unit': base_unit,
                'found_units': ', '.join(units),
                'needs_conversion': len(units) > 1
            }
            unit_analysis.append(unit_info)
    
    # 결과를 DataFrame으로 변환하고 CSV로 저장
    analysis_df = pd.DataFrame(unit_analysis)
    analysis_df.to_csv('nutrition_unit_analysis.csv', index=False, encoding='utf-8')
    
    return analysis_df

def convert_nutrition_value(value_str, from_unit, to_unit, conversion_factors=None):
    """영양소 값을 다른 단위로 변환하는 함수"""
    if conversion_factors is None:
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

def analyze_unit_conversions():
    # 변환 필요 케이스를 추적하기 위한 딕셔너리
    conversion_counts = {field: {'total': 0, 'conversions': {}} for field in FIELD_MAPPING.keys()}
    
    # 기본 단위 추출 (FIELD_MAPPING의 영문 필드명에서)
    field_base_units = {}
    for korean, english in FIELD_MAPPING.items():
        if '_g' in english:
            field_base_units[korean] = 'g'
        elif '_mg' in english:
            field_base_units[korean] = 'mg'
        elif '_μg' in english:
            field_base_units[korean] = 'μg'
    
    crawl_data_path = Path('crawl_data')
    total_files = 0
    processed_values = 0
    
    print("단위 변환 분석 시작...")
    
    for csv_file in crawl_data_path.glob('*.csv'):
        total_files += 1
        print(f"\r처리 중인 파일: {csv_file.name}", end='')
        
        try:
            df = pd.read_csv(csv_file, encoding='utf-8')
            
            for _, row in df.iterrows():
                for col in df.columns:
                    value = str(row[col])
                    if '[영양정보]' in value:
                        nutrition_info = value.split('[영양정보]')[1]
                        nutrients = nutrition_info.split('/')
                        
                        for nutrient in nutrients:
                            if ':' in nutrient:
                                name, value = nutrient.split(':', 1)
                                name = name.strip()
                                value = value.strip()
                                
                                if name in field_base_units:
                                    base_unit = field_base_units[name]
                                    processed_values += 1
                                    
                                    # 현재 값의 단위 추출
                                    current_unit = None
                                    for unit in ['mg', 'μg', 'g']:
                                        if unit in value:
                                            current_unit = unit
                                            break
                                    
                                    if current_unit and current_unit != base_unit:
                                        conversion_counts[name]['total'] += 1
                                        conversion_key = f'{current_unit}_to_{base_unit}'
                                        conversion_counts[name]['conversions'][conversion_key] = \
                                            conversion_counts[name]['conversions'].get(conversion_key, 0) + 1
                                    
        except Exception as e:
            print(f"\n파일 처리 중 오류 발생: {csv_file.name} - {str(e)}")
    
    print("\n\n=== 단위 변환 분석 결과 ===")
    print(f"처리된 파일 수: {total_files}")
    print(f"처리된 영양소 값 수: {processed_values}")
    
    # 결과를 DataFrame으로 변환
    results = []
    for field, data in conversion_counts.items():
        if data['total'] > 0:  # 변환이 필요한 케이스가 있는 경우만
            base_unit = field_base_units.get(field, 'unknown')
            conversions_detail = '; '.join([
                f"{conv}: {count}회" 
                for conv, count in data['conversions'].items()
            ])
            
            results.append({
                '영양소': field,
                '기준단위': base_unit,
                '변환필요_총횟수': data['total'],
                '변환상세': conversions_detail
            })
    
    results_df = pd.DataFrame(results)
    results_df = results_df.sort_values('변환필요_총횟수', ascending=False)
    
    # 결과를 CSV로 저장
    output_file = 'nutrition_conversion_analysis.csv'
    results_df.to_csv(output_file, index=False, encoding='utf-8')
    print(f"\n분석 결과가 {output_file}에 저장되었습니다.")
    
    # 상위 10개 변환 필요 영양소 출력
    print("\n=== 가장 많은 변환이 필요한 영양소 (상위 10개) ===")
    print(results_df.head(10))
    
    return results_df

if __name__ == "__main__":
    get_nutrition_fields()
    analysis_df = analyze_nutrition_units()
    print("\n=== 영양소 단위 분석 결과 ===")
    print(analysis_df)
    analysis_df = analyze_unit_conversions()
