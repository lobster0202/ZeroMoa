import pandas as pd
import os
from pathlib import Path

def get_nutrition_fields():
    # ... FIELD_MAPPING 정의 부분은 동일 ...

    known_fields = set(FIELD_MAPPING.keys())
    unknown_fields = set()
    
    crawl_data_path = Path('crawl_data')
    print(f"\n1. 검사할 폴더 경로: {crawl_data_path}")
    
    # 전체 처리된 파일 수와 영양정보가 발견된 횟수를 추적
    total_files = 0
    files_with_nutrition = 0
    
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
                            
                        print(f"\n3. 영양정보 발견: {value[:100]}...")  # 처음 100자만 출력
                        
                        nutrition_info = value.split('[영양정보]')[1]
                        nutrients = nutrition_info.split('/')
                        
                        for nutrient in nutrients:
                            if ':' in nutrient:
                                name, value = nutrient.split(':', 1)
                                name = name.strip()
                                
                                # 단위 제거
                                original_name = name
                                for unit in ['kcal', 'g', 'mg', 'μg']:
                                    if unit in name:
                                        name = name.replace(unit, '').strip()
                                        break
                                
                                if name and name not in known_fields:
                                    unknown_fields.add(name)
                                    print(f"4. 새로운 영양소 발견: {original_name} -> {name}")
            
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
    
    return unknown_fields
