import csv
import os
import re
from collections import defaultdict

def find_all_nutrition_fields(directory):
    # 영양성분과 그 단위를 저장할 딕셔너리
    nutrition_fields = defaultdict(set)
    
    # 정규표현식 패턴
    nutrition_pattern = r'([\w\s]+)[:：]\s*([\d.]+)([a-zA-Z%]*)'
    
    # directory 내의 모든 CSV 파일 검색
    for filename in os.listdir(directory):
        if filename.endswith('.csv'):
            file_path = os.path.join(directory, filename)
            
            with open(file_path, 'r', encoding='utf-8') as file:
                reader = csv.reader(file)
                for row in reader:
                    if not row:  # 빈 행 건너뛰기
                        continue
                        
                    # 제품 정보에서 영양성분 부분 추출
                    product_info = row[0].split(',')[1] if len(row[0].split(',')) > 1 else ''
                    
                    # 모든 매치 찾기
                    matches = re.finditer(nutrition_pattern, product_info)
                    for match in matches:
                        field_name = match.group(1).strip()
                        unit = match.group(3) if match.group(3) else 'no_unit'
                        nutrition_fields[field_name].add(unit)
    
    return nutrition_fields

def print_nutrition_analysis(nutrition_fields):
    print("\n=== 발견된 영양성분 필드 ===")
    print("\n필드명 : 사용된 단위")
    print("-" * 30)
    for field, units in sorted(nutrition_fields.items()):
        units_str = ', '.join(sorted(units)) if units else 'no unit'
        print(f"{field:<15} : {units_str}")

def generate_create_table_sql(nutrition_fields):
    sql_field_types = {
        'kcal': 'TEXT',
        'g': 'TEXT',
        'mg': 'TEXT',
        '%': 'TEXT',
        'no_unit': 'TEXT'
    }
    
    fields = []
    fields.append("id INTEGER PRIMARY KEY AUTOINCREMENT")
    fields.append("name TEXT")
    fields.append("category TEXT")
    fields.append("packaging TEXT")
    
    for field, units in sorted(nutrition_fields.items()):
        field_name = field.replace(' ', '_').lower()
        fields.append(f"{field_name} TEXT")
    
    sql = "CREATE TABLE IF NOT EXISTS zero_calorie_drinks (\n    "
    sql += ",\n    ".join(fields)
    sql += "\n)"
    
    return sql

if __name__ == "__main__":
    directory = 'crawl_data'
    nutrition_fields = find_all_nutrition_fields(directory)
    
    # 분석 결과 출력
    print_nutrition_analysis(nutrition_fields)
    
    # CREATE TABLE SQL 문 생성
    print("\n=== 생성된 CREATE TABLE SQL ===\n")
    print(generate_create_table_sql(nutrition_fields))