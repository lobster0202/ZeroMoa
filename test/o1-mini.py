import csv
import re
import sqlite3

def parse_nutrition_info(info_str):
    nutrition = {}
    # 정규식을 사용하여 영양정보 추출
    matches = re.findall(r'(\w+):\s*([\d\.]+)(\w?)', info_str)
    for match in matches:
        key = match[0]
        value = float(match[1])
        unit = match[2]
        nutrition[key] = f"{value}{unit}"
    return nutrition

def load_csv_to_db(csv_path, db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # 테이블 생성
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS zero_calorie_drinks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            category TEXT,
            packaging TEXT,
            calorie TEXT,
            carbohydrates TEXT,
            sugars TEXT,
            protein TEXT,
            fat TEXT,
            saturated_fat TEXT,
            trans_fat TEXT,
            cholesterol TEXT,
            sodium TEXT,
            additional_info TEXT
        )
    ''')
    
    with open(csv_path, 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        for row in reader:
            name_spec = row[0].split(',')
            if len(name_spec) < 2:
                continue
            name = name_spec[0].strip()
            spec = name_spec[1].strip()
            
            # 기본 분류 정보 파싱
            specs = spec.split('/')
            category = specs[0] if len(specs) > 0 else ''
            packaging = specs[2] if len(specs) > 2 else ''
            
            # 영양정보 추출
            nutrition_info = {}
            additional_info = ''
            for item in specs[3:]:
                if item.startswith('[영양정보]'):
                    nutrition_str = item.replace('[영양정보]', '').strip()
                    nutrition_info = parse_nutrition_info(nutrition_str)
                else:
                    # [영양정보] 없이 영양정보가 포함된 경우
                    nutrition_info = parse_nutrition_info(item)
            
            # 데이터베이스에 삽입할 값 준비
            calorie = nutrition_info.get('열량', '')
            carbohydrates = nutrition_info.get('탄수화물', '')
            sugars = nutrition_info.get('당류', '')
            protein = nutrition_info.get('단백질', '')
            fat = nutrition_info.get('지방', '')
            saturated_fat = nutrition_info.get('포화지방', '')
            trans_fat = nutrition_info.get('트랜스지방', '')
            cholesterol = nutrition_info.get('콜레스테롤', '')
            sodium = nutrition_info.get('나트륨', '')
            additional_info = '; '.join([f"{k}: {v}" for k, v in nutrition_info.items() if k not in ['열량', '탄수화물', '당류', '단백질', '지방', '포화지방', '트랜스지방', '콜레스테롤', '나트륨']])
            
            cursor.execute('''
                INSERT INTO zero_calorie_drinks (
                    name, category, packaging, calorie, carbohydrates, sugars, protein, fat,
                    saturated_fat, trans_fat, cholesterol, sodium, additional_info
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                name, category, packaging, calorie, carbohydrates, sugars, protein, fat,
                saturated_fat, trans_fat, cholesterol, sodium, additional_info
            ))
    
    conn.commit()
    conn.close()

if __name__ == "__main__":
    csv_file_path = 'crawl_data/정제_중복제거_제로칼로리음료.csv'
    database_path = 'crawl_data/zero_calorie_drinks.db'
    load_csv_to_db(csv_file_path, database_path)
    print("데이터베이스 적재 완료.")