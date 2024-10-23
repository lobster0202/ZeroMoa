def extract_nutrition_info(spec_text):
    # 기본 영양정보 패턴
    nutrition_pattern = r'(?:\[영양정보\])?\s*(?:1회\s*제공량|총\s*내용량)\s*\d+m?l당\s*((?:열량|탄수화물|당류|단백질|지방|포화지방|트랜스지방|콜레스테롤|나트륨|알룰로오스|에리스리톨)[:：]\s*[\d.]+[a-zA-Z]*\s*[/,]?\s*)*'
    
    # 영양정보 세부 항목 패턴
    item_pattern = r'(열량|탄수화물|당류|단백질|지방|포화지방|트랜스지방|콜레스테롤|나트륨|알룰로오스|에리스리톨)[:：]\s*([\d.]+(?:g|mg|kcal)?)'
    
    # 영양정보 추출
    nutrition_match = re.search(nutrition_pattern, spec_text)
    if not nutrition_match:
        return None
        
    nutrition_text = nutrition_match.group(0)
    nutrition_dict = {}
    
    # 용량 추출
    serving_size_match = re.search(r'(?:1회\s*제공량|총\s*내용량)\s*(\d+)m?l', nutrition_text)
    if serving_size_match:
        nutrition_dict['serving_size'] = int(serving_size_match.group(1))
    
    # 각 영양성분 추출
    for item_match in re.finditer(item_pattern, nutrition_text):
        key = item_match.group(1)
        value = item_match.group(2)
        # 단위 처리
        if 'kcal' in value:
            value = float(value.replace('kcal', ''))
        elif 'g' in value:
            value = float(value.replace('g', ''))
        elif 'mg' in value:
            value = float(value.replace('mg', '')) / 1000  # mg를 g으로 변환
        else:
            value = float(value)
        nutrition_dict[key] = value
    
    return nutrition_dict

# 사용 예시
def process_product_data(csv_path):
    products = []
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            product = {
                'name': row['Name'],
                'category': row['Spec'].split('/')[0] if '/' in row['Spec'] else None,
                'nutrition': extract_nutrition_info(row['Spec'])
            }
            products.append(product)
    return products