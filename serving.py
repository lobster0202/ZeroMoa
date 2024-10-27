import pandas as pd
import re
import os

def extract_serving_size(text):
    if pd.isna(text) or '[영양정보]' not in str(text):
        return None, None
    
    pattern = r'1회 제공량\s*(\d+)\s*(ml|g)'
    match = re.search(pattern, str(text))
    
    if match:
        amount = int(match.group(1))
        unit = match.group(2)
        return amount, unit
    return None, None

def process_csv_file(file_path):
    try:
        # CSV 파일 읽기
        df = pd.read_csv(file_path)
        
        # Spec 컬럼에서 1회 제공량 정보 추출
        serving_info = df['Spec'].apply(extract_serving_size)
        
        # 추출된 정보를 새로운 컬럼으로 분리
        df['Serving_Amount'] = serving_info.apply(lambda x: x[0])
        df['Serving_Unit'] = serving_info.apply(lambda x: x[1])
        
        # 결과 요약
        summary = df[['Name', 'Serving_Amount', 'Serving_Unit']].dropna()
        print(f"\n파일 분석 결과: {file_path}")
        print(f"총 제품 수: {len(df)}")
        print(f"1회 제공량 정보가 있는 제품 수: {len(summary)}")
        
        # 1회 제공량 분포 확인
        if not summary.empty:
            print("\n1회 제공량 분포:")
            for unit in summary['Serving_Unit'].unique():
                unit_data = summary[summary['Serving_Unit'] == unit]
                print(f"\n{unit} 단위 제품:")
                print(unit_data['Serving_Amount'].value_counts().sort_index())
        
        # 결과를 CSV 파일로 저장
        output_filename = os.path.splitext(file_path)[0] + '_serving_size.csv'
        df[['Name', 'Serving_Amount', 'Serving_Unit']].to_csv(output_filename, index=False, encoding='utf-8-sig')
        print(f"\n분석 결과가 저장된 파일: {output_filename}")
        
        return df
        
    except Exception as e:
        print(f"파일 처리 중 오류 발생: {e}")
        return None

# crawl_data 폴더의 모든 CSV 파일 처리
folder_path = "crawl_data"
for file_name in os.listdir(folder_path):
    if file_name.endswith('.csv') and file_name.startswith('정제_중복제거_'):
        file_path = os.path.join(folder_path, file_name)
        process_csv_file(file_path)
