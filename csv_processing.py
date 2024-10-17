import pandas as pd
import os
import csv
from tqdm import tqdm

# 경로 설정
directory = r'C:\dev\ZeroMoa\ZeroMoa\crawl_data'

# 디렉토리 내 모든 CSV 파일 처리
for filename in tqdm(os.listdir(directory), desc="파일 처리 중"):
    if filename.endswith('.csv'):
        file_path = os.path.join(directory, filename)
        
        # CSV 파일 읽기
        df = pd.read_csv(file_path, header=0, on_bad_lines='skip')  # 첫 번째 행을 헤더로 사용, 잘못된 행은 무시
        
        # 열 이름 확인
        print(f"Processing file: {filename}")
        print("열 이름: ", df.columns)  # 열 이름을 출력하여 확인
        
        # 'Name' 열이 존재하는지 확인
        if 'Name' not in df.columns:
            print(f"Warning: 'Name' column not found in {filename}. Changing header.")
            # 헤더를 변경하고 기존 데이터는 유지
            with open(file_path, 'r', newline='', encoding='utf-8') as file:
                reader = csv.reader(file)
                data = list(reader)  # 기존 데이터 읽기

            # 새로운 헤더 설정
            new_header = ['Id', 'Name', 'Type', 'Price', 'Mall', 'Date']
            data[0] = new_header  # 첫 번째 행에 새로운 헤더 추가

            # 변경된 내용을 파일에 다시 쓰기
            with open(file_path, 'w', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                writer.writerows(data)  # 기존 데이터와 새로운 헤더를 함께 저장

            # 헤더 변경 후, 다시 CSV 파일 읽기
            df = pd.read_csv(file_path, header=0)

        # 중복된 이름이 있는 경우 첫 번째 행만 남기고 나머지 행 삭제
        df.drop_duplicates(subset='Name', keep='first', inplace=True)
        
        # 수정된 내용을 원래 파일에 저장
        df.to_csv(file_path, index=False, header=True)  # 헤더 포함하여 저장

# 모든 파일 처리 완료 후 메시지 출력
print("모든 CSV 파일 처리가 완료되었습니다.")
