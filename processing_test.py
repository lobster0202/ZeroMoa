import csv
import re
from datetime import datetime

def remove_timestamp_row(input_file, output_file):
    with open(input_file, 'r', encoding='utf-8') as infile, open(output_file, 'w', newline='', encoding='utf-8') as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)
        
        # 타임스탬프 패턴 정의
        timestamp_pattern = re.compile(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}')
        
        for row in reader:
            # 행이 비어있지 않고 타임스탬프 패턴과 일치하지 않으면 쓰기
            if row and not timestamp_pattern.match(row[0]):
                writer.writerow(row)

# 파일 경로 설정
input_file = 'input.csv'  # 입력 파일 경로
output_file = 'output.csv'  # 출력 파일 경로

# 함수 실행
remove_timestamp_row(input_file, output_file)

print("타임스탬프 행이 제거된 새 CSV 파일이 생성되었습니다.")

