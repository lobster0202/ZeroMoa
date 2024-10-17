import csv
import re
import os
import shutil
from datetime import datetime

def remove_timestamp_row(input_file, output_file):
    with open(input_file, 'r', encoding='utf-8', errors='ignore') as infile, open(output_file, 'w', newline='', encoding='utf-8') as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)
        
        timestamp_pattern = re.compile(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}')
        
        for row in reader:
            if row and not timestamp_pattern.match(','.join(row)):
                writer.writerow(row)

def process_directory(directory):
    backup_dir = os.path.join(directory, 'backup_' + datetime.now().strftime('%Y%m%d_%H%M%S'))
    os.makedirs(backup_dir, exist_ok=True)

    for filename in os.listdir(directory):
        if filename.endswith('.csv'):
            input_file = os.path.join(directory, filename)
            temp_file = os.path.join(directory, 'temp_' + filename)
            backup_file = os.path.join(backup_dir, filename)

            # 원본 파일 백업
            shutil.copy2(input_file, backup_file)

            # 타임스탬프 제거
            remove_timestamp_row(input_file, temp_file)

            # 임시 파일을 원본 파일로 대체
            os.replace(temp_file, input_file)
            print(f"{filename} 처리 완료")

    print(f"모든 CSV 파일의 타임스탬프 행이 제거되었습니다. 원본 파일은 {backup_dir}에 백업되었습니다.")

# 디렉토리 경로 설정
directory = r'C:\dev\ZeroMoa\Danawa-Crawler\sorted_data'

# 함수 실행
process_directory(directory)
