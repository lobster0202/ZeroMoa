import os
import csv
import traceback

# 상수 정의
INPUT_PATH = r'C:\dev\ZeroMoa\Danawa-Crawler\data_sort'
OUTPUT_PATH = r'C:\dev\ZeroMoa\Danawa-Crawler\sorted_data'

def data_sort():
    print('데이터 정렬 시작')

    # 입력 디렉토리가 존재하는지 확인
    if not os.path.exists(INPUT_PATH):
        print(f"입력 디렉토리를 찾을 수 없음: {INPUT_PATH}")
        return

    # 출력 디렉토리가 존재하는지 확인하고 없으면 생성
    if not os.path.exists(OUTPUT_PATH):
        os.makedirs(OUTPUT_PATH)

    # 입력 디렉토리의 모든 CSV 파일 처리
    for filename in os.listdir(INPUT_PATH):
        if filename.endswith('.csv'):
            input_file_path = os.path.join(INPUT_PATH, filename)
            output_file_path = os.path.join(OUTPUT_PATH, filename)

            try:
                with open(input_file_path, 'r', newline='', encoding='utf-8-sig') as file:
                    csv_reader = csv.reader(file)
                    data_list = list(csv_reader)

                if len(data_list) == 0:
                    print(f"데이터가 없음: {filename}")
                    continue

                # 헤더 행 분리
                header = data_list.pop(0)

                # 데이터 정렬 (첫 번째 열 기준)
                data_list.sort(key=lambda x: x[0] if len(x) > 0 else '')

                # 정렬된 데이터 저장
                with open(output_file_path, 'w', newline='', encoding='utf-8') as file:
                    csv_writer = csv.writer(file)
                    csv_writer.writerow(header)
                    csv_writer.writerows(data_list)

                print(f"정렬 완료: {filename}")

            except Exception as e:
                print(f"오류 발생 - {filename}: {str(e)}")
                traceback.print_exc()

if __name__ == "__main__":
    data_sort()

