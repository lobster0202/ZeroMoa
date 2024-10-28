import os
import pandas as pd

def clean_images_for_all_csvs(csv_dir, image_dir):
    # 이미지 디렉토리의 모든 파일 목록
    existing_images = set(os.listdir(image_dir))
    
    # CSV 디렉토리의 모든 CSV 파일 순회
    csv_images = set()
    for csv_file in os.listdir(csv_dir):
        if csv_file.endswith('.csv'):
            csv_path = os.path.join(csv_dir, csv_file)
            df = pd.read_csv(csv_path)
            
            # CSV에 있는 이미지 파일명만 추출
            for image_path in df['ImageURL']:
                if pd.notna(image_path):  # NaN 값 체크
                    image_name = os.path.basename(image_path)
                    csv_images.add(image_name)
    
    # CSV에 없는 이미지 파일 삭제
    for image in existing_images:
        if image not in csv_images:
            os.remove(os.path.join(image_dir, image))
            print(f"삭제된 이미지: {image}")

# 실행
csv_dir = r'C:\dev\ZeroMoa\Danawa-Crawler\crawl_data'
image_dir = r'C:\dev\ZeroMoa\Danawa-Crawler\crawl_data\images'
clean_images_for_all_csvs(csv_dir, image_dir)
