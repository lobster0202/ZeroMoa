# Selenium 관련 라이브러리 임포트
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import TimeoutException

# 날짜 및 시간 관련 라이브러리 임포트
from datetime import datetime
from datetime import timedelta
from pytz import timezone

# 전체 과정 요약
# 1. 크롤링
# 2. 정렬
# 3. 정제 및 중복 제거

# Google 스프레드시트 저장 관련 라이브러리 (현재 주석 처리됨)
# from oauth2client.service_account import ServiceAccountCredentials
# import gspread

# 엑셀 파일 저장 관련 라이브러리 (현재 주석 처리됨)
# import openpyxl

# Google Drive 저장 관련 라이브러리 (현재 주석 처리됨)
# import googledrive

# GitHub 연동 관련 라이브러리 (현재 주석 처리됨)
# from github import Github

# 기타 유용한 라이브러리 임포트
import csv
import os
import os.path
import shutil
import traceback
from math import ceil
from time import sleep

from multiprocessing import Pool
import requests
from PIL import Image
from io import BytesIO
import re
from collections import defaultdict
import psutil

# SSL 경고 메시지 비활성화 (선택사항)
import urllib3
import warnings

# SSL 경고 메시지 비활성화
warnings.filterwarnings('ignore', message='Unverified HTTPS request')
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 멀티프로세싱에서 사용할 프로세스 수 설정
PROCESS_COUNT = 12

# GitHub 관련 설정 (현재 주석 처리됨)
# GITHUB_TOKEN_KEY = 'MY_GITHUB_TOKEN'
# GITHUB_REPOSITORY_NAME = 'SSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSun/Danawa-Crawler'

# Google Sheets 관련 설정 (현재 주석 처리됨)
SHEET_KEYFILE = 'danawa-428617-f6496870d619.json'
SHEET_NAME = 'DanawaData'

# 크롤링할 카테고리 정보가 담긴 CSV 파일명
CRAWLING_DATA_CSV_FILE = 'categories.csv'

# 크롤링된 데이터를 저장할 디렉토리 설정
DATA_PATH = r'C:\dev\ZeroMoa\ZeroMoa\crawl_data'  # 절대 경로로 수정
DATA_REFRESH_PATH = f'{DATA_PATH}/Last_Data'

# 시간대 설정
TIMEZONE = 'Asia/Seoul'

# ChromeDriver 경로 설정 (현재 Windows 환경에 맞춰 설정됨)
# CHROMEDRIVER_PATH = 'chromedriver_94.exe'
# CHROMEDRIVER_PATH = 'chromedriver'
CHROMEDRIVER_PATH = 'chromedriver-win64/chromedriver.exe'

# 데터 구분자 설정 (필요 시 사용)
DATA_DIVIDER = '---'
DATA_REMARK = '//'
DATA_ROW_DIVIDER = ' - '
DATA_PRODUCT_DIVIDER = '|'
DATA_MALL_DIVIDER = '+'

# CSV 파일에서 사용할 키 값 설정
STR_NAME = 'name'
STR_URL = 'url'
STR_CRAWLING_PAGE_SIZE = 'crawlingPageSize'

class Crawler:
    def __init__(self):
        """
        초기화 메서드.
        - 오류 목록과 크롤링할 카테고리 목록을 초기화합니다.
        - categories.csv 파일을 읽어 카테고리 이름과 URL을 로드합니다.
        """
        self.errorList = list()  # 크롤링 중 발생한 오류를 저장할 리스트
        self.crawlingCategory = list()  # 크롤링할 카테고리 정보를 저장할 리스트
        
        # categories.csv 파일을 읽어 크롤링할 카테고리 목록을 로드
        with open('categories.csv', 'r', newline='', encoding='utf-8') as file:
            csv_reader = csv.reader(file, skipinitialspace=True)
            next(csv_reader)  # 첫 줄(헤더) 건너뛰기
            
            for crawlingValues in csv_reader:
                # 주석 처리된 줄(//으로 시작하는 줄)은 무시
                if not crawlingValues[0].startswith(DATA_REMARK):
                    # 카테고리 이름과 URL을 딕셔너리 형태로 리스트에 추가
                    self.crawlingCategory.append({STR_NAME: crawlingValues[0], STR_URL: crawlingValues[1]})

        # 이미지 저장 경로 추가
        self.image_path = os.path.join(DATA_PATH, 'images')
        if not os.path.exists(self.image_path):
            os.makedirs(self.image_path)

    def StartCrawling(self):
        self.chrome_option = webdriver.ChromeOptions()

        # 크롤링 옵션 설정
        self.chrome_option.add_argument('--headless')  # 헤드리스 모드 (UI 없이 실행)
        self.chrome_option.add_argument('--window-size=1920,1080')  # 창 크기 설정
        self.chrome_option.add_argument('--start-maximized')  # 최대화 옵션
        self.chrome_option.add_argument('--disable-gpu')  # GPU 사용 비활성화
        self.chrome_option.add_argument('lang=ko=KR')  # 브라우저 언어 설정
        self.chrome_option.add_argument('--disable-blink-features=AutomationControlled')
        self.chrome_option.add_argument('--disable-web-security')
        self.chrome_option.add_experimental_option('excludeSwitches', ['enable-automation'])
        self.chrome_option.add_experimental_option('useAutomationExtension', False)

        # 멀티프로세싱 풀 생성
        pool = Pool(PROCESS_COUNT)
        pool.map(self.CrawlingCategory, self.crawlingCategory)
        pool.close()
        pool.join()

    def CrawlingCategory(self, crawlingData):
        """
        각 카테고리를 크롤링하는 메서드
        """
        crawlingName = crawlingData[STR_NAME].replace('/', '_')
        browser = None
        
        try:
            # 저장 경로 확인 및 생성
            if not os.path.exists(DATA_PATH):
                os.makedirs(DATA_PATH)
            
            # CSV 파일 경로 설정
            crawlingDataPath = os.path.join(DATA_PATH, f'{crawlingName}.csv')
            
            # CSV 파일 열기 및 헤더 수정
            with open(crawlingDataPath, 'w', newline='', encoding='utf-8') as crawlingFile:
                crawlingData_csvWriter = csv.writer(crawlingFile)
                crawlingData_csvWriter.writerow(['Name', 'Spec', 'ImageURL'])

                try:
                    # Chrome 브라우저 초기화
                    browser = webdriver.Chrome(service=Service(CHROMEDRIVER_PATH), options=self.chrome_option)
                    browser.implicitly_wait(10)
                    browser.get(crawlingData[STR_URL])  # 크롤링할 카테고리 페이지로 이동

                    # 페이지 제품 수를 90개로 정
                    browser.find_element(By.XPATH,'//option[@value="90"]').click()
                    wait = WebDriverWait(browser, 10)  # 명시적 대기 시간 증가
                    
                    try:
                        # 제품 목록 로딩 대기
                        wait.until(EC.invisibility_of_element((By.CLASS_NAME, 'product_list_cover')))
                    except TimeoutException:
                        # 타임아웃 발생 시 잠시 대기 후 계속 진행
                        sleep(5)
                        print(f"페이지 로딩 지연 발생 - {crawlingName}")
                    
                    # 총 제품 수 추출
                    crawlingSize = browser.find_element(By.CLASS_NAME,'list_num').text.strip()
                    crawlingSize = crawlingSize.replace(",","").lstrip('(').rstrip(')')
                    # 페이지 수 계산 (올림 처리)
                    crawlingSize = ceil(int(crawlingSize)/90)

                    page = 1
                    while True:
                        try:
                            print(f"{crawlingName} 카테고리 {page}/{crawlingSize} 페이지 크롤링 시작")  # 페이지 시작 알림
                            
                            # 페이지 로딩 대기
                            wait = WebDriverWait(browser, 10)
                            wait.until(EC.presence_of_all_elements_located((By.CLASS_NAME, 'prod_main_info')))
                            
                            # 현재 페이지의 제품들 처리
                            products = browser.find_elements(By.CLASS_NAME, 'prod_main_info')
                            
                            for product in products:
                                try:
                                    # 제품명 추출
                                    productName = product.find_element(By.XPATH, './/p[@class="prod_name"]/a').text.strip()
                                    
                                    # 스펙 정보 추출 추가
                                    try:
                                        spec_element = product.find_element(By.CLASS_NAME, 'spec_list')
                                        spec_list_text = spec_element.text.strip()
                                    except:
                                        spec_list_text = ""  # 스펙 정보가 없는 경우 빈 문자열로 설정
                                    
                                    # 이미지 URL 추출
                                    image_element = product.find_element(By.CSS_SELECTOR, '.thumb_image img')
                                    image_url = image_element.get_attribute('data-original') or image_element.get_attribute('src')
                                    if image_url.startswith('//'):
                                        image_url = 'https:' + image_url
                                        
                                    # 이미지 저장 시 파일명을 제품명으로 정확히 지정
                                    local_image_path = self.save_image(image_url, productName.replace('/', '_'), crawlingName)
                                    
                                    # CSV에 저장
                                    crawlingData_csvWriter.writerow([productName, spec_list_text, local_image_path])
                                    
                                except Exception as e:
                                    print(f"제품 처리 중 오류 발생 ({productName if 'productName' in locals() else 'unknown'}): {str(e)}")
                                    continue
                            
                            # 다음 페이지로 이동
                            try:
                                # 페이지 번호 요소들 찾기
                                page_numbers = browser.find_elements(By.XPATH, '//a[contains(@class, "num")]')
                                
                                # 현재 페이지가 마지막인지 확인
                                if not page_numbers:
                                    print(f"더 이상 페이지가 없음 - {crawlingName}")
                                    break
                                
                                # 다음 페이지 번호 찾기
                                next_page_found = False
                                for page_element in page_numbers:
                                    if page_element.text.strip() == str(page + 1):
                                        page_element.click()
                                        next_page_found = True
                                        page += 1
                                        sleep(2)  # 페이지 로딩 대기
                                        break
                                
                                # 다음 페이지를 찾지 못했다면
                                if not next_page_found:
                                    # 다음 페이지 그룹으로 이동 시도
                                    try:
                                        next_group = browser.find_element(By.XPATH, '//a[@class="edge_nav nav_next"]')
                                        if 'nav_edge' not in next_group.get_attribute('class'):  # 비활성화 상태 체크
                                            next_group.click()
                                            sleep(2)
                                            page += 1
                                        else:
                                            print(f"마지막 페이지 도달 - {crawlingName}")
                                            break
                                    except:
                                        print(f"마지막 페이지 도달 - {crawlingName}")
                                        break
                                
                                # 새 페이지 로딩 대기
                                wait = WebDriverWait(browser, 10)
                                wait.until(EC.invisibility_of_element((By.CLASS_NAME, 'product_list_cover')))
                                
                            except Exception as e:
                                print(f"페이지 이동 중 오류 발생 - {crawlingName}: {str(e)}")
                                break
                                
                        except Exception as e:
                            print(f"{crawlingName} 카테고리 {page}페이지 처리 중 오류 발생: {str(e)}")
                            break

                except Exception as e:
                    print('Error - ' + crawlingName + ' ->')
                    print(traceback.format_exc())
                    self.errorList.append(crawlingName)
                    
                    # 라우저 세션 정리
                    try:
                        browser.quit()
                    except:
                        pass

                print('Crawling Finish : ' + crawlingName)

        except FileNotFoundError as e:
            print(f"Error: {e}")
            print(f"Directory does not exist for: {crawlingName}")

        except Exception as e:
            print(f"{crawlingName} 카테고리 크롤링 중 오류 발생: {str(e)}")
            self.errorList.append(crawlingName)  # 오류 목록에 추가

        finally:
            # 브라우저와 드이버 정리
            if browser:
                try:
                    # 열려있는 모든 창 닫기
                    for handle in browser.window_handles:
                        browser.switch_to.window(handle)
                        browser.close()
                except:
                    pass
                
                try:
                    # 브라우저 종료
                    browser.quit()
                    sleep(1)  # 브라우저 종료 후 잠시 대기
                except:
                    pass
                
                try:
                    # 프로세스 강제 종료
                    
                    process = psutil.Process(browser.service.process.pid)
                    for proc in process.children(recursive=True):
                        proc.kill()
                    process.kill()
                except:
                    pass

    def DataSort(self):
        """
        크롤링된 데이터를 정렬하고, 정제하며, 중복을 제거는 메서드
        """
        print('데이터 정렬 시작')

        for crawlingValue in self.crawlingCategory:
            dataName = crawlingValue[STR_NAME].replace('/', '_')
            crawlingDataPath = os.path.join(DATA_PATH, f'{dataName}.csv')
            
            # 정된 데이터를 저장할 경로
            cleaned_data_path = os.path.join(DATA_PATH, f"정제_중복제거_{dataName}.csv")
            
            if not os.path.exists(crawlingDataPath):
                print(f"파일을 찾을 수 없음: {crawlingDataPath}")
                continue

            try:
                # 데이터 정제 및 중복 제거 수행
                self.remove_duplicates_and_units(crawlingDataPath, cleaned_data_path)
                print(f"정제 및 중복 제거 완료: {dataName}")
                
                # 원본 파일 삭제 (선택사항)
                # os.remove(crawlingDataPath)

            except Exception as e:
                print(f"오류 발생 - {dataName}: {str(e)}")
                traceback.print_exc()

    def ResetCsv(self, crawlingDataPath):
        """
        주어진 경로의 CSV 파일을 초기화하는 메서드
        """
        with open(crawlingDataPath, 'w', newline='') as file:
            csvWriter = csv.writer(file)
            csvWriter.writerows([])

    def GetCurrentDate(self):
        """
        현재 날짜와 시간을 반환하는 메서드
        """
        tz = timezone(TIMEZONE)
        return datetime.now(tz)

    def clean_product_name(self, name):
        """
        제품 이름에서 단위와 괄호 내용을 제거하는 메서드
        """
        # 끝 부분의 숫자와 단위를 제거하는 패턴 (T, P 등의 단위 추가)
        pattern = r'\s*\d+(?:\.\d+)?\s*(?:ml|정|포|l|g|kg|mg|개|can|캔|팩|페트|병|입|박스|캡슐|스틱|매|베지캡슐|분|da|달톤|mgα-te|mgne|㎍re|㎍|μg|T|P|세트)(?:\s*x\s*\d+(?:개|팩|병|캔|박스|정|포|매|스틱)?)?\s*'
        
        # 괄호 안 내용을 제거하는 패턴
        bracket_pattern = r'\s*\([^)]*\)\s*'
        
        # 패턴을 반복적으로 적용
        prev_name = name
        while True:
            cleaned = re.sub(pattern, '', prev_name, flags=re.IGNORECASE)
            cleaned = re.sub(bracket_pattern, '', cleaned)
            cleaned = cleaned.strip()
            if cleaned == prev_name:  # 더 이상 변화가 없으면 종료
                break
            prev_name = cleaned
        
        return cleaned

    def remove_duplicates_and_units(self, input_file, output_file):
        unique_entries = defaultdict(list)
        
        with open(input_file, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader, None)
            for row in reader:
                if row and len(row) >= 3:
                    original_name = row[0]
                    
                    # '+' 문자가 포함된 제품은 건너뛰기
                    if '+' in original_name:
                        print(f"제외된 제품 (+ 포함): {original_name}")
                        continue
                    
                    cleaned_name = self.clean_product_name(original_name)  # 제품명 정제
                    
                    # 파일명에 사용할 수 없는 문자 제거
                    safe_name = re.sub(r'[\\/*?:"<>|]', '_', cleaned_name)
                    safe_name = safe_name.replace('  ', ' ').strip()
                    key = cleaned_name.split(',')[0]
                    
                    # 이미지 파일 처리
                    original_image = row[2]
                    if original_image and os.path.exists(original_image):
                        image_dir = os.path.dirname(original_image)
                        new_image_path = os.path.join(image_dir, f"{safe_name}.jpg")
                        
                        try:
                            if os.path.exists(original_image):
                                if os.path.exists(new_image_path) and original_image != new_image_path:
                                    os.remove(original_image)
                                else:
                                    os.rename(original_image, new_image_path)
                                row[2] = new_image_path
                        except Exception as e:
                            print(f"이미지 이름 변경 실패 ({original_image} -> {new_image_path}): {str(e)}")
                            row[2] = original_image

                    new_row = [cleaned_name, row[1], row[2]]
                    if key not in unique_entries or len(row[1]) > len(unique_entries[key][0][1]):
                        unique_entries[key] = [new_row]

        # 정제된 데이터 저장
        with open(output_file, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(header)
            for entries in unique_entries.values():
                writer.writerow(entries[0])

        print(f"데이터 정제 완료: {output_file}")

    def update_image_name(self, image_path, new_name):
        """
        이미지 파일을 정제된 이름으로 변경하는 메서드
        """
        if not image_path:
            return ''
        
        # 기존 이미지 경로에서 파일명 추출
        old_name = os.path.basename(image_path)
        old_dir = os.path.dirname(image_path)
        
        # 새로운 파일명 생성
        safe_name = re.sub(r'[\\/*?:"<>|]', '', new_name)
        new_image_path = os.path.join(old_dir, f"{safe_name}.jpg")
        
        # 파일명 변경
        if os.path.exists(image_path) and not os.path.exists(new_image_path):
            os.rename(image_path, new_image_path)
        
        return new_image_path

    def save_image(self, image_url, product_name, csv_name, max_retries=3):
        """
        이미지 URL에서 이미지를 다운로드하여 저장하는 메서드
        """
        # '+'가 포함된 제품은 건너뛰기
        if '+' in product_name:
            print(f"제외된 이미지 (+ 포함): {product_name}")
            return ''
        
        for attempt in range(max_retries):
            try:
                if 'noImg' in image_url:
                    return ''
                
                # 이미지 저장 경로 생성
                safe_csv_name = re.sub(r'[\\/*?:"<>|]', '_', csv_name)
                image_dir = os.path.join(self.image_path, safe_csv_name)
                if not os.path.exists(image_dir):
                    os.makedirs(image_dir)
                
                # 이미지 파일명 생성 (원본 제품명 사용, 파일시스템 금지 문자만 제거)
                safe_name = re.sub(r'[\\/*?:"<>|]', '', product_name)
                image_filename = f"{safe_name}.jpg"
                save_path = os.path.join(image_dir, image_filename)
                
                # 이미지가 이미 존재하는 경우 건너뛰기
                if os.path.exists(save_path):
                    return save_path
                
                # 이미지 다운로드
                response = requests.get(image_url, verify=False)
                if response.status_code == 200:
                    # 이미지 처리 및 저장
                    img = Image.open(BytesIO(response.content))
                    img = img.convert('RGB')  # PNG 등의 형식을 JPG로 변환
                    img.save(save_path, 'JPEG')
                    return save_path
                else:
                    print(f"이미지 다운로드 실패 (status code: {response.status_code}): {image_url}")
                    return ''
            
            except Exception as e:
                if attempt == max_retries - 1:
                    print(f"이미지 저장 실패 ({product_name}): {str(e)}")
                    return ''
                else:
                    print(f"이미지 다운로드 재시도 중... ({attempt + 1}/{max_retries})")
                    sleep(2)

        return ''

# 메인 실행 블록
if __name__ == '__main__':
    crawler = Crawler()  # 크롤러 인스턴스 생성
    crawler.StartCrawling()  # 크롤링 시작
    crawler.DataSort()  # 데이터 정렬, 정제, 중복 제거 수행
