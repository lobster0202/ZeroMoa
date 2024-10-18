# Selenium 관련 라이브러리 임포트
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service

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

# 멀티프로세싱에서 사용할 프로세스 수 설정
PROCESS_COUNT = 6

# GitHub 관련 설정 (현재 주석 처리됨)
# GITHUB_TOKEN_KEY = 'MY_GITHUB_TOKEN'
# GITHUB_REPOSITORY_NAME = 'SSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSun/Danawa-Crawler'

# Google Sheets 관련 설정 (현재 주석 처리됨)
SHEET_KEYFILE = 'danawa-428617-f6496870d619.json'
SHEET_NAME = 'DanawaData'

# 크롤링할 카테고리 정보가 담긴 CSV 파일명
CRAWLING_DATA_CSV_FILE = 'CrawlingCategory.csv'

# 크롤링된 데이터를 저장할 디렉토리 설정
DATA_PATH = r'C:\dev\ZeroMoa\ZeroMoa\crawl_data'  # 절대 경로로 설정
DATA_REFRESH_PATH = f'{DATA_PATH}/Last_Data'

# 시간대 설정
TIMEZONE = 'Asia/Seoul'

# ChromeDriver 경로 설정 (현재 Windows 환경에 맞춰 설정됨)
# CHROMEDRIVER_PATH = 'chromedriver_94.exe'
# CHROMEDRIVER_PATH = 'chromedriver'
CHROMEDRIVER_PATH = 'chromedriver-win64/chromedriver.exe'

# 데이터 구분자 설정 (필요 시 사용)
DATA_DIVIDER = '---'
DATA_REMARK = '//'
DATA_ROW_DIVIDER = ' - '
DATA_PRODUCT_DIVIDER = '|'
DATA_MALL_DIVIDER = '+'

# CSV 파일에서 사용할 키 값 설정
STR_NAME = 'name'
STR_URL = 'url'
STR_CRAWLING_PAGE_SIZE = 'crawlingPageSize'

# 정규 표현식과 defaultdict를 위한 추가 import
import re
from collections import defaultdict

class Crawler:
    def __init__(self):
        """
        초기화 메서드.
        - 오류 목록과 크롤링할 카테고리 목록을 초기화합니다.
        - CrawlingCategory.csv 파일을 읽 카테고리 이름과 URL을 로드합니다.
        """
        self.errorList = list()  # 크롤링 중 발생한 오류를 저장할 리스트
        self.crawlingCategory = list()  # 크롤링할 카테고리 정보를 저장할 리스트
        
        # CrawlingCategory.csv 파일을 읽어 크롤링할 카테고리 목록을 로드
        with open('categories.csv', 'r', newline='', encoding='utf-8') as file:  # 인코딩 명시
            for crawlingValues in csv.reader(file, skipinitialspace=True):
                # 주석 처리된 줄(//으로 시작하는 줄)은 무시
                if not crawlingValues[0].startswith(DATA_REMARK):
                    # 카테고리 이름과 URL을 딕셔너리 형태로 리스트에 추가
                    self.crawlingCategory.append({STR_NAME: crawlingValues[0], STR_URL: crawlingValues[1]})


    def StartCrawling(self):
        self.chrome_option = webdriver.ChromeOptions()

        # 크롤링 옵션 설정
        self.chrome_option.add_argument('--headless')  # 헤드리스 모드 (UI 없이 실행)
        self.chrome_option.add_argument('--window-size=1920,1080')  # 창 크기 설정
        self.chrome_option.add_argument('--start-maximized')  # 최대화 옵션
        self.chrome_option.add_argument('--disable-gpu')  # GPU 사용 비활성화
        self.chrome_option.add_argument('lang=ko=KR')  # 브라우저 언 설정

        if __name__ == '__main__':
            # 멀티프로세싱 풀 생성
            pool = Pool(PROCESS_COUNT)
            # CrawlingCategory 메서드를 각 카테고리에 병렬로 적용
            pool.map(self.CrawlingCategory, self.crawlingCategory)
            pool.close()
            pool.join()

    def CrawlingCategory(self, crawlingData):
        """
        각 카테고리를 크롤링하는 메서드.
        - 주어진 카테고리 URL로 이동하여 제품 데이터를 수집하고 CSV 파일로 저장합니다.
        """
        crawlingName = crawlingData[STR_NAME].replace('/', '_')  # '/'를 '_'로 대체
        # 디렉토리 생성
        directory = os.path.dirname(f'{crawlingName}.csv')
        if not os.path.exists(directory) and directory != '':
            os.makedirs(directory)

        # 파일 열기
        try:
            crawlingFile = open(f'{crawlingName}.csv', 'w', newline='', encoding='utf-8')
            crawlingData_csvWriter = csv.writer(crawlingFile)

            # 헤더 수정
            crawlingData_csvWriter.writerow(['Name', 'Spec'])

            # 중복 체크를 위한 집합
            saved_product_names = set()

            try:
                # Chrome 브라우저 초기화 및 페이지 로드
                browser = webdriver.Chrome(service=Service(CHROMEDRIVER_PATH), options=self.chrome_option)
                browser.implicitly_wait(5)  # 암묵적 대기 설정
                browser.get(crawlingData[STR_URL])  # 크롤링할 카테고리 페이지로 이동

                # 페이지당 제품 수를 90개로 설정
                browser.find_element(By.XPATH,'//option[@value="90"]').click()
                wait = WebDriverWait(browser, 5)  # 명시적 대기 설정
                # 제품 목록 로딩 대기 (로딩 커버 요소가 사라질 때까지 대기)
                wait.until(EC.invisibility_of_element((By.CLASS_NAME, 'product_list_cover')))

                # 총 제품 수 추출
                crawlingSize = browser.find_element(By.CLASS_NAME,'list_num').text.strip()
                crawlingSize = crawlingSize.replace(",","").lstrip('(').rstrip(')')
                # 페이지 수 계산 (올림 처리)
                crawlingSize = ceil(int(crawlingSize)/90)

                # 각 페이지를 순회하며 데이터 크롤링
                for i in range(0, crawlingSize):
                    print("Start - " + crawlingName + " " + str(i+1) + "/" + str(crawlingSize) + " Page Start")
                    
                    # 첫 페이지일 경우 '신상품순' 정렬 클릭
                    if i == 0:
                        browser.find_element(By.XPATH,'//li[@data-sort-method="NEW"]').click()
                    elif i > 0:
                        # 10의 배수 페이지일 경우 '다음' 버튼 클릭하여 다음 페이지 그룹으로 이동
                        if i % 10 == 0:
                            browser.find_element(By.XPATH,'//a[@class="edge_nav nav_next"]').click()
                        else:
                            # 현재 페이지 그룹 내에서 페이지 번호 버튼 클릭
                            browser.find_element(By.XPATH,'//a[@class="num "][%d]'%(i%10)).click()
                    
                    # 페이지 로딩 대기
                    wait.until(EC.invisibility_of_element((By.CLASS_NAME, 'product_list_cover')))
                    
                    # 제품 리스트 추출
                    productListDiv = browser.find_element(By.XPATH,'//div[@class="main_prodlist main_prodlist_list"]')
                    products = productListDiv.find_elements(By.XPATH,'//ul[@class="product_list"]/li')

                    for product in products:
                        # 제품에 ID가 없으면 건너뜀
                        if not product.get_attribute('id'):
                            continue

                        # 광고 제품 제외
                        if 'prod_ad_item' in product.get_attribute('class').split(' '):
                            continue
                        if product.get_attribute('id').strip().startswith('ad'):
                            continue

                        # 제품 이름 추출
                        productName = product.find_element(By.XPATH, './div/div[2]/p/a').text.strip()

                        # 중복 체크
                        if productName in saved_product_names:
                            continue  # 이미 저장된 제품 이름이면 건너뜀
                        saved_product_names.add(productName)  # 새 제품 이름 추가

                        # 제품의 'spec_list' div 추출
                        spec_list = product.find_element(By.XPATH, './/div[@class="spec_list"]')
                        spec_list_text = spec_list.text.strip()

                        # CSV 파일에 제품 이름과 스펙 정보만 기록
                        crawlingData_csvWriter.writerow([productName, spec_list_text])

            except Exception as e:
                # 예외 발생 시 오류 메시지 출력 및 오류 목록에 추가
                print('Error - ' + crawlingName + ' ->')
                print(traceback.format_exc())
                self.errorList.append(crawlingName)

            # CSV 파일 닫기
            crawlingFile.close()

            print('Crawling Finish : ' + crawlingName)

        except FileNotFoundError as e:
            print(f"Error: {e}")
            print(f"Directory does not exist for: {crawlingName}")

    def DataSort(self):
        """
        크롤링된 데이터를 정렬하고, 정제하며, 중복을 제거하는 메서드
        """
        print('데이터 정렬 시작')

        for crawlingValue in self.crawlingCategory:
            dataName = crawlingValue[STR_NAME].replace('/', '_')  # 파일명에 사용할 수 없는 문자 대체
            crawlingDataPath = f'{dataName}.csv'
            
            # 크롤링된 파일이 존재하는지 확인
            if not os.path.exists(crawlingDataPath):
                print(f"파일을 찾을 수 없음: {crawlingDataPath}")
                continue

            crawl_dataList = list()
            dataList = list()

            try:
                # 크롤링된 데이터 파일 읽기
                with open(crawlingDataPath, 'r', newline='', encoding='utf-8-sig') as file:
                    csvReader = csv.reader(file)
                    for row in csvReader:
                        crawl_dataList.append(row)
            
                # 데이터가 비어있는지 확인
                if len(crawl_dataList) == 0:
                    print(f"데이터가 없음: {crawlingDataPath}")
                    continue
            
                # 데이터 저장 경로 확인 및 생성
                if not os.path.exists(DATA_PATH):
                    os.makedirs(DATA_PATH)
                
                dataPath = os.path.join(DATA_PATH, f'{dataName}.csv')

                # CSV 파일 초기화
                self.ResetCsv(dataPath)
            
                # 헤더 행 설정
                firstRow = ['Name', 'Spec']
                if len(crawl_dataList) > 0 and len(crawl_dataList[0]) > 0:
                    firstRow.append(crawl_dataList.pop(0)[0])
            
                # 데이터 리스트에 추가
                for product in crawl_dataList:
                    dataList.append(product)
            
                # 제품 이름으로 데이터 정렬
                dataList.sort(key=lambda x: x[0] if len(x) > 0 else '')
                
                # 정렬된 데이터를 CSV 파일에 쓰기
                with open(dataPath, 'w', newline='', encoding='utf-8') as file:
                    csvWriter = csv.writer(file)
                    csvWriter.writerow(firstRow)
                    for data in dataList:
                        csvWriter.writerow(data)
                
                # 원본 크롤링 파일 삭제
                if os.path.isfile(crawlingDataPath):
                    os.remove(crawlingDataPath)
                
                print(f"정렬 완료: {dataName}")

                # 정제 및 중복 제거 과정 수행
                self.remove_duplicates_and_units(dataPath, os.path.join(DATA_PATH, f"정제_중복제거_{dataName}.csv"))
                print(f"정제 및 중복 제거 완료: {dataName}")

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
        # 끝 부분의 숫자와 단위(ml, 정, 포 등)를 제거하는 패턴
        pattern = r'\s*\d+(?:\.\d+)?\s*(?:ml|정|포|l|g|kg|mg|개|can|캔|팩|페트|병|입|박스|캡슐|스틱|매|베지캡슐|분|da|달톤|mgα-te|mgne|㎍re|㎍|μg)(?:\s*x\s*\d+(?:개|팩|병|캔|박스|정|포|매|스틱)?)?\s*'
        
        # 괄호 안의 내용을 제거하는 패턴
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
        """
        입력 파일에서 중복을 제거하고 단위를 정제하여 출력 파일에 저장하는 메서드
        """
        unique_entries = defaultdict(list)

        # 입력 파일 읽기
        with open(input_file, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader, None)  # 헤더 행 읽기
            for row in reader:
                if row:
                    cleaned_name = self.clean_product_name(row[0])
                    key = cleaned_name.split(',')[0]
                    row[0] = cleaned_name  # 정제된 이름으로 업데이트
                    # 더 많은 정보를 가진 행을 유지
                    if key not in unique_entries or len(row) > len(unique_entries[key][0]):
                        unique_entries[key] = [row]

        # 정제된 데이터를 출력 파일에 쓰기
        with open(output_file, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            if header:
                writer.writerow(header)  # 헤더 행 쓰기
            for entries in unique_entries.values():
                writer.writerow(entries[0])

# 메인 실행 블록
if __name__ == '__main__':
    crawler = Crawler()  # 크롤러 인스턴스 생성
    crawler.StartCrawling()  # 크롤링 시작
    crawler.DataSort()  # 데이터 정렬, 정제, 중복 제거 수행
