# -*- coding: utf-8 -*-

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
DATA_PATH = 'crawl_data'
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

class DanawaCrawler:
    def __init__(self):
        """
        초기화 메서드.
        - 오류 목록과 크롤링할 카테고리 목록을 초기화합니다.
        - CrawlingCategory.csv 파일을 읽어 카테고리 이름과 URL을 로드합니다.
        """
        self.errorList = list()  # 크롤링 중 발생한 오류를 저장할 리스트
        self.crawlingCategory = list()  # 크롤링할 카테고리 정보를 저장할 리스트
        
        # CrawlingCategory.csv 파일을 읽어 크롤링할 카테고리 목록을 로드
        with open(CRAWLING_DATA_CSV_FILE, 'r', newline='') as file:
            for crawlingValues in csv.reader(file, skipinitialspace=True):
                # 주석 처리된 줄(//으로 시작하는 줄)은 무시
                if not crawlingValues[0].startswith(DATA_REMARK):
                    # 카테고리 이름과 URL을 딕셔너리 형태로 리스트에 추가
                    self.crawlingCategory.append({STR_NAME: crawlingValues[0], STR_URL: crawlingValues[1]})

    def StartCrawling(self):
        """
        크롤링을 시작하는 메서드.
        - Chrome 브라우저 옵션을 설정합니다.
        - 멀티프로세싱을 사용하여 각 카테고리를 병렬로 크롤링합니다.
        """
        self.chrome_option = webdriver.ChromeOptions()
        self.chrome_option.add_argument('--headless')  # 헤드리스 모드 (UI 없이 실행)
        self.chrome_option.add_argument('--window-size=1920,1080')  # 창 크기 설정
        self.chrome_option.add_argument('--start-maximized')  # 최대화 옵션
        self.chrome_option.add_argument('--disable-gpu')  # GPU 사용 비활성화
        self.chrome_option.add_argument('lang=ko=KR')  # 브라우저 언어 설정
        
        if __name__ == '__main__':
            # 멀티프로세싱 풀 생성
            pool = Pool(PROCESS_COUNT)
            # CrawlingCategory 메서드를 각 카테고리에 병렬로 적용
            pool.map(self.CrawlingCategory, self.crawlingCategory)
            pool.close()
            pool.join()

    def CrawlingCategory(self, categoryValue):
        """
        각 카테고리를 크롤링하는 메서드.
        - 주어진 카테고리 URL로 이동하여 제품 데이터를 수집하고 CSV 파일로 저장합니다.
        """
        crawlingName = categoryValue[STR_NAME]  # 카테고리 이름
        crawlingURL = categoryValue[STR_URL]  # 카테고리 URL

        print('Crawling Start : ' + crawlingName)

        # 크롤링 데이터를 저장할 CSV 파일 열기
        crawlingFile = open(f'{crawlingName}.csv', 'w', newline='', encoding='utf8')
        crawlingData_csvWriter = csv.writer(crawlingFile)
        # 첫 번째 행에 현재 날짜와 시간 기록
        crawlingData_csvWriter.writerow([self.GetCurrentDate().strftime('%Y-%m-%d %H:%M:%S')])
        
        try:
            # Chrome 브라우저 초기화 및 페이지 로드
            browser = webdriver.Chrome(service=Service(CHROMEDRIVER_PATH), options=self.chrome_option)
            browser.implicitly_wait(5)  # 암묵적 대기 설정
            browser.get(crawlingURL)  # 크롤링할 카테고리 페이지로 이동

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

                    # 광고 제품 제외 (클래스에 'prod_ad_item'이 있거나 ID가 'ad'로 시작)
                    if 'prod_ad_item' in product.get_attribute('class').split(' '):
                        continue
                    if product.get_attribute('id').strip().startswith('ad'):
                        continue

                    # 제품 이름 추출
                    productName = product.find_element(By.XPATH,'./div/div[2]/p/a').text.strip()
                    # 제품 가격 관련 요소 추출
                    productPrices = product.find_elements(By.XPATH,'./div/div[3]/ul/li')
                    
                    for productPrice in productPrices:
                        # 숨겨진 요소 표시 (style에 'display: none'이 있으면 표시하도록 변경)
                        if 'display: none' in productPrice.get_attribute('style'):
                            browser.execute_script("arguments[0].style.display = 'block';", productPrice)

                        # 제품 타입 추출 (예: RAM, HDD, SSD 등)
                        productType = productPrice.find_element(By.XPATH,'./div/p').text.strip()
                        # 순위 텍스트 제거 (예: '1위 HDD' → 'HDD')
                        productType = self.RemoveRankText(productType)

                        # 제품 타입을 리스트로 분리 (예: '6TB\n25원/1GB' → ['6TB', '25원/1GB'])
                        if '\n' in productType:
                            productTypeList = productType.split('\n')
                        else:
                            productTypeList = [productType, ""]

                        # 판매처(Mall) 추출
                        mall = productPrice.find_element(By.XPATH,'./p[1]').text.strip()   
                        # 가격 추출 및 쉼표 제거
                        price = productPrice.find_element(By.XPATH,'./p[2]/a/strong').text.replace(",","").strip()

                        # 제품 ID 추출 (ID 속성의 18번째 문자 이후)
                        productId = productPrice.get_attribute('id')[18:]

                        # CSV 파일에 제품 정보 기록
                        crawlingData_csvWriter.writerow([productId, productName, productTypeList[0], price, mall, productTypeList[1]])

        except Exception as e:
            # 예외 발생 시 오류 메시지 출력 및 오류 목록에 추가
            print('Error - ' + crawlingName + ' ->')
            print(traceback.format_exc())
            self.errorList.append(crawlingName)

        # CSV 파일 닫기
        crawlingFile.close()

        print('Crawling Finish : ' + crawlingName)

    def RemoveRankText(self, productText):
        """
        제품 타입 텍스트에서 순위 정보를 제거하는 메서드.
        예: '1위 HDD' → 'HDD'
        """
        if len(productText) < 2:
            return productText
        
        char1 = productText[0]
        char2 = productText[1]

        # 첫 두 글자가 숫자와 '위'로 시작하면 순위 정보로 간주
        if char1.isdigit() and (1 <= int(char1) and int(char1) <= 9):
            if char2 == '위':
                # 세 번째 문자가 '\n'인 경우를 처리
                if not len(productText) < 3:
                    char3 = productText[2]
                    if char3 == '\n':
                        return productText[3:].strip()

                return productText[2:].strip()
        
        return productText

    def DataSort(self):
        """
        크롤링된 데이터를 정렬하는 메서드.
        - 각 카테고리별 CSV 파일을 읽어 ID 기준으로 정렬한 후 다시 저장합니다.
        """
        print('Data Sort')

        for crawlingValue in self.crawlingCategory:
            dataName = crawlingValue[STR_NAME]
            crawlingDataPath = f'{dataName}.csv'
            
            # 해당 카테고리의 CSV 파일이 없으면 건너뜀
            if not os.path.exists(crawlingDataPath):
                continue

            crawl_dataList = list()  # 원본 데이터 리스트
            dataList = list()  # 정렬된 데이터 리스트

            # CSV 파일 읽기
            with open(crawlingDataPath, 'r', newline='', encoding='utf8') as file:
                csvReader = csv.reader(file)
                for row in csvReader:
                    crawl_dataList.append(row)
            
            # 데이터가 없으면 건너뜀
            if len(crawl_dataList) == 0:
                continue
            
            # 정렬된 데이터를 저장할 경로 설정
            dataPath = f'{DATA_PATH}/{dataName}.csv'

            # 저장할 경로에 파일이 없으면 새로 생성
            if not os.path.exists(dataPath):
                file = open(dataPath, 'w', encoding='utf8')
                file.close()

            # 기존 데이터를 초기화
            self.ResetCsv(dataPath)
            
            # 첫 번째 행에 헤더 추가
            firstRow = ['Id', 'Name', 'Type', 'Price', 'Mall']
            firstRow.append(crawl_dataList.pop(0)[0])  # 첫 번째 행에 날짜 및 시간 추가
            
            for product in crawl_dataList:
                # ID가 숫자가 아닌 경우 건너뜀
                if not str(product[0]).isdigit():
                    continue
                
                # 새 데이터 리스트 생성
                newDataList = []
                for i in range(0, len(product)):
                    if i == 0:
                        # ID를 정수로 변환
                        newDataList.append(int(product[i]))
                    else:
                        newDataList.append(product[i])

                dataList.append(newDataList)
            
            # ID 기준으로 오름차순 정렬
            dataList.sort(key= lambda x: x[0])
                
            # 정렬된 데이터를 CSV 파일에 기록
            with open(dataPath, 'w', newline='', encoding='utf8') as file:
                csvWriter = csv.writer(file)
                csvWriter.writerow(firstRow)
                for data in dataList:
                    csvWriter.writerow(data)
                file.close()
                
            # 원본 CSV 파일 삭제
            if os.path.isfile(crawlingDataPath):
                os.remove(crawlingDataPath)

    def ResetCsv(self, crawlingDataPath):
        """
        주어진 경로의 CSV 파일을 비우는 메서드.
        """
        with open(crawlingDataPath, 'w', newline='') as file:
            csvWriter = csv.writer(file)
            csvWriter.writerows([])

    def GetCurrentDate(self):
        """
        현재 날짜와 시간을 반환하는 메서드.
        - 설정된 시간대(Asia/Seoul)를 사용합니다.
        """
        tz = timezone(TIMEZONE)
        return datetime.now(tz)

    def CreateIssue(self):
        """
        크롤링 중 발생한 오류를 GitHub 이슈로 생성하는 메서드.
        - 현재 주석 처리되어 있으며, 활성화 시 GitHub 연동이 필요합니다.
        """
        if len(self.errorList) > 0:
            g = Github(os.environ[GITHUB_TOKEN_KEY])  # 환경 변수에서 GitHub 토큰 가져오기
            repo = g.get_repo(GITHUB_REPOSITORY_NAME)  # 지정된 저장소 접근
            
            title = f'Crawling Error - ' + self.GetCurrentDate().strftime('%Y-%m-%d')
            body = ''
            for err in self.errorList:
                body += f'- {err}\n'
            labels = [repo.get_label('bug')]
            repo.create_issue(title=title, body=body, labels=labels)
        
    def CsvToXlsx(self):
        """
        크롤링된 CSV 데이터를 엑셀 파일로 변환하는 메서드.
        - 현재 주석 처리되어 있으며, openpyxl 라이브러리가 필요합니다.
        """
        print('.csv to .xlsx')

        xlsxDataPath = f'{DATA_PATH}/data.xlsx'
        wb = openpyxl.load_workbook(xlsxDataPath)

        for crawlingValue in self.crawlingCategory:
            dataName = crawlingValue[STR_NAME]
            csvDataPath = f'{DATA_PATH}/{dataName}.csv'
            
            # CSV 파일이 없으면 건너뜀
            if not os.path.exists(csvDataPath):
                continue
            if not os.path.exists(xlsxDataPath):
                print(xlsxDataPath + ' pass')
                wb = openpyxl.Workbook()
                wb.save(xlsxDataPath)
                wb.close()

            # 기존 시트가 있으면 삭제
            if dataName in wb.sheetnames:
                wb.remove(wb[dataName])

            # 새 시트 생성
            wb.create_sheet(title=dataName)
            ws = wb[dataName]

            # CSV 파일을 읽어 엑셀 시트에 기록
            with open(csvDataPath, 'r', encoding='utf-8') as file:
                reader = csv.reader(file)
                for r_idx, row in enumerate(reader, 1):
                    for c_idx, value in enumerate(row, 1):
                        ws.cell(row=r_idx, column=c_idx, value=value)

            # 엑셀 파일 저장
            wb.save(xlsxDataPath)
            
        wb.close()

    def CsvToGspread(self):
        """
        크롤링된 CSV 데이터를 Google Sheets로 업로드하는 메서드.
        - 현재 주석 처리되어 있으며, gspread 및 oauth2client 라이브러리가 필요합니다.
        """
        print('.csv to gspread')

        try:
            # Google Sheets 인증 설정
            scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
            creds = ServiceAccountCredentials.from_json_keyfile_name(SHEET_KEYFILE, scope)
            client = gspread.authorize(creds)

            spreadsheet = client.open(SHEET_NAME)

            sheetList = spreadsheet.worksheets()

        except Exception as e:
            # 인증 실패 시 메서드 종료
            return

        for crawlingValue in self.crawlingCategory:

            dataName = crawlingValue[STR_NAME]
            csvDataPath = f'{DATA_PATH}/{dataName}.csv'
        
            # CSV 파일이 없으면 건너뜀
            if not os.path.exists(csvDataPath):
                continue

            # 시트 존재 여부 확인 후 초기화 또는 생성
            if any(sheet.title == dataName for sheet in spreadsheet.worksheets()):
                spreadsheet.worksheet(dataName).clear()
            else:
                spreadsheet.add_worksheet(title=dataName, rows='1', cols='1')

            worksheet = spreadsheet.worksheet(dataName)

            # CSV 파일을 읽어 Google Sheets에 추가
            with open(csvDataPath, 'r', encoding='utf-8') as file:
                reader = csv.reader(file)
                
                for row in reader:
                    while True:
                        try:
                            worksheet.append_row(row)
                            break
                        except gspread.exceptions.APIError as e:
                            if "quota exceeded" in str(e).lower():
                                print("API 제한 초과. 대기 중...")
                                sleep(10)  # 10초 대기 후 재시도
                            else:
                                raise e

    def CsvToGoogleDrive(self):
        """
        크롤링된 CSV 데이터를 Google Drive에 업로드하는 메서드.
        - 현재 주석 처리되어 있으며, googledrive 모듈이 필요합니다.
        """
        print('.csv to googledrive')
        service = googledrive.connectDrive()
        for crawlingValue in self.crawlingCategory:
            dataName = f"{crawlingValue[STR_NAME]}.csv"
            googledrive.uploadFile(service, dataName)

# 메인 실행 블록
if __name__ == '__main__':
    crawler = DanawaCrawler()  # 크롤러 인스턴스 생성
    crawler.StartCrawling()  # 크롤링 시작
    crawler.DataSort()  # 데이터 정렬
    crawler.CreateIssue()  # 오류 이슈 생성 (현재 주석 처리됨)
    # crawler.CsvToXlsx()  # CSV를 엑셀로 변환 (현재 주석 처리됨)
    # crawler.CsvToGspread()  # CSV를 Google Sheets로 업로드 (현재 주석 처리됨)
    # crawler.CsvToGoogleDrive()  # CSV를 Google Drive로 업로드 (현재 주석 처리됨)
