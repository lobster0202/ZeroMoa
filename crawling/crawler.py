import httpx
from bs4 import BeautifulSoup
import pandas as pd
import random
import time
from fake_useragent import UserAgent

class CategoryCrawler:
    def __init__(self, url):
        self.url = 'https://www.coupang.com/' + url  # URL 앞에 coupang.com 추가

    def crawling(self):
        headers = set_header()
        
        # 클라이언트 설정
        client = httpx.Client(headers=headers, timeout=120)  # 타임아웃을 120초로 증가
        
        max_retries = 3  # 최대 재시도 횟수
        for attempt in range(max_retries):
            try:
                response = client.get(self.url)
                response.raise_for_status()  # HTTP 에러가 발생하면 예외를 발생시킴
                break  # 성공하면 루프 탈출
            except (httpx.RequestError, httpx.HTTPStatusError) as e:
                print(f"Attempt {attempt + 1} failed: {e}")
                if attempt == max_retries - 1:
                    print("Max retries reached. Exiting.")
                    return
                crawling_waiting_time()  # 재시도 전 대기 시간 추가
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        product_list = soup.find('ul', {'id': 'productList'})
        if product_list:
            data = []
            for item in product_list.find_all('li', class_='search-product'):
                title = item.find('div', class_='name').get_text(strip=True)
                link = "https://www.coupang.com" + item.find('a', class_='search-product-link')['href']
                data.append({'Title': title, 'Link': link})
            
            # 데이터프레임 생성 및 CSV 저장
            df = pd.DataFrame(data)
            df.to_csv('products.csv', index=False, encoding='utf-8-sig')
            print("CSV 파일이 성공적으로 저장되었습니다.")
        else:
            print("Product list not found")

def crawling_waiting_time() -> None:
    return time.sleep(random.randint(1, 3))

def set_header() -> dict[str, str]:
    return {
        "User-Agent": UserAgent().random,
        "Accept-Language": "ko-KR,ko;q=0.8,en-US;q=0.5,en;q=0.3"
    }

# 사용 예시
crawler = CategoryCrawler('np/search?rocketAll=false&searchId=25e8c47413b3404b86e1d6417aceb194&q=%EC%A0%9C%EB%A1%9C&brand=&offerCondition=&filter=&availableDeliveryFilter=&filterType=&isPriceRange=false&priceRange=&minPrice=&maxPrice=&page=1&trcid=&traid=&filterSetByUser=true&channel=user&backgroundColor=&searchProductCount=7454&component=194906&rating=0&sorter=scoreDesc&listSize=36')
crawler.crawling()