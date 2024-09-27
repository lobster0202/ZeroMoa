import requests
from bs4 import BeautifulSoup
import re
from fake_useragent import UserAgent
import time
import random


class CategoryCrawler : 
    def __init__(self, url):
        self.url = f'https://www.coupang.com/np/search?rocketAll=false&searchId=25e8c47413b3404b86e1d6417aceb194&q=%EC%A0%9C%EB%A1%9C&brand=&offerCondition=&filter=&availableDeliveryFilter=&filterType=&isPriceRange=false&priceRange=&minPrice=&maxPrice=&page={i}&trcid=&traid=&filterSetByUser=true&channel=user&backgroundColor=&searchProductCount=7454&component=194906&rating=0&sorter=scoreDesc&listSize=36'
        
        self.headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
        }

    def set_header() -> dict[str, str]:

        return {
            "User-Agent": UserAgent().random,
            "Accept-Language": "ko-KR,ko;q=0.8,en-US;q=0.5,en;q=0.3"
        }

    def crawling_waiting_time() -> None:
        return time.sleep(random.randint(1, 3))

    def product_link(self, category_id):
        response = requests.get(construct_url(category_id, 1), headers=set_header())
        response.raise_for_status()
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            page = soup.find('div', class_='product-list-paging')
            return int(page['data-total'])
        return 1

        
