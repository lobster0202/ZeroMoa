import requests
from bs4 import BeautifulSoup
import pandas as pd


class LinkCrawling:
    def __init__(self, url):
        self.url = url

    def get_html(self):

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
        }

        response = requests.get(url, headers=headers)

        return response.text

    def extract_links(self, html):

        soup = BeautifulSoup(html, 'html.parser')
        categories = []

        # 'cat_list' 클래스의 ul 요소 찾기
        cat_list = soup.find('ul', class_='cat_list')
        
        if cat_list is None:
            print("Error: 'cat_list' not found in the HTML.")
            return categories  # 빈 리스트 반환

        for item in cat_list.find_all('li', class_='main_item'):
            title = item.find('a').text.strip()
            link = item.find('a')['href']
            categories.append({'Name': title, 'URL': "https://prod.danawa.com" + link})

        return categories

    def save_to_csv(self, categories, filename='categories.csv'):
        df = pd.DataFrame(categories)
        df.to_csv(filename, index=False, encoding='utf-8-sig')

# 카테고리와 URL 리스트
urls = [
    ('건강식품/홍삼', 'https://prod.danawa.com/list/?cate=16253962'),
    ('헬스 다이어트 식품', 'https://prod.danawa.com/list/?cate=16254123'),
    ('생수/음료/우유', 'https://prod.danawa.com/list/?cate=1623153'),
    ('커피/차', 'https://prod.danawa.com/list/?cate=1623162'),
    ('축산/수산/건어물', 'https://prod.danawa.com/list/?cate=16216086'),
    ('조미료/양념/식용유', 'https://prod.danawa.com/list/?cate=16222254'),
    ('과자/초콜릿/시리얼', 'https://prod.danawa.com/list/?cate=16228134')
]

# 각 URL에 대해 링크 크롤링 수행
all_categories = []
for category, url in urls:
    link_crawler = LinkCrawling(url)
    html = link_crawler.get_html()
    categories = link_crawler.extract_links(html)
    all_categories.extend(categories)

# 모든 카테고리를 CSV로 저장
link_crawler.save_to_csv(all_categories)

print("링크 크롤링 완료")
