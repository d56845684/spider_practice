
import pathlib
import sys
import pandas as pd
sys.path.append(str(pathlib.Path().resolve().parent))
sys.path.append('..')
from datetime import datetime, timezone, timedelta
import numpy as np
from utils_func.helpers import MySqlConnHelper_executemany
from utils_func.helpers import MySqlConnHelper as MySqlConnHelper_for_one
from datetime import datetime, timezone, timedelta
import requests
import json
from bs4 import BeautifulSoup
conn = MySqlConnHelper_executemany('sciket.com')
conn_select = MySqlConnHelper_for_one('sciket.com')
import time
TZ = timezone(timedelta(hours=+8))

def get_label_from_website(soup=None):
    address = []
    website = []
    tel = []
    name = []
    url = []
    ori_url = []
    email = []
    company = []
    categories = []
    business_style = []
    main_market = []
    import_market = []
    # 公司地址
    for com in soup.find_all('div', class_='boxlistings'):
        address.append(com.find('div', class_='address_listings').find('p', class_='diachisection').text)
    # 公司電話
    for com in soup.find_all('div', class_='boxlistings'):
        tel.append(com.find('div', class_='address_listings').find('p', class_='thoaisection').text.strip())
    # 公司名稱
    for com in soup.find_all('div', class_='boxlistings'):
        website.append(com.find('h2', class_='company_name').a['href'])
        company.append(com.find('h2', class_='company_name').text)
    # 郵件
    for com in soup.find_all('div', class_='boxlistings'):
        try:
            email.append(com.find('div', class_='email_text').a['title'])
        except:
            email.append('')
        try:
            ori_url.append(com.find('div', class_='website_text').a['href'])
        except:
            ori_url.append('')
    for com in soup.find_all('div', class_='boxlistings'):
        block = com.find('div', style='height:auto; width:468px; margin-left:39px').text
        # Categories
        try:
            categories.append(block.split('\n\n\n\n')[0].split(':\n')[1])
        except:
            categories.append('')
        # business style
        try:
            business_style.append(block.split('\n\n\n\n')[1].split(':\n')[1])
        except:
            business_style.append('')
        # Main markets
        try:
            main_market.append(block.split('\n\n\n\n')[2].split(':\n')[1])
        except:
            main_market.append('')
        # Import markets
        try:
            import_market.append(block.split('\n\n\n\n')[3].split(':\n')[1])
        except:
            import_market.append('')
    df = pd.DataFrame({'公司地址':address, '公司電話':tel, '公司名稱': company, 'yellow的連結網址': website, '公司窗口郵件': email,
     '公司網址': ori_url, 'Categories':categories, 'Business styles':business_style, 'Main markets':main_market, 'Import markets':import_market})
    df['分類'] = soup.find('h1').text
    return df

def parse_product():
    lst = ['Chemicals', 'Supplies', 'Furniture','Equipment', 'Laboratory', 'Laboratories', 'Instrument', 'Tools', 'Instruments']
    # lst = ['Laboratory', 'Laboratories', 'Instrument', 'Tools', 'Instruments']
    with pd.ExcelWriter(f'/Users/dennis/sciket_/dennis_test/越南黃頁爬蟲_v3.xlsx') as writer:
        for keyword in lst:
            print(keyword, 'start')
            url_begin = f'https://www.yellowpages.com.vn/srch/vietnam/{keyword}.html?'
            res_begin = requests.get(url=url_begin)
            soup_begin = BeautifulSoup(res_begin.content)
            p_number = round(int(soup_begin.find('div', style='width:auto; float:left; margin-left:6px').text.replace(' Listings)', '').replace('(', '')) / 25) + 1
            df_key_word = pd.DataFrame()
            for i in range(1, p_number+1):
                url = f'https://www.yellowpages.com.vn/srch/vietnam/{keyword}.html?page={i}'
                print(url)
                res = requests.get(url=url)
                soup = BeautifulSoup(res.content)
                df = get_label_from_website(soup=soup)
                df_key_word = pd.concat([df_key_word, df])
                time.sleep(1)
            df_key_word.to_excel(writer, sheet_name=keyword, engine='xlsxwriter', index=False)
            print(keyword, 'ok')


if __name__ == '__main__':
    # df = pd.read_excel('2023-01-17_Merck新上架.xlsx', index_col=0)
    # df = df[['供應商料號', '科研市集型號(科研市集填寫)']]
    # merck_scrapy(df.sample(10), 0, 10, cookie=COOKIE)
    parse_product()
