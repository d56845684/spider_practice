
import pathlib
import sys
import pandas as pd
sys.path.append(str(pathlib.Path().resolve().parent))
sys.path.append('..')
from datetime import datetime, timezone, timedelta
import numpy as np
from datetime import datetime, timezone, timedelta
import requests
import json
from bs4 import BeautifulSoup
import time
TZ = timezone(timedelta(hours=+8))


def main():
    url = 'https://www.monotaro.tw'
    res = requests.get(url=url)
    soup = BeautifulSoup(res.content)
    url_lst = []
    for b_cate in soup.find_all('div', class_='level2 third'):
        for s_cate in b_cate.find_all('a'):
            url = f"https://www.monotaro.tw/{s_cate['href']}"
            url_lst.append(url)
    dict_ = {'分類網址':[], '分類名稱':[], '分類商品數量':[], '分類所有階層':[]}
    for cat in url_lst:
        url = f"{cat}#block-visual-category"
        res = requests.get(url=url)
        soup = BeautifulSoup(res.content)
        # 階層
        layers = soup.find_all('ul', class_='items')[0].text.strip().replace('   ', ' --> ')
        for label in soup.find_all('div', class_='visual-category-block'):
            # 所有第四層url
            url_end = label.a['href']
            # 所有第四層分類名稱
            title = label.find('div', class_='visual-category-title').text
            # 所有第四層分類商品數量
            num = re.findall('[0-9]+', label.find('div', class_='visual-category-image').text)[0]
            dict_['分類網址'].append(url_end)
            dict_['分類名稱'].append(title)
            dict_['分類商品數量'].append(num)
            dict_['分類所有階層'].append(f'{layers} --> {title}')
        print(cat, 'ok')


if __name__ == '__main__':
    # df = pd.read_excel('2023-01-17_Merck新上架.xlsx', index_col=0)
    # df = df[['供應商料號', '科研市集型號(科研市集填寫)']]
    # merck_scrapy(df.sample(10), 0, 10, cookie=COOKIE)
    main()
