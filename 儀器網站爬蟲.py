
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
import random
from bs4 import BeautifulSoup
import string
import re
import multiprocessing as mp
TZ = timezone(timedelta(hours=+8))

class ChuanHuaScrapyAllSite(object):
    def __init__(self):
        self.update = datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")
        self.creater = 106
        self.conn = ''
        self.conn_select = ''
        self.df = pd.DataFrame()

    def fetch_all_brand(self):
        domain = 'https://www.chuanhua.com.tw/shop/'
        res = requests.get(url=domain)
        soup = BeautifulSoup(res.content, 'lxml')
        lst = []
        for data in soup.find('div', class_='shop-container').find_all('div', class_='col-inner'):
            lst.append(data.a['href'])
        return lst
    
    def fetch_all_mom_product_page(self):
        for brand_url in self.fetch_all_brand():
            res = requests.get(url=brand_url)
            soup = BeautifulSoup(res.content, 'lxml')
            # 得到媽媽頁
            for mom_page in soup.find('div', class_='shop-container').find_all('a'):
                if 'product-category' in mom_page['href']:
                    continue
                else:
                    try:
                        dict_prev = self.fetch_mom_product_info(url=mom_page['href'])
                        df_prev = pd.DataFrame(dict_prev, index=[0])
                        self.df = pd.concat([self.df, df_prev])
                    except:
                        print(mom_page['href'])
            print(brand_url, 'ok')
            self.df.to_excel('全華爬蟲測試.xlsx')

    def fetch_mom_product_info(self, url=None):
        res = requests.get(url=url)
        soup = BeautifulSoup(res.content, 'lxml')
        dict_ = {}
        # 媽媽圖片網址
        img_url = soup.find('figure').a.img['data-src']
        # 找出品牌
        try:
            brand = soup.find('div', class_='product-info summary entry-summary col col-fit product-summary text-center').a.text
            dict_['品牌'] = brand
        except:
            dict_['品牌'] = ''
        # 找出群組名稱
        mom_name = soup.find('h1').text.strip()
        # 找出短描述
        mom_desc = soup.find('div', class_='product-short-description').text.strip()
        # 找出所有內容
        all_content = str(soup.find('div', id='tab-description'))
        # 排除不需要的內容
        content = all_content.replace(str(soup.find('div', class_='row align-center')), '').replace('\n', '')
        # 找出分類
        label = soup.find('span', class_= 'posted_in').a.text
        # 找出標籤
        try:
            tags = soup.find('span', class_= 'tagged_as').text.replace('標籤: ', '')
            dict_['原網站標籤'] = tags
        except:
            dict_['原網站標籤'] = ''
        # 得到兒子規格
        spec_ = ''
        if soup.find('table'):
            df_table = pd.read_html(res.content)[0]
            spec_ += df_table.to_string()
        dict_['群組頁網址'] = url
        dict_['群組名稱'] = mom_name
        dict_['商品群組簡介'] = mom_desc
        dict_['商品群組介紹'] = content
        dict_['圖片網址'] = img_url
        dict_['分類'] = label
        dict_['未分類規格'] = spec_
        return dict_ 


def get_img_from_url(url, local_path=None):
    # 下載圖片
    response = requests.get(url)
    # 儲存圖片
    with open(local_path, 'wb') as f:
        f.write(response.content)


if __name__ == '__main__':
    # main(merck_scapy)
    obj = ChuanHuaScrapyAllSite()
    obj.fetch_all_mom_product_page()
