
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
import re
# 找出所有子分類
# 爬子分類
def fecth_all_labels_name(url_=None, large_label=None):
    url = f'https://shop.prokits.com.tw/{url_}'
    res = requests.get(url=url)
    soup = BeautifulSoup(res.content)
    all_small_label_dict_ = {'分類網址':[], '第三層分類名稱':[]}
    for item in soup.find_all('ul', class_='panel-body'):
        all_small_label_dict_['分類網址'] = all_small_label_dict_['分類網址'] + [label.a['href'] for label in item.find_all('li')]
        all_small_label_dict_['第三層分類名稱'] = all_small_label_dict_['第三層分類名稱'] + [label.a['title'] for label in item.find_all('li')]
    df = pd.DataFrame(all_small_label_dict_)
    df['第一層分類'] = large_label
    return df


# 爬子分類的所有商品url
def fetch_all_p_url(url=None):
    res = requests.get(url=url)
    soup = BeautifulSoup(res.content)
    # 確認頁數
    page = int(re.findall('(?:[共<b>])[0-9]+', str(soup.find('span', class_='btn-group page')))[0].replace('>', ''))
    p_son = [f"https://shop.prokits.com.tw{item['href']}" for item in soup.find('div', class_='row proList-img-xs-4').find_all('a')]
    for i in range(2, page+1):
        url = url.replace('?P=1', f'?P={i}')
        res = requests.get(url=url)
        soup = BeautifulSoup(res.content)
        p_son += [f"https://shop.prokits.com.tw{item['href']}" for item in soup.find('div', class_='row proList-img-xs-4').find_all('a')]
    return p_son


# 商品名稱
def fetch_p_info(url=None):
    res = requests.get(url=url)
    soup = BeautifulSoup(res.content)
    # 商品名稱
    p_name = soup.find('h1').text
    # second_label
    label = soup.find_all('span', itemprop='name')[1].text
    # 原廠型號
    p_ori_no = soup.find('small', class_='pro-code').text
    # 商品簡介
    p_desc = str(soup.find('h2', class_='description')).replace('<h2 class="description" itemprop="description">', '').replace(' </h2>', '').strip().replace('<br/>', '\n')
    # 價格
    p_price = soup.find('span', class_='price').text.strip()
    # 商品圖片網址
    p_url = f"https://shop.prokits.com.tw/{soup.find('div', class_='lightboxEnlarge-s').img['src']}"
    # 商品內文
    p_content = str(soup.find('div', class_='tab-content')).replace('\n', '').replace('\t', '').replace('\xa0', '').replace('\r', '')
    return {'商品名稱': p_name, '第二層分類': label, '原廠型號': p_ori_no, '商品簡介': p_desc, '價格': p_price, '商品圖片網址': p_url, '商品內文':p_content, '網址': url}

def main():
    # 除了玩具類，其他商品全爬)
    url = 'https://shop.prokits.com.tw/'
    res = requests.get(url=url)
    soup = BeautifulSoup(res.content)
    # 找出要爬的大分類
    dict_ = {}
    for i in range(2, 8):
        dict_[soup.find('li', class_=f'side-nav-list menu{i}').a.text.strip()] = soup.find('li', class_=f'side-nav-list menu{i}').a['href']
    df = pd.DataFrame()
    for key, value in dict_.items():
        df_prev = fecth_all_labels_name(url_=value, large_label=key)
        df = pd.concat([df, df_prev])
    lst_data = []
    for index, row in df.iterrows():
        # 爬子分類的所有商品url
        url = f"https://shop.prokits.com.tw/{row['分類網址']}?P=1&showMode=pic&SortMode="
        p_son = fetch_all_p_url(url=url)
        for son in p_son:
            dict_prev = fetch_p_info(url=son)
            dict_prev['第一層分類'] = row['第一層分類']
            dict_prev['第三層分類名稱'] = row['第三層分類名稱']
            lst_data.append(dict_prev)
    df_ok = pd.DataFrame(lst_data)
    df_ok[['第一層分類', '第二層分類', '第三層分類名稱', '商品名稱', '原廠型號', '商品簡介', '價格', '商品圖片網址', '商品內文', '網址']].to_excel('2023-02-10寶工爬蟲.xlsx')


if __name__ == '__main__':
    main()
