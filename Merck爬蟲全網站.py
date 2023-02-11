
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

class MerckScrapyAllSite(object):
    def __init__(self, key_word=None):
        """
        傳入關鍵字並回傳該關鍵字所有的爬蟲結果
        """
        self.update = datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")
        self.creater = 106
        self.conn = ''
        self.conn_select = ''
        self.key_word = key_word
        self.headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7',
            'cache-control': 'max-age=0',
            'cookie': '_ga=GA1.2.2090798484.1586327672; sid=KX6HiJuR2U6giMUEf4eWAHXEkPhF1wCC0hDmqBYL_fHzYhP_8bzomC8NWQ4Qjvlp-rqpkUjn6kN-IDd-9YZ3V34dS98Dgb_GTlhoH-QdmRaamJHq8qdp0Lj9u_C1bpDi1B4-DlnO6kN-IFis0NY-DlnO; pgid-Merck-TW-Site=JLtMKfaYL0NSRpEo11xgbI750000gjRA-6Kj; _gid=GA1.2.572423511.1586845435; FORMASSEMBLY=da75d916f30e55d841bfa88c5eae6701; JabmoSP0ses.af87=*; JabmoSP0id.af87=3dc57dd2-d2d1-462b-95dd-f845d5e77b24.1586328248.7.1586856602.1586853576.8e3c3156-c6da-4445-a758-c90452b0bc34; _4c_=hVJdb5swFP0rFQ99isFf2CZSNZGEVJ3Udv3YurcIsBNYADPjhH4o%2F30mIqm0PYwXfO459%2FjK93x4faEab4pCwUTIGMQk5BNvq946b%2FrhmVIOv7039RiSLFsTBRBHFFAeZSAVFAGECOMw41kqmTfxXgevCEGBWYQJCQ8TL29Hjw9vZypnVVjbdtMg6Pver5XJt3VZVWWrjfJzXQfPL8F7EbRGy11ug7ixJXjamXWa27Sx4JvRVpUNWICBybR8A3GlXlOwrHbaACoEmOvm126TWjW5vV3dLUE8ewo5BfHSkW7EXEvlpkCRL3zqsH13iArojuOlK%2FvWDpJeZRed3DpCqn2Zq1VfSlscezH8rBaq3BR2KMOji2zNANypLxup%2B7%2Fbxuq5LSLcVTOj%2B04NnfPC6FpdHL2024P3cmzoHDRqrYw5qhzqSjuM%2Be8bjpxbo6NVLc%2BkI%2B5vnx9XsySe39%2BNu3CrOFpsjN61viR%2Bl%2Fu6tkbmfqNskAVdF9TbTV1vKp2lVZXumrzAENEABV%2BfAPKZj4IOc8TCEHLMKOLsS%2Fwwu0KX%2B1Je%2FXhdlpvf92axbe6aZLstft7gtGnX9nohH4t3c7l2IhImdMlEEiaccDpjAPN5hPAyERQzAmF4GT8kV8ObtkMa0ZC163j1%2FWbhEIYR5JGggvpDkAnmjGPvMKZRsIhgGiIaQbds6xIoGIXD5xT78hTwSGZ0rWQEVBZJQNMwBUJABjKYOwMiM4K4d7YUlAtnKfhoicTJsa1GR%2FQpDiNBKAvJSUzP97f7k5r9f9zD4Q8%3D',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'none',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36'
        }
        self.alphabet = list(string.ascii_lowercase)
        self.label_name = None
        self.info_ouput = {}
        self.df = pd.DataFrame()

    def fetch_url_search_page(self):
        open = f'https://www.merckmillipore.com/Web-TW-Site/zh_TW/-/TWD/ViewParametricSearch-ToggleFilter?SearchParameter=%26%40QueryTerm%3D{self.key_word}%26channels%3DTW_or_GLOBAL%26%40P.MERCK_FF.defaultSimilarity%3D9000%26%40GroupSize.FilteredMultiCategoryUUIDs_ProductType%3D0%26%40Page%3D1&PageSize=10&ShowCount=true&ShowSelected=false&SearchContextPageletUUID=&SelectedSearchResult=SFProductSearch&SearchTerm={self.key_word}&TrackingSearchType=filter&FilterName=FilteredMultiCategoryUUIDs_ProductType'
        res = requests.get(url=open, headers=self.headers)
        soup = BeautifulSoup(res.content, 'lxml')
        return soup
    
    def fetch_all_product_info(self):
        soup = self.fetch_url_search_page()
        for url in soup.find_all('div', id='FilterGroup_FilteredMultiCategoryUUIDs_ProductType'):
            for _ in (url.form.ul.findAll('li', {'class':'filter ish-pageNavigation-filters-filter-layer3'})):
                self.label_name = _.label.text
                label_name_url = _.input['data-document-location']
                self.get_search_result_page(url_begin=label_name_url)
        self.df.to_excel(f'test{self.key_word}_Merck_(爬完).xlsx')

    def get_search_result_page(self, url_begin=None):
        url_begin = url_begin.replace('PageSize=10', 'PageSize=100').replace('SelectedSearchResult=SFProductSearch', 'SelectedSearchResult=SFProductSearch&PageNumber=0')
        res_table = requests.get(url_begin, headers=self.headers)
        soup = BeautifulSoup(res_table.content, 'lxml')
        # 找出網頁數量
        if soup.find('span',{'class':'selected-text'}):
            page_result = int(int(''.join(re.findall('[0-9]', soup.find('span',{'class':'selected-text'}).text)))/ 100) + 1
            for page in range(page_result):
                # print(f'第{page}')
                new_page = f"PageNumber={page}"
                url_page = url_begin.replace('PageNumber=0', new_page)
                res_table = requests.get(url_page, headers=self.headers)
                soup = BeautifulSoup(res_table.content, 'lxml')
                try:
                    for url in soup.find_all('h2', {'class':"search-r-title"}):
                        self.get_page_info(url=url.a['href'])
                except Exception as e:
                    print(f'{e}, {url.a["href"]}最終頁')
            self.df.to_excel(f'test{self.key_word}_Merck_test.xlsx')
    def get_page_info(self, url=None):
        def get_merge_mom_page(soup=None):
            for url in soup.find('div', {'class':'orderdingtable_mark'}).find_all('a', {'class':"name-link"}):
                self.get_page_info(url=url['href'])
        vendor_p_no = url.split(',')[1]
        res_table_test = requests.get(url, headers=self.headers)
        soup = BeautifulSoup(res_table_test.content, 'lxml')
        if soup.find('section', {'class':'tabs'}).ul.find('li').find('a', {'id':"product_detail_overview_tab_a"}):
            self.info_ouput = {}
            span1 = soup.find('span', 'pdp')
            brand = span1.text.split(' ')[1].strip()
            original_no = span1.text.split(' ')[0].strip()
            mom_name = soup.find('header', id='pdp-sort-description-for-print').text.replace(span1.text, '').strip()
            self.info_ouput['分類'] = self.label_name
            self.info_ouput['供應商料號'] = vendor_p_no
            self.info_ouput['品牌'] = brand
            self.info_ouput['原廠型號'] = original_no
            self.info_ouput['商品群組名稱'] = mom_name
            self.info_ouput['原廠網站連結(若是有建議提供)'] = url
            self.info_ouput['網址'] = url
            try:
                self.process_product_info(res=res_table_test, soup=soup)
            except Exception as e:
                print(f'{e}, {url}')
            df_prev = pd.DataFrame(self.info_ouput, index=[0])
            self.df = pd.concat([self.df, df_prev])
            # print(self.label_name, vendor_p_no, brand, original_no, mom_name)
        else:
            # print('聚集頁需要往下爬')
            get_merge_mom_page(soup=soup)
    
    def process_product_info(self, res=None, soup=None):
        mom_short_desc = soup.find('h2', id='desc_short').text
        # 去除所有空的說明欄位作為兒子和媽媽的content
        all_content = str(soup.find('section', id='rightContentOverviewTab'))
        lst_content_name = []
        for index, table in enumerate(pd.read_html(res.content)):
            if len(table) ==0:
                lst_content_name.append(table.columns[0])
        for index, table in enumerate(soup.find_all('table')):
            if table.find('tr'):
                if str(table.find('tr').text.strip()) in lst_content_name:
                    all_content = all_content.replace(str(table), '')
        # 加入SDS, CoA. Ref
        if soup.find('section', class_='doc-links clearfix'):
            all_content = str(soup.find('section', class_='doc-links clearfix')) + all_content.replace('\n', '').replace('\t', '')
        # 去除兒子
        if soup.find('table', class_='list'):
            all_content = all_content.replace(str(soup.find('table', class_='list')), '')
        # 找出規格
        if soup.find('h3', id='anchor_keySpecTable'):
            dict_spec = pd.read_html(res.content)[1].T.to_dict()[0]
            if 'CAS #' in list(dict_spec.keys()):
                cas_no = dict_spec['CAS #']
                self.info_ouput['CAS_NO'] = cas_no
                dict_spec.pop('CAS #')
            else:
                self.info_ouput['CAS_NO'] = ''
            self.info_ouput['所有規格'] = ','.join([f"{key}:{value.replace(',', '、')}" for key, value in dict_spec.items()])
        
        if soup.find('h2', id='anchor_orderingcomp'):
            for num in range(0, 10):
                df = pd.read_html(res.content)[num]
                try:
                    df = df[['Catalogue Number', 'Packaging', 'Qty/Pack']]
                    self.info_ouput['所有兒子'] = ','.join([f'{row[0]} || {row[1]} || {row[2]}' for row in df.values.tolist()])
                    break
                except:
                    pass
        else:
            self.info_ouput['所有兒子'] = ''
        # 找出圖片url
        domain = 'https://www.merckmillipore.com'
        url = domain + soup.find('div', class_='image-container').img['src']
        self.info_ouput['圖片連結'] = url
        # 找出包裝
        pack_age = {}
        for table in soup.find_all('table'):
            if table.find('input', 'attribute_Group_Header_Display'):
                content_type = table.find('input', 'attribute_Group_Header_Display')['name'] == 'attributeGroupHeaderDisplay_PDP_OverviewTab_Product_Packaging_Information'
                if content_type:
                    for pack_spec in table.findAll('tr'):
                        if pack_spec.findAll('td'):
                            pack_age[pack_spec.findAll('td')[0].text] = pack_spec.findAll('td')[1].text
        # 去除多餘的content
        button = soup.find('button', 'green')
        all_content = '<h3>Overview</h3>' + all_content.replace(str(button), '').replace('<table', '<table width="1100px"').replace(
        '<tr>\n<th colspan="2">Biological Information<input class="main-menu-link" name="Biological Information$PDP_OverviewTab_Product_Biological_Info$anchor_Biological Information" type="hidden" value="true"/>\n<a name="anchor_Biological Information"></a>\n</th>\n</tr>\n</thead><tbody></tbody>',
        '') \
        .replace(
        '<tr>\n<th colspan="2">Dimensions<input class="main-menu-link" name="Dimensions$PDP_OverviewTab_Product_Dimensions$anchor_Dimensions" type="hidden" value="true"/>\n<a name="anchor_Dimensions"></a>',
        '') \
        .replace(
        '<tr>\n<th colspan="2">Materials Information<input class="main-menu-link" name="Materials Information$PDP_OverviewTab_Product_Materials_Information$anchor_Materials Information" type="hidden" value="true"/>\n<a name="anchor_Materials Information"></a>\n</th>\n</tr>\n</thead><tbody></tbody>',
        '') \
        .replace(
        '<tr>\n<th colspan="2">Product Usage Statements<input class="main-menu-link" name="Product Usage Statements$PDP_OverviewTab_Product_Usage_Statements$anchor_Product Usage Statements" type="hidden" value="true"/>\n<a name="anchor_Product Usage Statements"></a>\n</th>\n</tr>\n</thead><tbody></tbody>',
        '') \
        .replace(
        '<tr>\n<th colspan="2">Packaging Information<input class="main-menu-link" name="Packaging Information$PDP_OverviewTab_Product_Packaging_Information$anchor_Packaging Information" type="hidden" value="true"/>\n<a name="anchor_Packaging Information"></a>\n</th>\n</tr>\n</thead><tbody></tbody>',
        '') \
        .replace(
        '<tr>\n<th colspan="2">Supplemental Information<input class="main-menu-link" name="Supplemental Information$PDP_OverviewTab_Supplemental_Info$anchor_Supplemental Information" type="hidden" value="true"/>\n<a name="anchor_Supplemental Information"></a>\n</th>\n</tr>\n</thead><tbody></tbody>',
        '') \
        .replace(
        '<tr>\n<th colspan="2">Product Information<input class="main-menu-link" name="Product Information$PDP_OverviewTab_Product_Information$anchor_Product Information" type="hidden" value="true"/>\n<a name="anchor_Product Information"></a>\n</th>\n</tr>\n</thead><tbody></tbody>',
        '') \
        .replace(
        '<tr>\n<th colspan="2">Physicochemical Information<input class="main-menu-link" name="Physicochemical Information$PDP_OverviewTab_Product_Physico_Chemical_Info$anchor_Physicochemical Information" type="hidden" value="true"/>\n<a name="anchor_Physicochemical Information"></a>\n</th>\n</tr>\n</thead><tbody></tbody>',
        '') \
        .replace(
        '<tr>\n<th colspan="2">Toxicological Information<input class="main-menu-link" name="Toxicological Information$PDP_OverviewTab_Product_Toxicological_Data$anchor_Toxicological Information" type="hidden" value="true"/>\n<a name="anchor_Toxicological Information"></a>\n</th>\n</tr>\n</thead><tbody></tbody>',
        '') \
        .replace(
        '<tr>\n<th colspan="2">Safety Information according to GHS<input class="main-menu-link" name="Safety Information according to GHS$PDP_OverviewTab_Product_Safety_Info_GHS$anchor_Safety Information according to GHS" type="hidden" value="true"/>\n<a name="anchor_Safety Information according to GHS"></a>\n</th>\n</tr>\n</thead><tbody></tbody>',
        '') \
        .replace(
        '<tr>\n<th colspan="2">Storage and Shipping Information<input class="main-menu-link" name="Storage and Shipping Information$PDP_OverviewTab_Product_Storage_and_Shipping_Info$anchor_Storage and Shipping Information" type="hidden" value="true"/>\n<a name="anchor_Storage and Shipping Information"></a>\n</th>\n</tr>\n</thead><tbody></tbody>',
        '') \
        .replace(
        '<tr>\n<th colspan="2">Transport Information<input class="main-menu-link" name="Transport Information$PDP_OverviewTab_Product_Transport_Info$anchor_Transport Information" type="hidden" value="true"/>\n<a name="anchor_Transport Information"></a>\n</th>\n</tr>\n</thead><tbody></tbody>',
        '') \
        .replace(
        '<tr>\n<th colspan="2">References<input class="main-menu-link" name="References$PDP_OverviewTab_References$anchor_References" type="hidden" value="true"/>\n<a name="anchor_References"></a>\n</th>\n</tr>\n</thead><tbody></tbody>',
        '') \
        .replace(
        '<tr>\n<th colspan="2">Safety Information<input class="main-menu-link" name="Safety Information$PDP_OverviewTab_Product_Safety_Info$anchor_Safety Information" type="hidden" value="true"/>\n<a name="anchor_Safety Information"></a>\n</th>\n</tr>\n</thead><tbody></tbody>',
        '') \
        .replace(
        '<tr>\n<th colspan="2">Applications<input class="main-menu-link" name="Applications$PDP_OverviewTab_Applications$anchor_Applications" type="hidden" value="true"/>\n<a name="anchor_Applications"></a>\n</th>\n</tr>\n</thead><tbody></tbody>',
        '') \
        .replace(
        '<tr>\n<th colspan="2">Applications<input class="main-menu-link" name="Applications$PDP_OverviewTab_Applications$anchor_Applications" type="hidden" value="true"/>\n<a name="anchor_Applications"></a>\n</th>\n</tr>\n</thead><tbody></tbody>',
        '') \
        .replace(
        '<tr>\n<th colspan="2">Replacement Information<input class="main-menu-link" name="Replacement Information$PDP_OverviewTab_Replacement_Information$anchor_Replacement Information" type="hidden" value="true"/>\n<a name="anchor_Replacement Information"></a>\n</th>\n</tr>\n</thead><tbody></tbody>',
        '') \
        .replace(
        '<tr>\n<th colspan="2">Specifications<input class="main-menu-link" name="Specifications$PDP_OverviewTab_Product_Specifications$anchor_Specifications" type="hidden" value="true"/>\n<a name="anchor_Specifications"></a>\n</th>\n</tr>\n</thead><tbody></tbody>',
        '').replace('td rowspan="1"', 'td width="200px" rowspan="1"')
        self.info_ouput['商品群組簡介'] = mom_short_desc
        self.info_ouput['商品群組介紹'] = all_content
        self.info_ouput['包裝資訊'] = ','.join([f"{key}:{value.replace(',', '、')}" for key, value in pack_age.items()]) if len(pack_age) > 0 else ''

def get_img_from_url(url, local_path=None):
    # 下載圖片
    response = requests.get(url)
    # 儲存圖片
    with open(local_path, 'wb') as f:
        f.write(response.content)

def merck_scapy(word_lst, start, end):
    for key in word_lst[start:end]:    
        # print(f"{key} start")
        obj = MerckScrapyAllSite(key_word=key)
        obj.fetch_all_product_info()
        print(f"{key} end")

def merck_scrapy(cookie=None, start=None, end=None, df=None, number=None):
    session_requests = requests.session()
    headers_get = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7',
            'cache-control': 'max-age=0',
            'cookie': cookie,
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36'
        }
    df_prev = df.loc[start:end,:]
    price = []
    price_ori = []
    for _, row  in df_prev.iterrows():
        res_prev = session_requests.get(row['json_url'], headers=headers_get)
        res_json = res_prev.json()
        try:
            price.append(res_json['priceRegularAsDecimal'])
        except:
            price.append('')
        try:
            price_ori.append(res_json['priceStrikeValue'])
        except:
            price_ori.append('')
        if _ % 100 == 0:
            print(f"執行到第{_}筆")
    df_prev['登入前價格'] = price_ori
    df_prev['登入後價格'] = price
    df_prev.to_excel(f'2023-1-14_test_{number}')


def main(func):
    # 使用多線程爬蟲
    s_index = 0
    step = 1
    e_index = s_index + step
    processes = []
    for line in range(0, 6):
        p = mp.Process(target=func, args=(already_lst, s_index, e_index))
        s_index += step
        e_index += step   
        processes.append(p)
    for process in processes:
        process.start()
    for process in processes:
        process.join()


if __name__ == '__main__':
    # main(merck_scapy)
    key='aa'
    obj = MerckScrapyAllSite(key_word=key)
    obj.fetch_all_product_info()
    print(f"{key} end")