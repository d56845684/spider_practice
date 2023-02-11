import ssl
import requests
from bs4 import BeautifulSoup
import pandas as pd
import sys
sys.path.append('.')
from utils_func.helpers import *
ssl._create_default_https_context = ssl._create_unverified_context

class AsoneNewProducts(object):
    """
    透過AsOne英文網站的分類去上架新商品
    """
    def __init__(self, url=None, label=None) -> None:
        self.url = url
        self.label = label
        self.headers = {
            'Connection': 'close',
            'Accept': 'text/javascript, application/javascript, application/ecmascript, application/x-ecmascript, */*; q=0.01',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7',
            'Host': 'www.axel-gl.com',
            'Referer': 'https://www.axel-gl.com/en/asone/keyword/?q=2',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.113 Safari/537.36',
            'X-Requested-With': 'XMLHttpRequest'
        }
        self.replace_words_list = ['INSTRUMENT CO., LTD. (Sanwa)', 'INDUSTRIES CO., LTD. (TITAN)',
                                'INDUSTRIAL COMPANY LIMITED', 'INSTRUMENTS WORKS, LTD.', 'Company of Japan, Ltd.',
                                'Mfg. Co., Ltd. (KANON)', 'IND. CO., LTD. COMPANY', 'Corporation Co., Ltd.',
                                'Instruments Co., Ltd.', 'INDUSTRIES CO., LTD.)', 'Company Of Japan, Ltd',
                                'INDUSTRIAL CO., LTD.', 'Industries Co., Ltd.', 'Industrial Co., LTD.',
                                'INDUSTRIES CO., LTD.', 'INDUSTRIAL Co., Ltd.', 'INDUSTRIES Co., Ltd.',
                                'Industrial Co., Ltd.', 'Industry Corporation', 'Inc. (Asia) Limited',
                                'Industries CO.,LTD.', 'Industries, Limited', 'MFG.CO., LTD., SURE',
                                'Industres Co., Ltd.', 'Industry Co,. Ltd.', 'INDUSTRI Co., Ltd.',
                                'Industry Co., Ltd.', 'INDUSTRY CO., Ltd.', 'INDUSTRY CO., LTD.',
                                'INDUSTRY Co., Ltd.', 'Infratec Co., Ltd.', 'COMPANY Co., Ltd.', 'INSTRUMENTS, Inc.',
                                'INDUSTRY CO., LTD', 'INDUSTRY CO. LTD.', 'Instruments Corp.', 'Industries, Inc.',
                                'Industries, Ltd.', 'CORPORATION LTD.', 'INDUSTRIES, INC.', 'Co., LTD. (MIWA)',
                                'Corporation Inc.', 'INSTRUMENTS INC.', 'INDUSTRIES, LTD.', 'Industry Co.,Ltd',
                                'Company,\u2002Limited', 'Industry Limited', 'COMPANY, LIMITED', 'Company, Limited',
                                'INDUSTRIES LTD.', 'MFG., Co., Ltd.', 'Mfg Corporation', 'INTEC Co., Ltd.',
                                'COMPANY LIMITED', 'INDUSTRIES INC.', 'Industries Ltd.', 'Company Limited',
                                'Mfg. Co., Ltd.', 'MFG. CO., LTD.', 'Mfg. Co., Inc.', 'MFG. Co., Ltd.',
                                'IND. Co., Ltd.', 'Mfg Co., Ltd.', 'Company, Ltd.', 'MFG CO., LTD.', 'Mfg.Co., Ltd.',
                                'MFG.Co., Ltd.', 'IN. Co., Ltd.', 'COMPANY, LTD.', 'MFG Co., Ltd.', 'COMPANY, INC.',
                                'Corporation.', 'CO., LIMITED', 'INCORPORATED', 'Corporation)', 'Company Ltd.',
                                'MFG.CO.,LTD.', 'Incorporated', 'Company LLC.', 'CORPORATION)', 'Company Inc.',
                                'MFG.CO.,LTD', 'Corporation', 'ＣＯＲＰＯＲＡＴＩＯＮ', '& Co., Ltd.', 'corporation',
                                'CORPORATION', 'Corpration.', 'Ind., Ltd.', 'MFG.CO.LTD', 'Co., Ltd.)', 'CO., LTD.',
                                'CO., Ltd.', 'CO. ,LTD.', 'co., ltd.', 'Co., ltd.', 'Co., Ltd.', 'Co,. Ltd.',
                                'Co., LTD.', 'co., LTD.', 'CO,. LTD.', 'CO., INC.', 'Co., Ltd', 'Co.,Ltd.',
                                'CO, LTD.', 'CO., LTD', 'Mfg Ltd.', 'co,.ltd.', 'co.,ltd.', 'CO., Ltd', 'CO. LTD.',
                                'Co. Ltd.', 'Co, Ltd.', 'co,. LTD', 'CO.,LTD.', 'Company)', 'Company', 'LIMITED',
                                'CO.,LTD', 'Limited', 'Co.,Ltd', 'COMPANY', 'Co Ltd', 'Co-op.', 'Corp.', 'Inc.)',
                                'corp.', 'CORP.', ',Ltd.', 'Ltd.', 'INC.', 'Mfg.', 'MFG.', 'LTD.', 'inc.', 'Corp',
                                ', Inc.', 'Inc.', 'LLC.', 'LTD', 'LLC', 'inc', 'Co.', 'INC', 'Mfg', 'MFG', 'CO.',
                                'Ltd', 'Inc']  # TODO: 先放長的, 再放短的
        self.strip_words_list = [' Co., ltd.', ' Co., LTD.', '.,', ' Co,..', ' CO,. LTD.', ' CO., LTD', ' Inc.',
                                ' Company', ' co., ltd.', ' Company Inc.', ', LTD.', ' Co.', ' CO.,.', '..', ' INC.',
                                ',.', ' LTD., Unipack', ' Ind.,.', '., INC.', ', INC.', '.', ' LTD.', '.,LTD.',
                                ' Co.,Ltd']
        # 用來清洗商品資訊的文字
        self.asone_address_replce = 'For details, please contact us using the contact information below.\nAS ONE corporation, special sales group\nTEL: 06-6447-8623\nFAX: 06-6447-8457\nE-mail: tokuchu@so.as-1.co.jp'
        # 用來對照英文規格的dict
        self.asone_chi_eng_spec_dict = {'Minimum display': '精度 (g)',
                                                'Maximum measurement temperature': '最高溫度',
                                                'Setting minimum temperature': '最低溫度',
                                                'Weighing': '秤重 (g)',
                                                'Capacity': '容量',
                                                '(-) size (mm)': '尺寸 (mm)',
                                                'size': '尺寸'}
        # 亞速望前綴型號
        self.asone_cn_product_prefix = ['', 'CC-', 'H', 'C']
        # 爬蟲失敗的名單
        self.error_df = pd.DataFrame()
        # 連結資料庫
        self.conn = MySqlConnHelper('sciket.com')

    def get_gno_result(self, gno=None):
        """
        gno: 多筆資料的型號
        """
        url_mut = f'https://www.axel-gl.com/en/asone/g/{gno}/'
        res = requests.get(url=url_mut, headers=self.headers)
        soup = BeautifulSoup(res.content)
        df_cleaned = pd.read_html(url_mut)[0].loc[:, ['Order No.', 'Name', 'Standard price']]
        mom_content = soup.find('div', class_='detail group-detail').text.strip()
        mom_name = soup.find('h1', class_='name').text.strip()
        mom_img_url = soup.find('div', class_='group-images').a['href']
        df_cleaned.columns = ['供應商料號', '商品名稱', '日本牌價']
        df_cleaned['原廠群組型號'] = gno 
        df_cleaned['商品群組介紹'] = mom_content 
        df_cleaned['商品群組名稱'] = mom_name
        df_cleaned['商品群組圖片網址'] = mom_img_url
        df_cleaned = df_cleaned[['原廠群組型號', '商品群組介紹', '商品群組名稱', '商品群組圖片網址', '供應商料號', '商品名稱', '日本牌價']]
        return df_cleaned
    
    def get_search_result(self, url_page=None):
        """
        爬搜尋結果
        """
        # url_mut = f'https://www.axel-gl.com/en/asone/keyword/?page={url_page}&q={url_name}'
        # url_mut = f'https://www.axel-gl.com/en/asone/keyword/?page={url_page}&q={url_name}'
        res_m = requests.get(url=url_page, headers=self.headers)
        bs_m = BeautifulSoup(res_m.content)
        cleaned_no = []
        cleaned_jp_price = []
        cleaned_title = []
        non_mom = []
        mom_no = []
        mom_need_check = []
        for p_info in bs_m.find_all('div',{'class':'af-detail-link'}):
            url_link = p_info.a['href']
            url_title = p_info.find('h2',{'class': 'name'}).text.strip()
            price = p_info.find('div', {'class':'data'}).find('p', {'class':'data-col data-price'}).text
            # 假如是/d/代表該商品沒有媽媽
            if url_link.split('/')[3] == 'd':
                cleaned_no.append(url_link.split('/')[4])
                cleaned_jp_price.append(price)
                cleaned_title.append(url_title)
                non_mom.append('需要確認媽媽')
            else:
                # 把需要進一步爬媽媽頁的商品記下來
                mom_need_check.append(url_link.split('/')[4])
        return pd.DataFrame({'原廠群組型號':non_mom, '商品群組介紹':non_mom, '商品群組名稱':non_mom, '商品群組圖片網址':non_mom, '供應商料號': cleaned_no,
        '商品名稱': cleaned_title,
        '日本牌價': cleaned_jp_price}), mom_need_check
    

    def parse_son(self, res=None, mom=None):
        """
        主要爬取兒子頁面的method
        """
        soup = BeautifulSoup(res.content)
        # 取得網頁右邊的資訊表格並組裝成df
        df = pd.read_html(res.content)[0].T
        df.columns = df.iloc[0,:]
        df = df.iloc[1:2, :]
        dict_ = {}
        # 商品內容
        content = soup.find('ul', class_='detail-desc-list').text.strip().replace('\t', '')
        content = content.replace(self.asone_address_replce, '')
        # 商品品牌
        try:
            brand = soup.find('a', class_='manufacturer').text
        except:
            brand = ''
        # 清理商品品牌文字
        for _word in self.replace_words_list:
            if _word in brand:
                brand = brand.replace(_word, "")
                brand = brand.strip()
                if ',' in brand:
                    brand = brand.replace(',', '')
                break
        # 所有圖片的網址
        imgurl_lis = '\n'.join([item.a['href'] for item in soup.find('ul', class_='gallery-images').find_all('li')])
        # 是否有體積過大的標示
        oversize = soup.find('span', class_='item-icon.item-icon-gray')
        # 是否有毒物品的標示
        hazardous = soup.find('span',class_='span.item-icon.item-icon-red')
        # 如果有大體積的標示, 要去商品資訊那邊增加說明文字
        if oversize:
            if 'Oversized' in oversize.text or 'Flat Rate Shipping' in oversize.text:
                content = content + "<b><font color=\'#4d7be9\'><span style=\'font-size:18px;\'>此商品依照不同裝置現場，可能產生附加運費，請您訂購前與客服人員聯繫</span></font></b><br />"
        # 如果有毒化物要特別標出
        if hazardous:
            if 'Hazardous' in hazardous.text:
                dict_['毒化物'] = '是'
        else:
            dict_['毒化物'] = '否'
        # 商品規格
        spec_list = ','.join([item.text.strip().replace(',','、') for item in soup.find('section', class_='detail-desc-section detail-spec').find_all('li') if ':' in item.text])
        # 進行中英文規格對照
        for key, value in self.asone_chi_eng_spec_dict.items():
            if key in spec_list:
                spec_list = spec_list.replace(key, value)
        # 原廠型號
        try:
            dict_['原廠型號'] = str(df['Model No.'].values[0])
        except:
            dict_['原廠型號'] = ''
        # 最小分類(根據有沒有媽媽會爬不同的index)
        label_index = -3 if mom else -2
        labels_no = soup.find('ul', class_='af-breadcrumb').find_all('li')[label_index].a['href'].split('/')[-2]
        # 所有分類
        label_name = ' --> '.join([item.text.strip() for item in soup.find('ul', class_='af-breadcrumb').find_all('li')[0:label_index+1]])
        # 包裝大小
        dict_['包裝'] = df['Quantity'].values[0]
        dict_['商品規格(用逗號半形隔開)'] = spec_list
        dict_['商品內容'] = content
        dict_['商品簡介'] = ''
        dict_['圖片連結'] = imgurl_lis
        dict_['品牌'] = brand
        dict_['圖片連結'] = imgurl_lis
        dict_['交期'] = '諮詢/訂購後通知'
        dict_['幣值'] = '日幣'
        dict_['庫存比例'] = 1
        dict_['基底商品型號'] = ''
        dict_['原廠網站連結(若是有建議提供)'] = ''
        dict_['限購類型'] = ''
        dict_['分類型號'] = labels_no
        dict_['全部分類名稱'] = label_name
        return dict_
    
    def get_page_number(self):
        """
        取得該分類連結的所有筆數，AsOne預設每頁40筆資料
        return 該分類的總頁數
        """
        res = requests.get(self.url, headers=self.headers)
        soup = BeautifulSoup(res.content)
        return round(int(soup.find('span', class_='result-count-num').text) / 40) + 1


    def data_cleaned_merged(self):
        # 先得到所有的AsOne型號
        print(f'{self.url}, start')
        page_num = self.get_page_number()
        df_ = pd.DataFrame()
        for i in range(1, page_num + 1):
            df_ok, lst_check = self.get_search_result(url_page=f'{self.url}?page={i}')
            df_son_all = pd.DataFrame()
            # 把所有原廠群組型號進一步去爬兒子的型號
            lst_check = list(set(lst_check))        # 透過set去重複
            for no in lst_check:
                df_prev = self.get_gno_result(gno=no)
                df_son_all = pd.concat([df_son_all, df_prev])
            df_all_ready = pd.concat([df_son_all, df_ok])
            df_ = pd.concat([df_, df_all_ready])
        df_parse_all = pd.DataFrame()
        error_lst = []
        for index, row in df_.iterrows():
            no = row['供應商料號']
            mom = row['原廠群組型號']
            try:
                url  = f'https://www.axel-gl.com/en/asone/d/{no}/'
                res_m = requests.get(url=url, headers=self.headers)
                mom_exist = True if mom != '需要確認媽媽'else False
                dict_ = self.parse_son(res=res_m, mom=mom_exist)
                df_prev = pd.DataFrame(dict_, index=[0])
                df_prev['供應商料號'] = no
                df_parse_all = pd.concat([df_parse_all, df_prev])
            except Exception as e :
                error_lst.append({'AsOne型號':no, '錯誤原因': e})
                print(f'{no}爬蟲失敗', e)
        if len(error_lst) > 0:
            self.error_df = pd.DataFrame(error_lst)
        df_ = df_.merge(df_parse_all, how='inner', on='供應商料號')
        df_['網址'] = f'https://www.axel-gl.com/en/asone/d/' + df_['供應商料號'] + '/'
        print(f'{self.url}, end')
        return df_
        # cmd = f"""
        # select id as '群組分類(科研市集填寫)', no as '分類型號' from labels
        # where no in {tuple(df_['分類型號'].tolist())}
        # """
        # df_label = pd.DataFrame(conn.execute_query(cmd, execute_type='select'))
        # df_ = df_.merge(df_label, how='left', on='分類型號')

if __name__ == "__main__":
    obj = AsoneNewProducts(url='https://www.axel-gl.com/en/asone/s/R0000200/')
    # 提供要爬的網址和分類ID

    df = obj.data_cleaned_merged()
    df.to_excel('2023-02-08Great_Deal.xlsx')
    
