import requests
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup as bs
import time
from tqdm.notebook import tqdm
import sys

def load_data(link, time_sleep, page_num):
    
    # метод принимает в себя время для ождания в секундах
    time.sleep(time_sleep)
    
    # тут будет генерироваться новая страница 
    # если вы обратите внимание чем отличаються страницы, сделать цикл по ним не будет проблемой 
    # https://www.ss.lv/lv/real-estate/flats/riga/all/hand_over/page48.html
    # https://www.ss.lv/lv/real-estate/flats/riga/all/hand_over/page49.html
    
    link = link + 'page' + str(page_num) + '.html'
    
    # получаем реквест 
    r = requests.get(URL_TEMPLATE)
    
    # важно, если вернулся статус не 200, то из функции можно выходить 
    if r.status_code!=200:
        print('Error status', r.status_code)
        return 
    
    # парсим наши данные, все проводимые ранее операции 
    soup = bs(r.text, "html.parser")
    parsed_data = soup.find_all('td', class_='msga2-o pp6')
    
    page_array = []

    i = 0
    for data in parsed_data:
        page_array.append([i, data.get_text("|")])
        i += 1

    df_tmp = pd.DataFrame(page_array, columns=['line', 'data'])
    df_tmp['head'] = df_tmp['line']%6
    df_tmp['group'] = df_tmp.groupby('head').cumcount()
    
    
    # собираем наши данные в единный датафрейм  
    df_page = df_tmp.loc[df_tmp['head']==0][['group', 'data']]       
    df_page = df_page.merge(df_tmp.loc[df_tmp['head']==1][['group', 'data']], how='left', on='group', suffixes=('','_rooms'))
    df_page = df_page.merge(df_tmp.loc[df_tmp['head']==2][['group', 'data']], how='left', on='group', suffixes=('','_m2'))
    df_page = df_page.merge(df_tmp.loc[df_tmp['head']==3][['group', 'data']], how='left', on='group', suffixes=('','_floor'))
    df_page = df_page.merge(df_tmp.loc[df_tmp['head']==4][['group', 'data']], how='left', on='group', suffixes=('','_seria'))
    df_page = df_page.merge(df_tmp.loc[df_tmp['head']==5][['group', 'data']], how='left', on='group', suffixes=('','_price'))
    
    return df_page

URL_TEMPLATE = "https://www.ss.lv/lv/real-estate/flats/riga/all/hand_over/"

df = load_data(URL_TEMPLATE, 1, 1)

for page in range(1,100):
    if page == 1:
        df = load_data(URL_TEMPLATE, 1, page)
    else:
        df = pd.concat([df, load_data(URL_TEMPLATE, 1, page)])
        
# почистим данные в data_price, удалим ' €','|' и ','
df['data_price'] = df['data_price'].apply(lambda x: x.replace(' €','').replace('|','').replace(',','').replace(' /mēn.', ''))
df['data_price'] = df['data_price'].astype('int64')

# разделим дистрикт и улицу на два столбца
df[['data_district', 'data_street']] = df['data'].str.split(pat='|', n=1 , expand=True )

# разделим этаж и максимальный этаж на два столбца
df[['data_cur_floor', 'data_max_floor']] = df['data_floor'].str.split(pat='/', n=1 , expand=True )

df['data_cur_floor'] = df['data_cur_floor'].astype('int64')
df['data_max_floor'] = df['data_max_floor'].astype('int64')

df['data_rooms'] = df['data_rooms'].astype('int64')

df = df[['data_district','data_street','data_rooms','data_cur_floor','data_max_floor','data_m2','data_seria','data_price']]

#test










