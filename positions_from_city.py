import os
from datetime import datetime
import urllib
from operator import itemgetter
from multiprocessing import freeze_support, Pool
import configparser
import httplib2
import googleapiclient.discovery
import requests
from oauth2client.service_account import ServiceAccountCredentials
import openpyxl
import time
import openpyxl
from tqdm import tqdm
import json

current_dir = os.getcwd()
config = configparser.ConfigParser()
config.read('config.ini')

CREDENTIALS_FILE = 'credentials.json'

spreadsheet_id = config['DEFAULT']['spreadsheet_id']
LIST_NAME = config['DEFAULT']['list_name']

credentials = ServiceAccountCredentials.from_json_keyfile_name(
    CREDENTIALS_FILE,
    ['https://www.googleapis.com/auth/spreadsheets',
     'https://www.googleapis.com/auth/drive'])
httpAuth = credentials.authorize(httplib2.Http())
service = googleapiclient.discovery.build('sheets', 'v4', http=httpAuth, static_discovery=False)

pages_count_to_search = 100


def table_clear():
    service.spreadsheets().values().clear(
        spreadsheetId=spreadsheet_id,
        range=f"{LIST_NAME}!A:ZZ"
    ).execute()


def read_xl_file(filename):
    data_from_file = []
    try:
        wb = openpyxl.load_workbook(filename)
        ws = wb.active
        for row in ws.rows:
            if row[0].value is None or row[1].value is None:
                continue
            data_from_file.append([row[0].value, row[1].value, row[2].value])
        return data_from_file[1:]
    except Exception as e:
        print(e, '[Error] Нет такого файла или каталога')


def google_append_data(data):
    try:
        sheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sheet_titles = [s['properties']['title'] for s in sheet['sheets']]
        if LIST_NAME not in sheet_titles:
            requests = [{
                'addSheet': {
                    'properties': {
                        'title': LIST_NAME
                    }
                }
            }]
            service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body={'requests': requests}).execute()

        values = service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range=f"{LIST_NAME}!A:ZZ",
            valueInputOption="USER_ENTERED",
            insertDataOption="INSERT_ROWS",
            body={"values": data}
        ).execute()
        print(f"[INFO] Данные успешно записаны в лист {LIST_NAME}")
    except Exception as e:
        print(f"[INFO] Ошибка записи данных в лист {LIST_NAME}: {e}")


def parse_query(data):
    step_id = data[0]
    article = data[1]
    query = data[2]
    text = urllib.parse.quote_plus(query)
    headers = {
        'accept': '*/*',
        'accept-encoding': 'gzip, deflate, br',
        'accept-language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
        'dnt': '1',
        'origin': 'https://www.wildberries.ru',
        'referer': 'https://www.wildberries.ru/catalog/0/search.aspx?search={query}&xsearch=true',
        'sec-ch-ua': '"Chromium";v="92", " Not A;Brand";v="99", "Google Chrome";v="92"',
        'sec-ch-ua-mobile': '?0',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36',
    }

    headers['referer'] = headers['referer'].replace('{query}', text)
    CITYES = {
        'Москва': {
            'target': f'https://search.wb.ru/exactmatch/ru/common/v4/search?appType=1&couponsGeo=2,12,7,3,6,22,21,8&curr=rub&dest=-1059500,-77665,-1099982,-4039473&emp=0&lang=ru&locale=ru&pricemarginCoeff=1.0&query={text}&reg=0&regions=80,64,83,4,38,33,70,82,69,68,86,30,40,48,1,22,66,31&resultset=catalog&sort=popular&spp=0&suppressSpellcheck=false',
            'cookie': urllib.parse.quote_plus(
                '__wbl=cityId=0&regionId=0&city=Москва&phone=84957755505&latitude=55,776218&longitude=37,629171&src=1')
        },
        'Казань': {
            'target': f'https://search.wb.ru/exactmatch/ru/common/v4/search?appType=1&couponsGeo=12,7,3,6,18,22,21&curr=rub&dest=-1075831,-79374,-367666,-2133462&emp=0&lang=ru&locale=ru&pricemarginCoeff=1.0&query={text}&reg=0&regions=80,64,83,4,38,33,70,82,69,68,86,30,40,48,1,22,66,31&resultset=catalog&sort=popular&spp=0&suppressSpellcheck=false',
            'cookie': urllib.parse.quote_plus(
                '__wbl=cityId=0&regionId=0&city=Казань&phone=84957755505&latitude=55,789604073&longitude=49,124949102&src=1')
        },
        'Краснодар': {
            'target': f'https://search.wb.ru/exactmatch/ru/common/v4/search?appType=1&couponsGeo=2,7,3,6,19,21,8&curr=rub&dest=-1059500,-108082,-269701,12358062&emp=0&lang=ru&locale=ru&pricemarginCoeff=1.0&query={text}&reg=0&regions=80,64,83,4,38,33,70,82,69,68,86,30,40,48,1,22,66,31&resultset=catalog&sort=popular&spp=0&suppressSpellcheck=false',
            'cookie': urllib.parse.quote_plus(
                '__wbl=cityId=0&regionId=0&city=Краснодар&phone=84957755505&latitude=45,050437&longitude=38,959727&src=1')
        },
        'Екатеринбург': {
            'target': f'https://search.wb.ru/exactmatch/ru/common/v4/search?appType=1&couponsGeo=2,12,7,3,6,13,21&curr=rub&dest=-5818948,-5803327&emp=0&lang=ru&locale=ru&pricemarginCoeff=1.0&query={text}&reg=0&regions=80,64,38,4,115,83,33,68,70,69,30,86,40,1,66,48,110,31,22,114,111&resultset=catalog&sort=popular&spp=0&suppressSpellcheck=false',
            'cookie': urllib.parse.quote_plus(
                '__wbl=cityId=0&regionId=0&city=Екатеринбург&phone=84957755505&latitude=56,843829&longitude=60,625187&src=1')
        },
        'Санкт-Петербург': {
            'target': f'https://search.wb.ru/exactmatch/ru/common/v4/search?appType=1&couponsGeo=12,7,3,6,5,18,21&curr=rub&dest=-1216601,-337422,-1114902,-1198055&emp=0&lang=ru&locale=ru&pricemarginCoeff=1.0&query={text}&reg=0&regions=80,64,83,4,38,33,70,82,69,68,86,30,40,48,1,22,66,31&resultset=catalog&sort=popular&spp=0&suppressSpellcheck=false',
            'cookie': urllib.parse.quote_plus(
                '__wbl=cityId=0&regionId=0&city=Санкт-Петербург&phone=84957755505&latitude=59,934568&longitude=30,298117&src=1')
        },
        'Новосибирск': {
            'target': f'https://search.wb.ru/exactmatch/ru/common/v4/search?appType=1&couponsGeo=2,12,7,3,6,21,16&curr=rub&dest=-1221148,-140294,-1751445,-364763&emp=0&lang=ru&locale=ru&pricemarginCoeff=1.0&query={text}&reg=0&regions=80,64,58,83,4,38,33,70,82,69,68,86,30,40,48,1,22,66,31&resultset=catalog&sort=popular&spp=0&suppressSpellcheck=false',
            'cookie': urllib.parse.quote_plus(
                '__wbl=cityId=0&regionId=0&city=Новосибирск&phone=84957755505&latitude=55,034727&longitude=82,917024&src=1')
        },
        'Хабаровск': {
            'target': f'https://search.wb.ru/exactmatch/ru/common/v4/search?appType=1&couponsGeo=2,12,7,6,9,21,11&curr=rub&dest=-1221185,-151223,-1782064,-1785058&emp=0&lang=ru&locale=ru&pricemarginCoeff=1&query={text}&reg=0&regions=80,64,4,38,70,82,69,86,30,40,48,1,66&resultset=catalog&sort=popular&spp=0&suppressSpellcheck=false',
            'cookie': urllib.parse.quote_plus(
                '__wbl=cityId=0&regionId=0&city=Хабаровск&phone=84957755505&latitude=48,476717&longitude=135,078796&src=1')
        },
        'Тула': {
            'target': f'https://search.wb.ru/exactmatch/ru/common/v4/search?appType=1&couponsGeo=2,12,3,18,22,21&curr=rub&dest=-1029256,-81993,-4775559,-5663270&emp=0&lang=ru&locale=ru&pricemarginCoeff=1.0&query={text}&reg=0&regions=80,64,83,4,38,33,70,82,69,68,86,30,40,48,1,22,66,31&resultset=catalog&sort=popular&spp=0&suppressSpellcheck=false',
            'cookie': urllib.parse.quote_plus(
                '__wbl=cityId=0&regionId=0&city=Тула&phone=84957755505&latitude=54,191284&longitude=37,619423&src=1')
        },
        'Нур-Султан': {
            'target': f'https://search.wb.ru/exactmatch/ru/common/v4/search?appType=1&couponsGeo=7,3,1,6,21,16&curr=rub&dest=12358388,12358412,-3479876,85&emp=0&lang=ru&locale=kz&pricemarginCoeff=1&query={text}&reg=0&regions=58,4,70,82,102,69,68,86,30,40,48,1,22,66,31&resultset=catalog&sort=popular&spp=0&suppressSpellcheck=false',
            'cookie': urllib.parse.quote_plus(
                '__wbl=cityId=0&regionId=0&city=Нур-Султан&phone=84957755505&latitude=51,12749&longitude=71,461081&src=1')
        },
        'Невинномысск': {
            'target': f'https://search.wb.ru/exactmatch/ru/common/v4/search?appType=1&couponsGeo=2,7,3,6,19,21,8&curr=rub&dest=-389344,-108081,-1030099,123585553&emp=0&lang=ru&locale=ru&pricemarginCoeff=1.0&query={text}&reg=0&regions=80,64,83,4,38,33,70,82,69,68,86,30,40,48,1,22,66,31&resultset=catalog&sort=popular&spp=0&suppressSpellcheck=false',
            'cookie': urllib.parse.quote_plus(
                '__wbl=cityId=0&regionId=0&city=Невинномысск&phone=84957755505&latitude=44,638673&longitude=41,941178&src=1')
        },
        'Домодедово': {
            'target': f'https://search.wb.ru/exactmatch/ru/common/v4/search?appType=1&couponsGeo=2,12,3,18,15,21,101&curr=rub&dest=-1029256,-51490,-184001,123586109&emp=0&lang=ru&locale=ru&pricemarginCoeff=1.0&query={text}&reg=0&regions=80,64,83,4,38,33,70,82,69,68,86,75,30,40,48,1,22,66,31,71&resultset=catalog&sort=popular&spp=0&suppressSpellcheck=false',
            'cookie': urllib.parse.quote_plus(
                '__wbl=cityId=0&regionId=0&city=Домодедово&phone=84957755505&latitude=55,438517&longitude=37,772981&src=1')
        },
        'Ереван': {
            'target': f'https://search.wb.ru/exactmatch/ru/common/v4/search?appType=1&couponsGeo=12,7,3,6,21&curr=rub&dest=12358387,12358400,-13404218,36&emp=0&lang=ru&locale=am&pricemarginCoeff=1&query={text}&reg=0&regions=80,4,82,69,68,86,30,40,48,1,66&resultset=catalog&sort=popular&spp=0&suppressSpellcheck=false',
            'cookie': urllib.parse.quote_plus(
                '__wbl=cityId=0&regionId=0&city=Ереван&phone=84957755505&latitude=40,180147&longitude=44,521028&src=1')
        },
        'Минск': {
            'target': f'https://search.wb.ru/exactmatch/ru/common/v4/search?appType=1&couponsGeo=12,7,3,21&curr=rub&dest=12358386,12358404,3,-59202&emp=0&lang=ru&locale=by&pricemarginCoeff=1&query={text}&reg=0&regions=80,83,4,33,70,82,69,68,86,30,40,48,1,22,66,31&resultset=catalog&sort=popular&spp=0&suppressSpellcheck=false',
            'cookie': urllib.parse.quote_plus(
                '__wbl=cityId=0&regionId=0&city=Минск&phone=84957755505&latitude=53,900893&longitude=27,569787&src=1')
        }
    }

    city_data = [step_id, article, query, f'https://www.wildberries.ru/catalog/0/search.aspx?sort=popular&search={text}']
    for k, v in CITYES.items():
        session = requests.Session()
        headers['cookie'] = v['cookie']

        breaker = False
        get_page_cnt = 0
        while True:
            if breaker:
                break
            get_page_cnt += 1
            try:
                json_data = session.get(v['target'], headers=headers, allow_redirects=True).json()
                break
            except Exception as e:
                if get_page_cnt == 3:
                    print(f'Ошибка получения страницы выдачи: ', e, f'артикул={article}, запрос={query}')
                    breaker = True
                    break
                else:
                    continue
        if breaker:
            city_data.append(0)
            continue

        page_cnt = 1
        breaker = False
        while not breaker:
            if page_cnt == pages_count_to_search + 1:
                city_data.append(0)
                breaker = True
            position_cnt = 1
            try:
                for i in json_data['data']['products']:
                    if str(article) == str(i['id']):
                        if page_cnt == 1:
                            city_data.append(position_cnt)
                            breaker = True
                        else:
                            city_data.append((page_cnt - 1) * 100 + position_cnt)
                            breaker = True
                    position_cnt += 1
            except Exception as e:
                city_data.append(0)
                breaker = True

            page_cnt += 1
            url = v['target'] + f'&page={page_cnt}'
            try:
                json_data = session.get(url, headers=headers).json()
            except Exception as e:
                city_data.append(0)
                breaker = True
    return city_data


def main():
    global pages_count_to_search
    pages_count_to_search = int(input('Введите количество страниц для поиска... '))

    current_datetime = datetime.now()
    utc = current_datetime.strftime('%d-%m-%Y %H:%M')
    data = [[utc]]
    data.append(['Артикул', 'Запрос', 'URL запроса', 'Москва', 'Казань', 'Краснодар', 'Екатеринбург',
                  'Санкт-Петербург', 'Новосибирск', 'Хабаровск', 'Тула', 'Нур-Султан', 'Невинномысск', 'Домодедово',
                 'Ереван', 'Минск'])

    print('[INFO] Начинаем сбор даных... ')
    print('[INFO] Пожалуйста, ожидайте! Создаются рабочие процессы... ')

    filename = 'data.xlsx'
    data_xl = read_xl_file(filename)
    with Pool(processes=10) as pool:
        result = list(tqdm(pool.imap(parse_query, data_xl), total=len(data_xl)))

    sorted(result, key=itemgetter(0))

    for i in result:
        data.append([i[1], i[2], i[3], i[4], i[5], i[6], i[7], i[8], i[9], i[10], i[11], i[12], i[13], i[14], i[15], i[16], ])

    print('[INFO] Очищаем Гугл таблицу... ')
    table_clear()
    print('[INFO] Запись данных в таблицу... ')
    google_append_data(data)


if __name__ == '__main__':
    with open('config.json', 'r') as f:
        data = json.load(f)
    pause = int(data['pause']) * 60
    while True:
        freeze_support()
        main()
        print(f'[INFO] Скрипт запустится вновь через {pause / 60} min')
        time.sleep(pause)
