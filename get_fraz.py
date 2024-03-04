import json
import time
import requests
import logging
import urllib.parse
import pandas as pd
import asyncio
import aiohttp
from fake_useragent import UserAgent

from create_bd import *

cunte_page = 0
cunte_bd = 1
cunte_fraz = 0
cunte_fraz_raz = 0
ua = UserAgent()

def get_phrases():
    # sourcery skip: collection-into-set, extract-method, remove-redundant-continue
    try:
        
        count = 0 
        cookies = {
                'external-locale': 'ru',
                'wbx-validation-key': '4d807e7c-20b7-4ce7-beb0-d676eb827020',
                '__zzatw-wb': 'MDA0dBA=Fz2+aQ==',
                '_wbauid': '7183448531705469461',
                '___wbu': '774523b1-5132-4b3f-a7bb-1fc83058be28.1705469460',
                'BasketUID': '38774099dfb64a03adb715ea98863127',
                'x-supplier-id-external': 'ff12f93b-4b0c-4e0e-9989-f59652dbff2a',
                'cfidsw-wb': 'd7ZOtf3YnEr/2ZIcfpPYIdMRRyV05lzs5Mgb9EPvawBy/0rUV0Mif0ihbCZsW3mQPVpiMmBOZeKmtMGusF+xx6cE+H3ynw2RsE2yQ2eGDw49kbtig3MjcPlrq8mWV87p0cM617HZ2LlvyJey7ou8V1tSoVExsEcwV/tbkfw=',
                'WBTokenV3': 'eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE3MDU0NjQzMzMsInZlcnNpb24iOjIsInVzZXIiOiIyNTE4MjE0MiIsInNoYXJkX2tleSI6IjEzIiwiY2xpZW50X2lkIjoic2VsbGVyLXBvcnRhbCIsInNlc3Npb25faWQiOiJiZDc4MmYwZWU4ZTc0OTY3YjViNzcwMjI3MjBkY2VlNyIsInVzZXJfcmVnaXN0cmF0aW9uX2R0IjoxNjc0MTA4NDIxLCJ2YWxpZGF0aW9uX2tleSI6IjFiN2JkYjIzYjFhNzFjNjAwMGQ0YWNjNjY1YWNhNjU5NjhmYjNkZTk1NjFhOGE5ZTdhYTNkODYxMDQwZmIyZDAiLCJwaG9uZSI6IkJpc2Y4WWdHN0E2U0o5elFBVFhIeFE9PSJ9.gX6eVInaO0jZa51D-59a6nzQG3J9fpddMAGvTt79yOw6eZrMbum98Q-i4jsrshBP2EvZna2FIBiclJFoB-22HkTjg8KATYOozrNnCrFYtfpILVokYp8rPNk870KHEwi395ORYLH5wwQKJw5bho_Mx6pMxYvznc60SQ4eMS77gbIbRZbxd0xvsPfWPz5FHT0sYqpt2oen6wibP9q-qgK_DrLMSDyCj6FOC7asAdf0Ec82mL-8W5i12MBoPZzdjYKeIbu9IZRIbiHRfuFkTkK0Ca0R_utudKDT7QA9hmdPVg9mEkiVrtWeGBve9c5bavDgy-pmbP2vxdjjZS3M1jiswA',
            }
        list_text = []
        list_requestCount = []
        for item in range(10000):
            headers = {
                'authority': 'seller-weekly-report.wildberries.ru',
                'accept': '*/*',
                'accept-language': 'ru,en;q=0.9',
                'content-type': 'application/json',
                # 'cookie': 'external-locale=ru; wbx-validation-key=4d807e7c-20b7-4ce7-beb0-d676eb827020; __zzatw-wb=MDA0dBA=Fz2+aQ==; _wbauid=7183448531705469461; ___wbu=774523b1-5132-4b3f-a7bb-1fc83058be28.1705469460; BasketUID=38774099dfb64a03adb715ea98863127; x-supplier-id-external=ff12f93b-4b0c-4e0e-9989-f59652dbff2a; cfidsw-wb=d7ZOtf3YnEr/2ZIcfpPYIdMRRyV05lzs5Mgb9EPvawBy/0rUV0Mif0ihbCZsW3mQPVpiMmBOZeKmtMGusF+xx6cE+H3ynw2RsE2yQ2eGDw49kbtig3MjcPlrq8mWV87p0cM617HZ2LlvyJey7ou8V1tSoVExsEcwV/tbkfw=; WBTokenV3=eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE3MDU0NjQzMzMsInZlcnNpb24iOjIsInVzZXIiOiIyNTE4MjE0MiIsInNoYXJkX2tleSI6IjEzIiwiY2xpZW50X2lkIjoic2VsbGVyLXBvcnRhbCIsInNlc3Npb25faWQiOiJiZDc4MmYwZWU4ZTc0OTY3YjViNzcwMjI3MjBkY2VlNyIsInVzZXJfcmVnaXN0cmF0aW9uX2R0IjoxNjc0MTA4NDIxLCJ2YWxpZGF0aW9uX2tleSI6IjFiN2JkYjIzYjFhNzFjNjAwMGQ0YWNjNjY1YWNhNjU5NjhmYjNkZTk1NjFhOGE5ZTdhYTNkODYxMDQwZmIyZDAiLCJwaG9uZSI6IkJpc2Y4WWdHN0E2U0o5elFBVFhIeFE9PSJ9.gX6eVInaO0jZa51D-59a6nzQG3J9fpddMAGvTt79yOw6eZrMbum98Q-i4jsrshBP2EvZna2FIBiclJFoB-22HkTjg8KATYOozrNnCrFYtfpILVokYp8rPNk870KHEwi395ORYLH5wwQKJw5bho_Mx6pMxYvznc60SQ4eMS77gbIbRZbxd0xvsPfWPz5FHT0sYqpt2oen6wibP9q-qgK_DrLMSDyCj6FOC7asAdf0Ec82mL-8W5i12MBoPZzdjYKeIbu9IZRIbiHRfuFkTkK0Ca0R_utudKDT7QA9hmdPVg9mEkiVrtWeGBve9c5bavDgy-pmbP2vxdjjZS3M1jiswA',
                'origin': 'https://seller.wildberries.ru',
                'referer': 'https://seller.wildberries.ru/',
                'sec-ch-ua': '"Chromium";v="118", "YaBrowser";v="23.11", "Not=A?Brand";v="99", "Yowser";v="2.5"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Windows"',
                'sec-fetch-dest': 'empty',
                'sec-fetch-mode': 'cors',
                'sec-fetch-site': 'same-site',
                'user-agent': f'{ua.random}',
            }
            count += 1
            params = {
                'itemsPerPage': '100',
                'offset': f'{item * 100}',
                'period': 'month',
                'query': '',
                'sort': 'desc',
            }
            if count in [50, 100, 150, 200, 250, 300, 350, 400]:
                time.sleep(120)
            response = requests.get(
                'https://seller-weekly-report.wildberries.ru/ns/trending-searches/suppliers-portal-analytics/api',
                params=params,
                cookies=cookies,
                headers=headers,
            )
            if response.status_code == 200:
                response_1 = response.json()
                # with open(f"34.json", "w", encoding="utf_8") as file_create:
                #     json.dump(response, file_create, indent=4, ensure_ascii=False)
                for item_requestCount in response_1["data"]["list"]:
                    if not item_requestCount["text"].isdigit():
                        index = item_requestCount["text"].find('\ufeff')
                        if index != -1:
                            list_text.append(item_requestCount["text"][:index].replace("'", "_") + item_requestCount["text"][(index+1):].replace("'", "_"))
                        else:
                            list_text.append(item_requestCount['text'].replace("'", "_"))
                        list_requestCount.append(item_requestCount["requestCount"])
                if item_requestCount["requestCount"] >= 10000:  
                    print(item)
                else:
                    break
            else:
                time.sleep(120)
                continue
        return list_text, list_requestCount
    except Exception as ex:
        print(ex)
        time.sleep(30)
        get_phrases()
        logging.exception(ex)
        
def create_phrases():
    try:
        list_text = get_phrases()
        connection = connect_bd()
        connection.autocommit = True
        cursor = connection.cursor()
        sql_delete_query = """Delete from phrases"""
        cursor.execute(sql_delete_query)
        cursor.execute(f"""INSERT INTO phrases(search_query, number_requests)
                                    VALUES(UNNEST(ARRAY{list_text[0]}), 
                                            UNNEST(ARRAY{list_text[1]})
                                            );""")
    except Exception as ex:
        logging.exception(ex)

def red_fraz():
    return pd.read_sql("SELECT * FROM phrases", con=get_engine_from_settings())
    

async def get_page_data(querys, item_name, number_request):
    list_data = [[], [], [], [], [], []]
    async with aiohttp.ClientSession() as session:
        url = "https://search.wb.ru/exactmatch/ru/common/v4/search"
        for page in range(1, 40):
            headers = {
                'Accept': '*/*',
                'Accept-Language': 'ru,en;q=0.9',
                'Connection': 'keep-alive',
                'Origin': 'https://www.wildberries.ru',
                'Referer': f'https://www.wildberries.ru/catalog/0/search.aspx?page={page}&sort=popular&search={urllib.parse.quote(querys)}',
                'Sec-Fetch-Dest': 'empty',
                'Sec-Fetch-Mode': 'cors',
                'Sec-Fetch-Site': 'cross-site',
                'User-Agent': f'{ua.random}',
                'authorization': 'Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE3MDQ4NjEwNjAsInZlcnNpb24iOjIsInVzZXIiOiIyNTE4MjE0MiIsInNoYXJkX2tleSI6IjEzIiwiY2xpZW50X2lkIjoid2IiLCJzZXNzaW9uX2lkIjoiOGEzMjJjNjkyMTc3NDc0MjhkYzAzN2E5ZWM2YzEyY2IiLCJ1c2VyX3JlZ2lzdHJhdGlvbl9kdCI6MTY3NDEwODQyMSwidmFsaWRhdGlvbl9rZXkiOiIwNWZjYjQzZWMyZjYxYTdiMDk3MmJhOGM2NGVjZGU3NWQ0YzIwNTJlNzY5ZWFkMjU2YzkwOWZjNjRlYjVmYzZlIiwicGhvbmUiOiJCaXNmOFlnRzdBNlNKOXpRQVRYSHhRPT0ifQ.duqxbEMwt4NFGqKLVbkgLK0MvVjbgJrwsjcEu_z6X62w3PSeBXg4xnZlrOTq2MN3-UhyeHMaqac6ArjlAQTP-9kezt7GWg8-hE1lkgalT1L3hmYdLUhYwU9A6wI5Afb0eu2Vjx-BZtSmJy0RGIVpA6QNQPLLHmxsXkzwnQ5P6oEpgJTyeHqKRK0a77bsh-conWV_E2UMHVut1yZAO9OcGZxZ1AghxhBGL4gExCbDyg9dVGI8hkUmYlPwLSSSQG61OuIBgc6btVK5wdS0VJcDLjF3VzzGcL2k4YaPeGnJ2swylRKYGoM_CH3wcRTPfkS-s7bccym0zZGLyh_fGNPBDg',
                'sec-ch-ua': '"Chromium";v="118", "YaBrowser";v="23.11", "Not=A?Brand";v="99", "Yowser";v="2.5"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Windows"',
                'x-userid': '25182142',
            }
            params = {
                'TestGroup': 'no_test',
                'TestID': 'no_test',
                'appType': '1',
                'curr': 'rub',
                'dest': '123585602',
                'page': f'{page}',
                'query': f'{querys}',
                'resultset': 'catalog',
                'sort': 'popular',
                'spp': '29',
                'suppressSpellcheck': 'false',
                'uclusters': '0',
            }
            try:
                index_1 = 0
                async with session.get(url=url, params=params, headers=headers, raise_for_status=True) as response:
                    # if response.status != 200:
                    #     print(response.status)
                    #     time.sleep(20)
                    #     raise aiohttp.ClientResponseError()
                    # else:
                    data = await response.json(content_type=None)
                    for index, item_data in enumerate(data["data"]["products"]):
                        list_data[0].append(int(item_data["id"]))
                        list_data[1].append(str(item_name))
                        list_data[2].append(int(number_request))
                        list_data[3].append(int(page))
                        list_data[4].append(int(index))
                        list_data[5].append(int(item_data["sale"]))
                        index_1 = index
                if index_1 not in [96, 97, 98, 99]:
                    break
            except Exception:
                continue
        create_card(list_data)
    
def change_cunte_page():
    global cunte_page
    cunte_page = cunte_page + 1
    
def change_cunte_bd_minus(minus):
    global cunte_page
    cunte_page = cunte_page - minus

def change_cunte_bd():
    global cunte_bd
    cunte_bd = cunte_bd + 1
    
def change_cunte_fraz(change):
    global cunte_fraz
    global cunte_fraz_raz
    cunte_fraz = cunte_fraz + change
    cunte_fraz_raz = cunte_fraz // 31 + 1
    
def create_card(list_data):
    try:
        connection = connect_bd()
        connection.autocommit = True
        cursor = connection.cursor()
        if len(list_data[0]) != 0:
            if cunte_page < cunte_fraz_raz:
                change_cunte_page()
                cursor.execute(f"""INSERT INTO search_results{cunte_bd}(id, query, number, page, place, sale)
                                            VALUES(UNNEST(ARRAY{list_data[0]}), 
                                                    UNNEST(ARRAY{list_data[1]}),
                                                    UNNEST(ARRAY{list_data[2]}),
                                                    UNNEST(ARRAY{list_data[3]}),
                                                    UNNEST(ARRAY{list_data[4]}),
                                                    UNNEST(ARRAY{list_data[5]})
                                                    );""")
            if cunte_page == cunte_fraz_raz:
                cursor.execute(f"""INSERT INTO search_results{cunte_bd}(id, query, number, page, place, sale)
                                            VALUES(UNNEST(ARRAY{list_data[0]}), 
                                                    UNNEST(ARRAY{list_data[1]}),
                                                    UNNEST(ARRAY{list_data[2]}),
                                                    UNNEST(ARRAY{list_data[3]}),
                                                    UNNEST(ARRAY{list_data[4]}),
                                                    UNNEST(ARRAY{list_data[5]})
                                                    );""")
                change_cunte_bd_minus(cunte_fraz_raz)
                change_cunte_bd() 
                print(f"База данных: {cunte_bd}")                             
    except Exception as ex:
        print("Тут ошибка записи в бд")
        logging.exception(ex)

def split(arr, size):
     arrs = []
     while len(arr) > size:
         pice = arr[:size]
         arrs.append(pice)
         arr = arr[size:]
     arrs.append(arr)
     return arrs
     
async def get_card():
    try:
        dell_bd()
        table_stocks = red_fraz()
        change_cunte_fraz(len(table_stocks["search_query"]))
        arrs = split(list(table_stocks["search_query"]), 3000)
        number = split(list(table_stocks["number_requests"]), 3000)
        for index in range(0, len(arrs)):
            tasks = []
            for iindex, itemm in enumerate(arrs[index]):
                querys = itemm.replace("_", "'")
                task = asyncio.create_task(get_page_data(querys, itemm, number[index][iindex]))
                tasks.append(task)
            print(index)
            print(time.strftime("%Y-%m-%d %H:%M"))
            await asyncio.gather(*tasks)
    except Exception as ex:
        logging.exception(ex)
        
def dell_bd():
    connection = connect_bd()
    connection.autocommit = True
    cursor = connection.cursor()
    for page in range(1, 33):
        sql_delete_query = f"""Delete from search_results{page}"""
        cursor.execute(sql_delete_query)   
        
async def competitors_by_phrases():  
    table_stocks = red_fraz()
    dickt_query = {}
    for item in list(table_stocks["search_query"]):
        dickt_query[item] = [""]
        print(dickt_query)
    print(dickt_query)
    
async def get_card_1():  # sourcery skip: merge-list-append, move-assign-in-block
    try:
        table_stocks = red_fraz()
        change_cunte_fraz(len(table_stocks["search_query"]))
        arrs = split(list(table_stocks["search_query"]), 10000)
        for index in range(0, len(arrs)):
            tasks = []
            list_querys = []
            for itemm in arrs[index]:
                querys = itemm.replace("_", "'")
                list_querys.append(querys)
            print(index)
            print(time.strftime("%Y-%m-%d %H:%M"))
            task = asyncio.create_task(get_page_data_1(list_querys))
            tasks.append(task)
            await asyncio.gather(*tasks)
    except Exception as ex:
        logging.exception(ex)
        
async def get_page_data_1(list_querys):
    dict_data = [[],[]]
    async with aiohttp.ClientSession() as session:
        url = "https://search.wb.ru/exactmatch/ru/common/v4/search"
        for querys in list_querys:
            quer = ''
            dict_data[0].append(querys.replace("'", "_"))
            headers = {
                'Accept': '*/*',
                'Accept-Language': 'ru,en;q=0.9',
                'Connection': 'keep-alive',
                'Origin': 'https://www.wildberries.ru',
                'Referer': f'https://www.wildberries.ru/catalog/0/search.aspx?search={urllib.parse.quote(querys)}',
                'Sec-Fetch-Dest': 'empty',
                'Sec-Fetch-Mode': 'cors',
                'Sec-Fetch-Site': 'cross-site',
                'User-Agent': f'{ua.random}',
                'authorization': 'Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE3MDQ4NjEwNjAsInZlcnNpb24iOjIsInVzZXIiOiIyNTE4MjE0MiIsInNoYXJkX2tleSI6IjEzIiwiY2xpZW50X2lkIjoid2IiLCJzZXNzaW9uX2lkIjoiOGEzMjJjNjkyMTc3NDc0MjhkYzAzN2E5ZWM2YzEyY2IiLCJ1c2VyX3JlZ2lzdHJhdGlvbl9kdCI6MTY3NDEwODQyMSwidmFsaWRhdGlvbl9rZXkiOiIwNWZjYjQzZWMyZjYxYTdiMDk3MmJhOGM2NGVjZGU3NWQ0YzIwNTJlNzY5ZWFkMjU2YzkwOWZjNjRlYjVmYzZlIiwicGhvbmUiOiJCaXNmOFlnRzdBNlNKOXpRQVRYSHhRPT0ifQ.duqxbEMwt4NFGqKLVbkgLK0MvVjbgJrwsjcEu_z6X62w3PSeBXg4xnZlrOTq2MN3-UhyeHMaqac6ArjlAQTP-9kezt7GWg8-hE1lkgalT1L3hmYdLUhYwU9A6wI5Afb0eu2Vjx-BZtSmJy0RGIVpA6QNQPLLHmxsXkzwnQ5P6oEpgJTyeHqKRK0a77bsh-conWV_E2UMHVut1yZAO9OcGZxZ1AghxhBGL4gExCbDyg9dVGI8hkUmYlPwLSSSQG61OuIBgc6btVK5wdS0VJcDLjF3VzzGcL2k4YaPeGnJ2swylRKYGoM_CH3wcRTPfkS-s7bccym0zZGLyh_fGNPBDg',
                'sec-ch-ua': '"Chromium";v="118", "YaBrowser";v="23.11", "Not=A?Brand";v="99", "Yowser";v="2.5"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Windows"',
                'x-userid': '25182142',
            }
            params = {
                'TestGroup': 'no_test',
                'TestID': 'no_test',
                'appType': '1',
                'curr': 'rub',
                'dest': '123585602',
                'query': f'{querys}',
                'resultset': 'catalog',
                'sort': 'popular',
                'spp': '29',
                'suppressSpellcheck': 'false',
                'uclusters': '0',
            }
            try:
                async with session.get(url=url, params=params, headers=headers, raise_for_status=True) as response:
                    data = await response.json(content_type=None)
                    for index, item_data in enumerate(data["data"]["products"]):
                        if index < 5:
                            quer = quer + "|" + str(item_data["id"])
                        if index >= 5:
                            break
                    dict_data[1].append(quer)
            except Exception as error:
                print(error)
                dict_data[1].append("")
                time.sleep(30)
                continue
    create_card_1(dict_data)
    
def create_card_1(list_data):
    try:
        connection = connect_bd()
        connection.autocommit = True
        cursor = connection.cursor()
        if len(list_data[1]) != 0:
                cursor.execute(f"""INSERT INTO concurent(query, art_concurent)
                                            VALUES(UNNEST(ARRAY{list_data[0]}), 
                                                    UNNEST(ARRAY{list_data[1]})
                                                    );""")                             
    except Exception as ex:
        print("Тут ошибка записи в бд")
        logging.exception(ex) 

def main():
    start_time = time.time()
    asyncio.run(get_card())
    asyncio.run(get_card_1())
    finish_time = time.time() - start_time
    print(f"Затрачено времени: {finish_time}")

if __name__=="__main__":
    create_phrases()
    main()