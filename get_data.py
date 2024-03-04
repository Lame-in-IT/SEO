import time
import logging
import pandas as pd
import asyncio
import psycopg

from create_bd import *
  
cunte_page = 0
cunte_bd = 1
cunte_fraz = 0
cunte_fraz_raz = 0
  
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
    cunte_fraz_raz = cunte_fraz // 9 + 1
        
async def get_article():
    try:
        connection = connect_bd()
        connection.autocommit = True
        cursor = connection.cursor()
        sql_delete_query = f"""Delete from id_saler"""
        cursor.execute(sql_delete_query)
        async with await psycopg.AsyncConnection.connect(
                                "dbname=Maxim_SEO_optimization user=postgres password=6d6a9785 host=localhost") as aconn:
            async with aconn.cursor() as acur:
                for page in range(1, 33):
                    sull_id_1 = pd.read_sql(f"SELECT id FROM id_saler", con=get_engine_from_settings())
                    sull_id = sorted(list(sull_id_1["id"]))
                    list_id = []
                    await acur.execute(f"SELECT * FROM search_results{page}")
                    await acur.fetchone()
                    async for record in acur:
                        first = 0
                        last = len(sull_id)-1
                        index = -1
                        while (first <= last) and (index == -1):
                            mid = (first+last)//2
                            if sull_id[mid] == record[0]:
                                index = mid
                            elif record[0]<sull_id[mid]:
                                last = mid -1
                            else:
                                first = mid +1
                        if index == -1:
                            list_id.append(record[0])
                    if list_id:
                        task = asyncio.create_task(record_id(list_id))
                        tasks = [task]
                        await asyncio.gather(*tasks)
                    print(page)
    except Exception as ex:
        logging.exception(ex)
        
async def record_id(list_data):
    connection = connect_bd()
    connection.autocommit = True
    cursor = connection.cursor()
    cursor.execute(f"""INSERT INTO id_saler(id)
                                VALUES(UNNEST(ARRAY{list_data})
                                        );""")
    print(f"Записано {list_data[0]}")
    
async def get_id_is_bd():
    sull_id = pd.read_sql(f"SELECT id FROM id_saler", con=get_engine_from_settings())
    dict_data = {iyem_dict: ['','','',''] for iyem_dict in list(sull_id["id"])}
    async with await psycopg.AsyncConnection.connect(
        "dbname=Maxim_SEO_optimization user=postgres password=6d6a9785 host=localhost") as aconn:
        async with aconn.cursor() as acur:
            for page in range(1, 33):
                await acur.execute(f"SELECT * FROM search_results{page}")
                await acur.fetchone()
                async for record in acur:
                    dict_data[record[0]][0] =  dict_data[record[0]][0] + '|' + record[1]
                    dict_data[record[0]][1] =  dict_data[record[0]][1] + '|' + str(record[2])
                    dict_data[record[0]][2] =  dict_data[record[0]][2] + '|' + str(record[3])
                    dict_data[record[0]][3] =  dict_data[record[0]][3] + '|' + str(record[4])
                print(page)
    task = asyncio.create_task(record_in_bd_optimal_seo(dict_data))
    tasks = [task]
    await asyncio.gather(*tasks)
    
def split(arr, size):
     arrs = []
     while len(arr) > size:
         pice = arr[:size]
         arrs.append(pice)
         arr = arr[size:]
     arrs.append(arr)
     return arrs   
    
async def record_in_bd_optimal_seo(list_datas):
    list_art = []
    list_query = []
    list_number = []
    list_page = []
    list_place = []
    for ite in list_datas:
        list_art.append(ite)
        list_query.append(list_datas[ite][0])
        list_number.append(list_datas[ite][1])
        list_page.append(list_datas[ite][2])
        list_place.append(list_datas[ite][3])
    art1 = split(list_art, 1500000)
    query1 = split(list_query, 1500000)
    number1 = split(list_number, 1500000)
    page1 = split(list_page, 1500000)
    place1 = split(list_place, 1500000)
    for del_index in range(1, 11):
        connection = connect_bd()
        connection.autocommit = True
        cursor = connection.cursor()
        sql_delete_query = f"""Delete from optimal_{del_index}"""
        cursor.execute(sql_delete_query)
        cursor.close()
    for index in range(0, len(art1)):
        connection = connect_bd()
        connection.autocommit = True
        cursor = connection.cursor()
        cursor.execute(f"""INSERT INTO optimal_{(index // 2) + 1}(id, query, number, page, place)
                                    VALUES(UNNEST(ARRAY{art1[index]}), 
                                            UNNEST(ARRAY{query1[index]}),
                                            UNNEST(ARRAY{number1[index]}),
                                            UNNEST(ARRAY{page1[index]}),
                                            UNNEST(ARRAY{place1[index]})
                                            );""")
        print(f"Записано {index}")
        cursor.close()
    
async def get_cancur_fraz_1():
    connection = connect_bd()
    connection.autocommit = True
    cursor = connection.cursor()
    sql_delete_query = f"""Delete from concurent_1"""
    cursor.execute(sql_delete_query)
    sull_id = pd.read_sql(f"SELECT art_concurent FROM concurent", con=get_engine_from_settings())
    list_concur = []
    for item in sull_id["art_concurent"]:
        try:
            listt = item.split("|")
            for ite_listt in listt:
                if ite_listt not in list_concur:
                    list_concur.append(ite_listt)
        except Exception:
            continue
    list_concur.pop(0)
    task = asyncio.create_task(full_fraf_for_top(list_concur))
    tasks = [task]
    await asyncio.gather(*tasks) 
    
async def full_fraf_for_top(list_concur):
    dict_art = {ite: "" for ite in list_concur}
    async with await psycopg.AsyncConnection.connect(
        "dbname=Maxim_SEO_optimization user=postgres password=6d6a9785 host=localhost") as aconn:
        async with aconn.cursor() as acur:
            for page in range(1, 11):
                await acur.execute(f"SELECT * FROM optimal_{page}")
                await acur.fetchone()
                async for record in acur:
                    try:
                        dict_art[str(record[0])] =  dict_art[str(record[0])] + '|' + record[1]
                    except KeyError:
                        continue
                print(page)
    task = asyncio.create_task(record_in_bd_optimal_seo_1(dict_art))
    tasks = [task]
    await asyncio.gather(*tasks)
    
async def record_in_bd_optimal_seo_1(dict_art):
    try:
        list_1 = [[], []]
        for ity in dict_art:
            list_1[0].append(ity)
            list_1[1].append(dict_art[ity])
        connection = connect_bd()
        connection.autocommit = True
        cursor = connection.cursor()
        if len(list_1[1]) != 0:
                cursor.execute(f"""INSERT INTO concurent_1(art_concurent, top_fraz)
                                            VALUES(UNNEST(ARRAY{list_1[0]}), 
                                                    UNNEST(ARRAY{list_1[1]})
                                                    );""") 
                print("Записани concurent_1")                            
    except Exception as ex:
        print("Тут ошибка записи в бд")
        logging.exception(ex)  
    
def requests_from_phrases():
    dict_data = {}
    concurent_1 = pd.read_sql(f"SELECT * FROM concurent_1", con=get_engine_from_settings())
    for index, item_concurent in enumerate(concurent_1["art_concurent"]):
        dict_data[item_concurent] = []
        try:
            list_art_concurent = concurent_1["top_fraz"][index].split("|")
            list_art_concurent.pop(0)
            list_art_concurent.pop(0)
            dict_data[item_concurent] = list_art_concurent
        except Exception:
            continue
    list_data = [[], []]
    concurent = pd.read_sql(f"SELECT * FROM concurent", con=get_engine_from_settings())
    for indexx, itemm in enumerate(concurent["query"]):
        try:
            list_frazz = []
            list_data[0].append(itemm)
            list_top_art = concurent["art_concurent"][indexx].split("|")
            list_top_art.pop(0)
            for item_fraz in list_top_art:
                for itee in dict_data[item_fraz]:
                    if itee not in list_frazz:
                        list_frazz.append(itee)
            top_zapros = ''
            for iteemm in list_frazz:
                top_zapros = top_zapros + "|" + iteemm
            list_data[1].append(top_zapros)
        except Exception:
            continue
    record_21(list_data)
    
def record_21(dict_art):
    try:
        connection = connect_bd()
        connection.autocommit = True
        cursor = connection.cursor()
        if len(dict_art[1]) != 0:
                cursor.execute(f"""INSERT INTO concurent_2(query, top_fraz)
                                            VALUES(UNNEST(ARRAY{dict_art[0]}), 
                                                    UNNEST(ARRAY{dict_art[1]})
                                                    );""") 
                print("Записани concurent_2")                            
    except Exception as ex:
        print("Тут ошибка записи в бд")
        logging.exception(ex) 
        
async def get_id():
    concurent = pd.read_sql(f"SELECT * FROM concurent_2", con=get_engine_from_settings())
    dict_concurent = dict(zip(list(concurent["query"]), list(concurent["top_fraz"])))
    dict_optimal_seo = {}
    async with await psycopg.AsyncConnection.connect(
            "dbname=Maxim_SEO_optimization user=postgres password=6d6a9785 host=localhost") as aconn:
        async with aconn.cursor() as acur:
            for page in range(1, 11):
                await acur.execute(f"SELECT * FROM optimal_{page}")
                await acur.fetchone()
                async for record in acur:
                    competitors = record[1].split("|")[1]
                    dict_optimal_seo[record[0]] = [
                        record[0],
                        record[1],
                        record[2],
                        dict_concurent[competitors],
                        f"{str(record[3])}:{str(record[4])}",
                    ]
                print(f"Страница {page}")
    task = asyncio.create_task(record_in_bd_optimal__1(dict_optimal_seo))
    tasks = [task]
    await asyncio.gather(*tasks)
        
async def record_in_bd_optimal__1(dict_optimal_seo):
    # try:
    list_1 = [[], [], [], [], []]
    for ity in dict_optimal_seo:
        for inde, itee in enumerate(dict_optimal_seo[ity]):
            list_1[inde].append(itee)
    art1 = split(list_1[0], 300000)
    query1 = split(list_1[1], 300000)
    number1 = split(list_1[2], 300000)
    query_competitors1 = split(list_1[3], 300000)
    number_page = split(list_1[4], 300000)
    connection = connect_bd()
    connection.autocommit = True
    cursor = connection.cursor()
    for del_index in range(1, 11):
        sql_delete_query = f"""Delete from optimal_seo_{del_index}"""
        cursor.execute(sql_delete_query)
        # cursor.close()
    for index in range(0, len(art1)):
        cursor.execute(f"""INSERT INTO optimal_seo_{int(index / 10) + 1}(id, query, number, description_user, query_competitors)
                                    VALUES(UNNEST(ARRAY{art1[index]}), 
                                        UNNEST(ARRAY{query1[index]}),
                                        UNNEST(ARRAY{number1[index]}),
                                        UNNEST(ARRAY{number_page[index]}),
                                        UNNEST(ARRAY{query_competitors1[index]})
                                        );""")
        print(f"optimal_seo_{int(index / 10) + 1}")
    
def main():
    start_time = time.time()
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    loop = asyncio.get_event_loop()
    loop.run_until_complete(get_article())
    loop.run_until_complete(get_id_is_bd())
    loop.run_until_complete(get_cancur_fraz_1())
    requests_from_phrases()
    loop.run_until_complete(get_id())
    finish_time = time.time() - start_time
    print(f"Затрачено времени: {finish_time}")


if __name__=="__main__":
    main()   