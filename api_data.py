import requests
import pandas as pd

from create_bd import *


def get_article():  # sourcery skip: low-code-quality
    """
    Retrieves article data from an API and stores it in a database.

    Retrieves API keys from the user_api table in the database.
    Sends requests to the API to get article data.
    Stores the retrieved data in the users_api_data table in the database.

    Args:
        None

    Returns:
        None
    """
    api = pd.read_sql("SELECT * FROM user_api", con=get_engine_from_settings())
    list_id = []
    list_article = []
    list_title = []
    list_description = []
    list_characteristics = []
    url_WB_price = "https://suppliers-api.wildberries.ru/content/v2/get/cards/list"
    body = {
          "settings": {
            "cursor": {
              "limit": 1000
            },
            "filter": {
              "withPhoto": -1
              }
          }
        }
    for index_api, item_api in enumerate(api["api"]):
      split_data = item_api.split(",")
      for item_api_user in split_data:
        if len(item_api_user) > 0:
          id_user = api['id'][index_api]
          old_data = pd.read_sql(f"SELECT * FROM users_api_data WHERE id = {id_user}", con=get_engine_from_settings())
          if len(old_data["article"]) > 0:
            price_api = requests.post(url=url_WB_price, json=body, headers={'Content-Type': 'application/json', 'Authorization': f'{item_api_user}'}).json()
            for item_nm in price_api["cards"]:
              old_data_1 = pd.read_sql(f"SELECT * FROM users_api_data WHERE article = {item_nm['nmID']}", con=get_engine_from_settings())
              list_title_time = old_data_1["title"][0].split("|")
              str_title_time = ''
              list_description_time = old_data_1["description"][0].split("|")
              str_description_time = ''
              list_characteristics_time = old_data_1["characteristics"][0].split("|")
              str_characteristics_time = ''
              if len(list_title_time) >= 14:
                 for _ in range(2):
                    list_title_time.pop(0)
                    list_description_time.pop(0)
                    list_characteristics_time.pop(0)
              for index, item in enumerate(list_title_time):
                str_title_time = f"{item}" if index == 0 else f"{str_title_time}|{item}"
                str_description_time = f"{list_description_time[index]}" if index == 0 else f"{str_description_time}|{list_description_time[index]}"
                str_characteristics_time = f"{list_characteristics_time[index]}" if index == 0 else f"{str_characteristics_time}|{list_characteristics_time[index]}"
              list_id.append(id_user)
              list_article.append(item_nm["nmID"])
              list_title.append(str_title_time + "|" + str(datetime.datetime.now()).split(" ")[0] + "|" + str(len(item_nm["title"])))
              list_description.append(str_description_time + "|" + str(datetime.datetime.now()).split(" ")[0] + "|" + str(len(item_nm["description"])))
              list_characteristics.append(str_characteristics_time + "|" + str(datetime.datetime.now()).split(" ")[0] + "|" + str(len(item_nm["characteristics"])))
          else:
            price_api = requests.post(url=url_WB_price, json=body, headers={'Content-Type': 'application/json', 'Authorization': f'{item_api_user}'}).json()
            # with open(f"Все карточки.json", "w", encoding="utf_8") as file_create:
            #     json.dump(price_api, file_create, indent=4, ensure_ascii=False)
            for item_nm in price_api["cards"]:
              list_id.append(id_user)
              list_article.append(item_nm["nmID"])
              list_title.append(str(datetime.datetime.now()).split(" ")[0] + "|" + str(len(item_nm["title"])))
              list_description.append(str(datetime.datetime.now()).split(" ")[0] + "|" + str(len(item_nm["description"])))
              list_characteristics.append(str(datetime.datetime.now()).split(" ")[0] + "|" + str(len(item_nm["characteristics"])))
    get_price_cart(list_id, list_article, list_title, list_description, list_characteristics)
    
def get_price_cart(list_id, list_article, list_title, list_description, list_characteristics):
  """
    Retrieves price data for articles and stores it in a list.

    Retrieves article data from the users_api_data table in the database.
    Sends requests to the API to get price data for each article.
    Stores the retrieved price data in the list_price list.

    Args:
        list_id (list): List of user IDs.
        list_article (list): List of article IDs.
        list_title (list): List of article titles.
        list_description (list): List of article descriptions.
        list_characteristics (list): List of article characteristics.

    Returns:
        None
    """
  list_price = []
  for item_price in list_article:
    old_data = pd.read_sql(f"SELECT * FROM users_api_data WHERE article = {item_price}", con=get_engine_from_settings())
    headers = {
          'Accept': '*/*',
          'Accept-Language': 'ru,en;q=0.9',
          'Connection': 'keep-alive',
          'Origin': 'https://www.wildberries.ru',
          'Referer': f'https://www.wildberries.ru/catalog/{item_price}/detail.aspx',
          'Sec-Fetch-Dest': 'empty',
          'Sec-Fetch-Mode': 'cors',
          'Sec-Fetch-Site': 'cross-site',
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 YaBrowser/23.11.0.0 Safari/537.36',
          'authorization': 'Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE3MDUxMjkzNDgsInZlcnNpb24iOjIsInVzZXIiOiIyNTE4MjE0MiIsInNoYXJkX2tleSI6IjEzIiwiY2xpZW50X2lkIjoid2IiLCJzZXNzaW9uX2lkIjoiOGEzMjJjNjkyMTc3NDc0MjhkYzAzN2E5ZWM2YzEyY2IiLCJ1c2VyX3JlZ2lzdHJhdGlvbl9kdCI6MTY3NDEwODQyMSwidmFsaWRhdGlvbl9rZXkiOiIwNWZjYjQzZWMyZjYxYTdiMDk3MmJhOGM2NGVjZGU3NWQ0YzIwNTJlNzY5ZWFkMjU2YzkwOWZjNjRlYjVmYzZlIiwicGhvbmUiOiJCaXNmOFlnRzdBNlNKOXpRQVRYSHhRPT0ifQ.pM2zV8UZlaQSc1H-IRTHDSZG0TjxAzHxij-TdBAW8de03xsUFLizyuw_r-In7fDhHleToHIDSTU4sA5__a-hiykQKkMzbImxK97JaD4KI3S7X5OuAfbAyRJXLPeOhhexYTB029PLmQRKzDkAPGLnmTO5c6LR06aJ9RoqijVm_9Eq0uUiicme05RfQlgP8uMvAfHspXHbYORjPcnalurfJcqutYXi7AstCEATl0C5VAmhdEXXKuyuFUBw-_GDGYr99SN929-Fip2bsPac3al56i7d0FIOSZOTc94NtQ6Fe8AqWQ18Bfljk3m8TXgAPIIIGAfGVEfG6vrbIES9TtpLjQ',
          'sec-ch-ua': '"Chromium";v="118", "YaBrowser";v="23.11", "Not=A?Brand";v="99", "Yowser";v="2.5"',
          'sec-ch-ua-mobile': '?0',
          'sec-ch-ua-platform': '"Windows"',
      }
    response = requests.get(
        f'https://card.wb.ru/cards/v1/detail?appType=1&curr=rub&dest=123585602&spp=29&nm={item_price}',
        headers=headers,
    ).json() 
    if len(old_data["price"]) > 0:
      list_price_time = old_data["price"][0].split("|")
      if len(list_price_time) >= 14:
        str_price_time = ''
        for _ in range(2):
          list_price_time.pop(0)
        for index, item in enumerate(list_price_time):
          str_price_time = f"{item}" if index == 0 else f"{str_price_time}|{item}"
        list_price.append(str_price_time + "|" + str(datetime.datetime.now()).split(" ")[0] + "|" + str(response["data"]["products"][0]["salePriceU"] / 100))
      else:
        list_price.append(old_data["price"][0] + "|" + str(datetime.datetime.now()).split(" ")[0] + "|" + str(response["data"]["products"][0]["salePriceU"] / 100))
    if len(old_data["price"]) == 0:
      list_price.append(str(datetime.datetime.now()).split(" ")[0] + "|" + str(response["data"]["products"][0]["salePriceU"] / 100))
    # with open(f"197766674.json", "w", encoding="utf_8") as file_create:
    #   json.dump(response, file_create, indent=4, ensure_ascii=False)
  create_record_bd(list_id, list_article, list_title, list_description, list_characteristics, list_price)
  
def create_record_bd(list_id, list_article, list_title, list_description, list_characteristics, list_price):
  """
    Creates records in the users_api_data table in the database.

    Connects to the database.
    Deletes existing records in the users_api_data table.
    Inserts new records into the users_api_data table.

    Args:
        list_id (list): List of user IDs.
        list_article (list): List of article IDs.
        list_title (list): List of article titles.
        list_description (list): List of article descriptions.
        list_characteristics (list): List of article characteristics.
        list_price (list): List of article prices.

    Returns:
        None
    """
  connection = connect_bd()
  connection.autocommit = True
  cursor = connection.cursor()
  sql_delete_query = """Delete from users_api_data"""
  cursor.execute(sql_delete_query)
  cursor.execute(f"""INSERT INTO users_api_data(id, article, title, description, characteristics, price)
                                    VALUES(UNNEST(ARRAY{list_id}), 
                                           UNNEST(ARRAY{list_article}),
                                           UNNEST(ARRAY{list_title}),
                                           UNNEST(ARRAY{list_description}),
                                           UNNEST(ARRAY{list_characteristics}),
                                           UNNEST(ARRAY{list_price})
                                            );""")
  print("Записано в бд users_api_data")
          
        
        
def created_api():
  """
    Updates API keys in the user_api table in the database.

    Connects to the database.
    Updates the API key for a specific user.

    Args:
        None

    Returns:
        None
    """
  connection = connect_bd()
  connection.autocommit = True
  cursor = connection.cursor()
  # cursor.execute("""INSERT INTO user_api(id, api) VALUES(%s, %s)""",
  #                   [1323522063, "eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjMxMDI1djEiLCJ0eXAiOiJKV1QifQ.eyJlbnQiOjEsImV4cCI6MTcxNTIwMzQ0MSwiaWQiOiI4MzVkYjU0Ni04NTYwLTQ1NWYtYjI5Zi01YjQ0NzJkZTcwMzEiLCJpaWQiOjE5Nzg4MTMwLCJvaWQiOjU3OTU3LCJzIjoxMDIsInNpZCI6IjFlYzE1MzE4LTc5ODctNTE5MC1hYWFiLWM5MGRmNTRlZTc5NCIsInVpZCI6MTk3ODgxMzB9.M5woNrR3BcIVBUBnINlKk2Hg6J4pfYbvA1JrDBIdEMHEixi9LK8imXvXXhA454DIlsVTUBr826eBAVCM1EQV-g,"])
  with connection.cursor() as cursor:
                        cursor.execute("""UPDATE user_api SET api = %s WHERE id = %s""", [
                                    "eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjMxMDI1djEiLCJ0eXAiOiJKV1QifQ.eyJlbnQiOjEsImV4cCI6MTcxNTIwMzQ0MSwiaWQiOiI4MzVkYjU0Ni04NTYwLTQ1NWYtYjI5Zi01YjQ0NzJkZTcwMzEiLCJpaWQiOjE5Nzg4MTMwLCJvaWQiOjU3OTU3LCJzIjoxMDIsInNpZCI6IjFlYzE1MzE4LTc5ODctNTE5MC1hYWFiLWM5MGRmNTRlZTc5NCIsInVpZCI6MTk3ODgxMzB9.M5woNrR3BcIVBUBnINlKk2Hg6J4pfYbvA1JrDBIdEMHEixi9LK8imXvXXhA454DIlsVTUBr826eBAVCM1EQV-g,", 1323522063])
  print("Запись добавлена.") 



  
    
if __name__ == "__main__":
    get_article()