import time
import psycopg2
from config import config_wb as settings
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import database_exists, create_database
from psycopg2 import sql
import datetime

import logging
import json

def get_engine(user, passwd, host, port, db):
    url = f"postgresql://{user}:{passwd}@{host}:{port}/{db}"
    if not database_exists(url):
        create_database(url)
    return create_engine(url, pool_size=50, echo=False)

def get_engine_from_settings():  # sourcery skip: raise-specific-error
    keys = ["user", "password", "host", "port", "database"]
    if any(key not in keys for key in settings.keys()):
        raise Exception("Файл настроек не правельный")
    return get_engine(settings["user"],
                      settings["password"],
                      settings["host"],
                      settings["port"],
                      settings["database"])
    
def get_session():
    engine = get_engine_from_settings()
    return sessionmaker(bind=engine)() # type: ignore


def connect_bd():
    return psycopg2.connect(
        database=settings["database"],
        user=settings["user"],
        password=settings["password"],
        host=settings["host"],
        port=settings["port"],
        connect_timeout=100
    )
    
def create_bd_23():
    try:
        connection = connect_bd()
        connection.autocommit = True
        # создаем курсор для SQL
        with connection.cursor() as cursor:
            cursor.execute(
                """ CREATE TABLE phrases(
                    search_query TEXT,
                    number_requests INT8
                    );"""
            )
            print("База создана")
        # записываем данные в таблицы
    except Exception as err:
        logging.exception(err)

def create_bd():
    try:
        connection = connect_bd()
        connection.autocommit = True
        # создаем курсор для SQL
        with connection.cursor() as cursor:
            cursor.execute(
                """ CREATE TABLE search_results32(
                    id INT8,
                    query TEXT,
                    number INT8,
                    page INT8,
                    place INT8,
                    sale INT8
                    );"""
            )
            print("База создана")
        # записываем данные в таблицы
    except Exception as err:
        logging.exception(err)
        
def create_bd1():
    try:
        connection = connect_bd()
        connection.autocommit = True
        # создаем курсор для SQL
        with connection.cursor() as cursor:
            cursor.execute(
                """ CREATE TABLE id_saler(
                    id INT8
                    );"""
            )
            print("База создана")
        # записываем данные в таблицы
    except Exception as err:
        logging.exception(err)

def create_bd2():
    try:
        connection = connect_bd()
        connection.autocommit = True
        # создаем курсор для SQL
        for itt in range(2, 11):
            with connection.cursor() as cursor:
                cursor.execute(
                    f""" CREATE TABLE optimal_{itt}(
                        id INT8,
                        query TEXT,
                        number TEXT,
                        page TEXT,
                        place TEXT
                        );"""
                )
                print(f"База {itt} создана")
        # записываем данные в таблицы
    except Exception as err:
        logging.exception(err)
      
def create_bd22():
    try:
        connection = connect_bd()
        connection.autocommit = True
        # создаем курсор для SQL
        with connection.cursor() as cursor:
            cursor.execute(
                """ CREATE TABLE optimal_seo_2(
                    id INT8,
                    query TEXT,
                    number TEXT,
                    description_user TEXT,
                    query_competitors TEXT
                    );"""
            )
            print("База создана")
        # записываем данные в таблицы
    except Exception as err:
        logging.exception(err)
        
def create_bd24():
    try:
        connection = connect_bd()
        connection.autocommit = True
        # создаем курсор для SQL
        with connection.cursor() as cursor:
            cursor.execute(
                """ CREATE TABLE top_art_to_fraz(
                    query TEXT,
                    id TEXT
                    );"""
            )
            print("База создана")
        # записываем данные в таблицы
    except Exception as err:
        logging.exception(err)
        
def create_bd_25():
    try:
        connection = connect_bd()
        connection.autocommit = True
        # создаем курсор для SQL
        with connection.cursor() as cursor:
            cursor.execute(
                """ CREATE TABLE concurent(
                    query TEXT,
                    art_concurent TEXT
                    );"""
            )
            print("База создана")
        # записываем данные в таблицы
    except Exception as err:
        logging.exception(err)
        
def create_bd_26():
    try:
        connection = connect_bd()
        connection.autocommit = True
        # создаем курсор для SQL
        with connection.cursor() as cursor:
            cursor.execute(
                """ CREATE TABLE concurent_1(
                    art_concurent TEXT,
                    top_fraz TEXT
                    );"""
            )
            print("База создана")
        # записываем данные в таблицы
    except Exception as err:
        logging.exception(err)
        
def create_bd_27():
    try:
        connection = connect_bd()
        connection.autocommit = True
        # создаем курсор для SQL
        with connection.cursor() as cursor:
            cursor.execute(
                """ CREATE TABLE concurent_2(
                    query TEXT,
                    top_fraz TEXT
                    );"""
            )
            print("База создана")
        # записываем данные в таблицы
    except Exception as err:
        logging.exception(err)
        
def create_bd2222():
    try:
        connection = connect_bd()
        connection.autocommit = True
        # создаем курсор для SQL
        with connection.cursor() as cursor:
            cursor.execute(
                """ CREATE TABLE description_user_2(
                    id INT8,
                    description_user TEXT
                    );"""
            )
            print("База создана")
        # записываем данные в таблицы
    except Exception as err:
        logging.exception(err)
        
def create_users():
    try:
        connection = connect_bd()
        connection.autocommit = True
        # создаем курсор для SQL
        query = sql.SQL("alter table users add column {} timestamp")
        with connection.cursor() as cursor:
            cursor.execute(query.format(sql.Identifier("date_of_action")))
            print("База создана")
        # записываем данные в таблицы
    except Exception as err:
        logging.exception(err)
        
def create_phrases():
    try:
        connection = connect_bd()
        connection.autocommit = True
        cursor = connection.cursor()
        date = get_date_14()
        with connection.cursor() as cursor:
                        cursor.execute("""UPDATE users SET date_of_action = %s WHERE id = %s""", [
                                    date, 1323522063])
        # with connection.cursor() as cursor:
        #                 cursor.execute("""INSERT INTO users(id, is_bot, first_name, username, language_code, requests, 
        #                                date_of_action) VALUES(%s, %s, %s, %s, %s, %s, %s)""",
        #                                             [214760168, False, "Макс", "Maksberries", "ru", 500, date])
        print("Обновлено")
    except Exception as ex:
        logging.exception(ex)
        
def get_date_14():
    dt_now = datetime.datetime.now()
    end_date = dt_now - datetime.timedelta(days=1)
    return end_date
        
if __name__ == "__main__":
    create_phrases()