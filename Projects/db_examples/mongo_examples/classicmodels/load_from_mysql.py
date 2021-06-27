import pymysql
from pymongo import MongoClient
from datetime import date, datetime
import decimal


_mysql_conn = None
_mongo_conn = None


def get_sql_conn():
    global _mysql_conn

    if _mysql_conn is None:
        _mysql_conn = pymysql.connect(
            host="localhost",
            password="dbuserdbuser",
            user="dbuser",
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=True
        )

    return _mysql_conn


def get_mongo_conn():
    global _mongo_conn

    if _mongo_conn is None:
        _mongo_conn = MongoClient()

    return _mongo_conn


def map_types(d):
    for k, v in d.items():
        tt = type(v)
        if isinstance(v, date):
            v = datetime(
                year=v.year,
                month=v.month,
                day=v.day,
            )
        if isinstance(v, decimal.Decimal):
            v = float(v)

        if isinstance(v, dict):
            v = map_types(v)

        if isinstance(v, list):
            new_v = []
            for e in v:
                new_v.append(map_types(e))

        d[k] = v


    return d


def table_to_mongo(sql_conn, sql_table, mongo_conn, mongo_collection):

    cur = sql_conn.cursor()
    rows = cur.execute("select * from " + sql_table)
    rows = cur.fetchall()

    for r in rows:
        new_r = map_types(r)
        mongo_collection.insert(dict(r))




def drive_it():
    sqlc = get_sql_conn()
    mongoc = get_mongo_conn()

    s_table = "classicmodelsnew.customers"
    m_collection = mongoc["classic_models"]
    m_collection= m_collection["customers"]

    #table_to_mongo(sqlc, s_table, mongoc, m_collection)
    s_table = "classicmodelsnew.productlines"
    m_collection = mongoc["classic_models"]
    m_collection = m_collection["productlines"]
    table_to_mongo(sqlc, s_table, mongoc, m_collection)

    s_table = "classicmodelsnew.payments"
    m_collection = mongoc["classic_models"]
    m_collection = m_collection["payments"]
    table_to_mongo(sqlc, s_table, mongoc, m_collection)



drive_it()
