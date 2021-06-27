from Services.DataServices.RDBDataTable_old import RDBDataTable
from Services.DataServices.TeamsRDBDataTable import TeamsRDBDataTable

import json

def t1():

    c_info = {
        "host": "w4111s21.ckkqqktwkcji.us-east-1.rds.amazonaws.com",
        "port" : 3306,
        "user" : "admin",
        "password": "W4!11C0lumb!a2C"
    }

    tbl = RDBDataTable("lahmansbaseballdb", "people", c_info)
    print("t1: Got back table = ", tbl)

    template = {"nameLast": "Williams", "birthCity": "San Diego"}
    field_list = ['playerID', 'nameLast', 'nameFirst',
                  'birthCity', 'birthCountry', 'birthYear']

    res = tbl.find_by_template(template, field_list=field_list)

    print("t1: query result = \n", json.dumps(res, indent=2, default=str))


def t2():
    c_info = {
        "host": "w4111s21.ckkqqktwkcji.us-east-1.rds.amazonaws.com",
        "port": 3306,
        "user": "admin",
        "password": "W4!11C0lumb!a2C",
    }

    tbl = RDBDataTable("lahmansbaseballdb", "batting", key_columns=['playerID', 'yearID', 'stint'],
                       connect_info=c_info)
    print("t2: Got back table = ", tbl)

    field_list = ['playerID', 'teamID', 'yearID', 'stint', 'G', 'AB', 'R', 'H']

    res = tbl.find_by_primary_key(key_fields=['willite01', '1960', '1'], field_list=field_list)

    print("t2: query result = \n", json.dumps(res, indent=2, default=str))


def t3():
    c_info = {
        "host": "localhost",
        "port": 3306,
        "user": "dbuser",
        "password": "dbuserdbuser",
    }
    tbl = TeamsRDBDataTable("lahmansbaseballdb", "teams", connect_info=c_info, key_columns=["teamID", "yearID"])
    res = tbl.get_distinct_teams_count()
    print("t3: res = ", res)


def t4():
    c_info = {
        "host": "localhost",
        "port": 3306,
        "user": "dbuser",
        "password": "dbuserdbuser",
    }
    tbl = TeamsRDBDataTable("lahmansbaseballdb", "people", connect_info=c_info, key_columns=["playerID"])
    res = tbl.get_by_pattern("nameLast", "Willia%")
    print("t4: res = ", res)

#t1()
#t2()
#t3()
t4()