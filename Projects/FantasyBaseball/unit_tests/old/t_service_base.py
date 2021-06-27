from Services.LahmanService.OldPersonService import PersonService

def t1():

    config_info = {
        "db_name": "lahmansbaseballdb",
        "table_name": "people",
        "db_connect_info" : {
            "host": "localhost",
            "user": "dbuser",
            "password": "dbuserdbuser"
        }
    }

    p_svc = PersonService(config_info)


def t2():

    config_info = {
        "db_name": "lahmansbaseballdb",
        "table_name": "people",
        "db_connect_info" : {
            "host": "localhost",
            "user": "dbuser",
            "password": "dbuserdbuser"
        }
    }

    p_svc = PersonService(config_info)
    res = p_svc.find_by_template({"nameLast": "Williams"})
    print("t2 result = ", res)


def t3():

    config_info = {
        "db_name": "lahmansbaseballdb",
        "table_name": "batting",
        "db_connect_info" : {
            "host": "localhost",
            "user": "dbuser",
            "password": "dbuserdbuser"
        },
        "key_columns": ["playerID", "yearID", "stint"]
    }

    p_svc = PersonService(config_info)
    res = p_svc.find_by_primary_key(["willite01", "1960", "1"],
                                    fields=[
                                        'playerID', 'teamID', 'yearID', 'stint', 'H', 'HR', 'RBI', 'R'
                                    ])
    print("t3 result = ", res)


#t1()
#t2()
t3()