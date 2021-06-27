import logging
import json

from Services.DataServices.RDBDataTable import RDBDataTable

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

def t1():

    config_info = {
        "user": "dbuser",
        "password": "dbuserdbuser",
        "host": "localhost",
        "db": "lahmansbaseballdb"
    }

    r_table = RDBDataTable("people", config_info)
    print("t1: Data table = ", r_table)
    keys = r_table.get_key_columns()
    print("t1: Key columns = ", keys)
    tmp = {"nameLast": "Williams", "birthCity": "San Diego"}
    res = r_table.find_by_template(tmp)

    print("t1: rows = \n", json.dumps(res, indent=2, default=str))



t1()

