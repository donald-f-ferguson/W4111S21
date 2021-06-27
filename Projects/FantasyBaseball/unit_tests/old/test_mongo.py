from Services.DataServices.MongoDBTable import MongoDBTable as MongoDBTable
import json
from datetime import datetime

c_info = {
    "host": "localhost",
    "port": 27017,
    "db": "fantasy_basevall"
}


def t1():
    m_table = MongoDBTable("fantasy_comments", key_columns=["playerID"], connect_info=c_info)
    print("t1: Connected.")
    doc = {
        "playerID": "willite01",
        "Comment": "Greatest player ever!"
    }
    res = m_table.insert(doc)
    print("t1: Inserted!")


def t2():
    m_table = MongoDBTable("fantasy_comments", key_columns=["playerID"], connect_info=c_info)
    print("t2: Connected.")
    res = m_table.find_by_primary_key(["willite01"])
    print("t2: returned", res)


def t3():
    m_table = MongoDBTable("fantasy_comments", key_columns=["playerID"], connect_info=c_info)
    print("t3: Connected.")
    res = m_table.update_by_key(["willite01"], {
        "$set": {
            "responses": []
        }
    })
    print("t3: returned", json.dumps(res, indent=2, default=str))
    print("Matched count: ", res.matched_count)
    print("Modified count: ", res.modified_count)


def t4():
    m_table = MongoDBTable("fantasy_comments", key_columns=["playerID"], connect_info=c_info)
    print("t4: Connected.")
    res = m_table.update_by_key(["willite01"], {
        "$push": {
            "responses": {
                "user": "dff9", "comment": "Unquestionably!",
                "timestamp": datetime.now()}
        }
    })
    print("t4: returned", json.dumps(res, indent=2, default=str))
    print("Matched count: ", res.matched_count)
    print("Modified count: ", res.modified_count)


def t5():
    m_table = MongoDBTable("fantasy_comments", key_columns=["playerID"], connect_info=c_info)
    print("t5: Connected.")
    res = m_table.find_by_template( {"playerID": "willite01"})
    print("t5: returned", json.dumps(res, indent=2, default=str))


def t6():
    m_table = MongoDBTable("fantasy_comments", key_columns=["playerID"], connect_info=c_info)
    print("t6: Connected.")
    tmp = {
        "responses": "dff9"
    }
    res = m_table.find_by_template( tmp )
    print("t6: returned", json.dumps(res, indent=2, default=str))



#t2()
#t3()
#t4()
#t5()
t6()