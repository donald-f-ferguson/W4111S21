import csv
import re
import pymysql
from sqlalchemy import create_engine
import pandas as pd
db_url = 'mysql+pymysql://admin:W4!11C0lumb!a2C@w4111s21.ckkqqktwkcji.us-east-1.rds.amazonaws.com/harry_potter'




input_csv_file = "/Users/donaldferguson/Dropbox/Columbia/W4111_S21_New/W4111S21/Data/characters_fixed.csv"


def find_odd_characters(fn):

    result = []
    with open(fn, "r") as in_file:

        for l in in_file:
            for c in l:
                if ord(c) > 128:
                    result.append(c)

    result = set(result)
    return result


def clean_csv_file(fn, new_fn):

    headers = []
    cleaned = []

    with open(fn, "r") as in_file:
        csv_rdr = csv.DictReader(in_file)
        for r in csv_rdr:

            if len(headers) == 0:
                headers = r.keys()

            bm = str(b'')

            for k,v in r.items():
                tmp = r[k]
                tmp = re.sub(r'([0-9]).(")', r'\1\2', tmp)
                tmp = re.sub('†', ' ', tmp)
                tmp = re.sub(r'ñ|º|æ|ë|î|í|Ω', ' ', tmp)
                tmp = re.sub('  ', ' ', tmp)
                tmp = re.sub(bm, ' ', tmp)
                tmp = tmp.lstrip()

                r[k] = tmp

            cleaned.append(r)

    with open(new_fn, "w") as out_file:
        csv_write = csv.DictWriter(out_file, headers)
        csv_write.writeheader()
        for r in cleaned:
            csv_write.writerow(r)


def get_characters_map(in_file, out_file, field):

    result = []

    with open(in_file, "r") as in_file:
        csv_rdr = csv.DictReader(in_file)
        for r in csv_rdr:
            j = r[field]
            if j is not None and len(j) > 0:
                l = j.split("|")
                if l and len(l) > 0:
                    for k in l:
                        id = r['Id']
                        loy = k.strip()
                        print(field, " = ", id, loy)
                        result.append({"character_id": id, field: loy})

    with open(out_file, "w") as o_file:
        field_name = result[0].keys()
        out_csv = csv.DictWriter(o_file, fieldnames=field_name)
        out_csv.writeheader()
        for r in result:
            out_csv.writerow(r)



def load_csv(fn, table_name):

    engine = create_engine(db_url)

    df = pd.read_csv(fn)
    res = df.to_sql(table_name, con=engine, if_exists="replace")
    return res


def load_all():
    load_csv("harry_cleaned_3.csv", "characters")
    load_csv("skills.csv", "skills")
    load_csv("loyalties.csv", "loyalties")

def t1():

    res = find_odd_characters(input_csv_file)
    for c in res:
        print(ord(c))


def t2():
    clean_csv_file(input_csv_file, "harry_cleaned.csv")
    res = find_odd_characters(fn)
    foo = ''.join(res)
    foo = bytes(foo, encoding='utf-8')
    print("Weird characters = ", foo)


def t3():
    clean_csv_file("harry_cleaned_2.csv", "harry_cleaned_3.csv")
    res = find_odd_characters("harry_cleaned_3.csv")

    if len(res) > 0:
        foo = ''.join(res)
        foo = bytes(foo, encoding='utf-8')
        print("Weird characters = ", foo)
    else:
        print("No weird characters in harry_cleaned_3.csv")


def map_one_to_many():
    res = get_characters_map("harry_cleaned_3.csv", "loyalties.csv", "Loyalty")
    res = get_characters_map("harry_cleaned_3.csv", "skills.csv", "Skills")




#t1()
#t2()
#t3()
#map_one_to_many()
load_all()
