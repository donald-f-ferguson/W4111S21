import csv
import pymysql
import copy

_conn = None

def get_connection():
    global _conn

    _conn = pymysql.connect(
        host="localhost",
        db="aaaaaaaNetflix",
        user="dbuser",
        password="dbuserdbuser",
        autocommit=True
    )

    return _conn


def save_countries(country_names):

    con = get_connection()
    cur = con.cursor()

    q = "insert into country_names(country_name) values(%s)"

    for c in country_names:
        res = cur.execute(q, (c))

    cur.close()


def load_csv(fn):

    result = []
    with open(fn, "r") as in_file:
        dict_rdr = csv.DictReader(in_file)
        for r in dict_rdr:
            result.append(r)

    return result


def load_netflix_file():

    netflix_dir = "/Users/donaldferguson/Dropbox/Columbia/W4111_S21_New/Data/"
    fn = "netflix_titles.csv"

    raw_data = load_csv(netflix_dir + fn)

    print("load_netflix_file: Loaded ", len(raw_data), "records.")

    return raw_data


def trim_values(vals):

    result = []
    for v in vals:
        v = v.strip()
        result.append(v)

    return result


def explode_unique_values(string_list):

    result = []

    for s in string_list:
        s = s.split(",")
        result.extend(s)

    result = set(result)

    return result


def get_unique_values(dict_list, column_name):
    column_values = list()

    for m in dict_list:
        t = m.get(column_name, None)
        if t is not None and len(t) > 0:
            column_values.append(t)

    column_values = set(column_values)

    return column_values


def convert_null_string(dict_list, column_name, null_strings):

    result = []

    for d in dict_list:
        n = copy.copy(d)
        v = n.get(column_name, None)
        if v is not None:
            if v in null_strings:
                v = None
                n[column_name] = v
        result.append(n)

    return result





def drive_it():
    net_data = load_netflix_file()

    media_types = get_unique_values(net_data, "type")
    print("The media types are:\n", media_types)

    country_names = get_unique_values(net_data, "country")
    country_names = explode_unique_values(country_names)
    country_names = trim_values(country_names)
    country_names.remove('')
    print("The country names are:\n", country_names)
    save_countries(country_names)



drive_it()