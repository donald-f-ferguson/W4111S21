import pymysql.cursors

db_schema = None                                # Schema containing accessed data
cnx = None                                      # DB connection to use for accessing the data.
key_delimiter = '_'                             # This should probably be a config option.

_db_connection = None


def _get_db_connection():
    global _db_connection

    if _db_connection is None:
        _db_connection = pymysql.connect(
            host="w4111s21.ckkqqktwkcji.us-east-1.rds.amazonaws.com",
            port=3306,
            db="lahmansbaseballdb",
            user="admin",
            password="W4!11C0lumb!a2C",
            cursorclass=pymysql.cursors.DictCursor
        )
    return _db_connection


def run_q(cnx, q, args, fetch=False, commit=True):

    result = None
    cursor = None
    try:
        cursor = cnx.cursor()
        result = cursor.execute(q, args)
        if fetch:
            result = cursor.fetchall()
        if commit:
            cnx.commit()
        cursor.close()
    except Exception as original_e:
        cnx.rollback()
        cursor.close()
        print("run_q exception e = ", original_e)
        raise(original_e)

    return result


# Given one of our magic templates, forms a WHERE clause.
# { a: b, c: d } --> WHERE a=b and c=d. Currently treats everything as a string.
# We can fix this by using PyMySQL connector query templates.
def templateToWhereClause(t):
    s = ""
    for k,v in t.items():
        if s != "":
            s += " AND "
        s += k + "='" + str(v) + "'"

    if s != "":
        s = "WHERE " + s;

    return s


# Given a table, template and list of fields. Return the result.
def retrieve_by_template(dbname, table, t, fields=None, limit=None, offset=None, orderBy=None):

    r = None

    try:
        if t is not None:
            w = templateToWhereClause(t)
        else:
            w = ""

        if orderBy is not None:
            o = "order by " + ",".join(orderBy['fields']) + " " + orderBy['direction'] + " "
        else:
            o = ""

        if limit is not None:
            w += " LIMIT " + str(limit)
        if offset is not None:
            w += " OFFSET " + str(offset)

        if fields is None:
            fields = " * "

        cnx = _get_db_connection()
        q = "SELECT " + fields + " FROM " + dbname + "." + table + " " + w + ";"

        r = run_q(cnx, q, None, fetch=True, commit=True)

    except Exception as e:
        raise(e)

    return r


def retrieve_by_pattern(dbname, table, t, fields=None, limit=None, offset=None, orderBy=None):

    try:

        p = t["pattern"]
        f = t["property"]

        w = " where " + f + " like '" + p + "'"

        if orderBy is not None:
            o = "order by " + ",".join(orderBy['fields']) + " " + orderBy['direction'] + " "
        else:
            o = ""

        if limit is not None:
            w += " LIMIT " + str(limit)
        if offset is not None:
            w += " OFFSET " + str(offset)

        if fields is None:
            fields = " * "

        cnx = _get_db_connection()
        q = "SELECT " + fields + " FROM " + dbname + "." + table + " " + w + ";"

        r = run_q(cnx, q, None, fetch=True, commit=True)

    except Exception as e:
        raise(e)

    return r


















