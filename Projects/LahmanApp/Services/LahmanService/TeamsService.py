import pymysql
import Services.DataServices.RDBDataService as data_service

_cnx = None


def _get_cnx():
    global _cnx

    if _cnx is None:
        _cnx = pymysql.connect(
            host="w4111s21.ckkqqktwkcji.us-east-1.rds.amazonaws.com",
            port=3306,
            user="admin",
            password="W4!11C0lumb!a2C",
            cursorclass=pymysql.cursors.DictCursor
        )
    return _cnx


def get_team_by_id(team_id):

    sql = """
        SELECT teamID, name,
        min(yearID) as firstYear, max(yearID) as lastYear
        FROM lahmansbaseballdb.teams
            where teamID=%s
        group by teamID, name;
        """

    try:
        cnx = _get_cnx()
        cursor = cnx.cursor()

        msg = cursor.mogrify(sql, (team_id))
        print("SQL to submit = ", msg)
        res = cursor.execute(sql, (team_id))
        res = cursor.fetchall()

        cursor.close()

        return res

    except Exception as e:
        print("Exception e = ", e)
        cursor.close()


def get_team_by_template(t):

    res = data_service.retrieve_by_template("lahmansbaseballdb", "teams", t=t)

    return res

