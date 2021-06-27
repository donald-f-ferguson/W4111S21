import pymysql
from Services.DataServices.TeamsRDBDataTable import TeamsRDBDataTable


import Services.DataServices.RDBDataService as data_service
from Services.LahmanService.ServiceBase import ServiceBase


class TeamsService(ServiceBase):

    def __init__(self, config_info):
        super().__init__(config_info)

        self._configInfo = config_info
        self._data_table = TeamsRDBDataTable(
            config_info["db_name"],
            config_info["table_name"],
            connect_info=config_info["db_connect_info"],
            key_columns=config_info.get("key_columns", None),
            debug=False
        )

    def get_count(self):
        res = self._data_table.get_distinct_teams_count()
        return res

    def get_team_summary(self, team_id, year_id):
        res = self._data_table.get_player_team_summary(team_id, year_id)
        return res


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


def get_player_summary(team_id, year_id):
    template = {"teamID": team_id, "yearID": year_id}
    res = data_service.retrieve_by_template('lahmansbaseballdb', "team_summary", t=template)
    return res


