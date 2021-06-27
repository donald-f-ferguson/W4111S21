import pymysql;

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


def get_career_batting(playerId):

    sql = """
        SELECT 
	        playerid, yearid, sum(G) as games,
		    sum(ab) as at_bats, sum(h) as hits, 
            sum(h - `2b` - `3b` - hr) as `S`,
            sum(`2b`) as `D`, sum(`3b`) as `T`, sum(hr) as HRs,
            sum(RBI) as RBIs, sum(BB) as BBs,
            if(sum(ab)=0, Null, (sum(h)/sum(ab))) as Avg
        FROM lahmansbaseballdb.batting
	    batting
            where playerid=%s
            group by playerId, yearID
            order by yearid asc;
        """

    try:
        cnx = _get_cnx()
        cursor = cnx.cursor()

        msg = cursor.mogrify(sql, (playerId))
        print("SQL to submit = ", msg)
        res = cursor.execute(sql, (playerId))
        res = cursor.fetchall()

        cursor.close()

        return res

    except Exception as e:
        print("Exception e = ",e)
        cursor.close()


