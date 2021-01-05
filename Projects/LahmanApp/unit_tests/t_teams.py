import json
import Services.LahmanService.TeamsService as t_service

def t1():
    tid = 'NYA'
    res = t_service.get_team_by_id(tid)

    print("Query result = \n", json.dumps(res, indent=3, default=str))


t1()