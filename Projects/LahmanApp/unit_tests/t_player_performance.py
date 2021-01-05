import json
import Services.LahmanService.player_performance as p_service

def t1():
    pid = 'willite01'
    res = p_service.get_career_batting(pid)

    print("Query result = \n", json.dumps(res, indent=3, default=str))


t1()