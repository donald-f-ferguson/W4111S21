import Services.DataServices.RDBDataService as data_service


def get_person_by_id(person_id, fast=False):

    temp = {"playerID": person_id}

    if fast:
        tn = "people"
    else:
        tn = "people"

    res = data_service.retrieve_by_template("lahmansbaseballdb", tn, t=temp)

    if res is None or len(res) < 1:
        res = None
    else:
        res = res[0]

    return res


def get_by_id(resource_id, fast=False):
    return get_person_by_id(resource_id, fast)


def get_person_by_lastName(nameLast):

    t = {"nameLast": nameLast}

    res = data_service.retrieve_by_template(
        "lahmansbaseballdb", "people", t=t
    )

    if res and len(res) >= 1:

        for r in res:

            r['personId'] = r['playerID']

    else:
        res = None

    return res
