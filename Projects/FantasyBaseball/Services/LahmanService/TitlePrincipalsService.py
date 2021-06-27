import Services.DataServices.RDBDataService as data_service

def get_title_principal_template(t, fast=False):

    if fast:
        tn = "title_principals_fast"
    else:
        tn = "title_principals"

    res = data_service.retrieve_by_template("imdbnew", tn, t=t)

    if res:

        for r in res:
            del r["nconst"]

    else:
        res = None

    return res
