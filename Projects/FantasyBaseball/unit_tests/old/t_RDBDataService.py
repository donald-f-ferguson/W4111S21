import Services.DataServices.RDBDataService as data_service


def t1():
    template = {"primary_name": "Tom Hanks"}
    res = data_service.retrieve_by_template(
        "imdbnew", "name_basics", t=template
    )
    print(res)


def t2():

    template = {"pattern": "%hanks%", "property": "primary_name"}

    res = data_service.retrieve_by_pattern(
        "aaaIMDBF20fixed", "name_basics_fixed", t=template
    )
    print(res)


def t3():

    col = "nameLast"
    pattern = "Willi%"

    res = data_service.retrieve_by_pattern("la")


#t1()
t2()



