from Services.DataServices.Neo4JDataTable import HW3Graph as HW3Graph


def t1():
    hw3g = HW3Graph()


def t2():
    q = "match (n:Person {name: 'Tom Hanks'}) return n"
    hw3g = HW3Graph()
    res = hw3g.run_q(q, args=None)
    print("t2 -- res =", res)

    tmp = { "label": "Person", "template": {"name": "Tom Hanks"}}
    res2 = hw3g.find_nodes_by_template(tmp)
    print("t2 -- res =", res2)


def t3():

    hw3g = HW3Graph()
    res = hw3g.create_node(label="Person", name="Fred Astaire", nconst='nm0000001', firstName='Fred',
                     lastName='Astaire')
    print("t3 -- res = ", res)


t2()