import middleware.security as sec


def t1():
    email = "admin@contoso.org"
    pw = "admin"

    result = sec._validate_password(email, pw)
    print("Password verification result 1 = ", result)

    result = sec._validate_password(email + "1", pw)
    print("Password verification result 2 = ", result)

    result = sec._validate_password(email, pw + "1")
    print("Password verification result 3 = ", result)



def t2():
    u_info =  {
        "firstName": "Fan",
        "lastName": "Dude",
        "password": "admin",
        "email": "fan@contoso.org"
    }
    tok2 = sec._generate_user_token(u_info)
    print("t2: result token = ", tok2)

    val = sec._validate_user_token(tok2)
    print("t2: valid = ", val)

    tok2['firstName'] = "Fan" + "1"

    val = sec._validate_user_token(tok2)
    print("t3: valid = ", val)


#t1()
t2()
