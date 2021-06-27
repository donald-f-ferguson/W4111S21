import json

# DFF TODO -- Not critical for W4111, but should switch from print statements to logging framework.
import logging

from datetime import datetime

from flask import Flask, Response
from flask import Flask, request, render_template, jsonify
from flask_cors import CORS

import utils.rest_utils as rest_utils

from Services.CommentService.FantasyTeam import FantasyTeam as FantasyTeam
from Services.CommentService.FantasyManager import FantasyManager as FantasyManager


# DFF TODO - We should not hardcode this here, and we should do in a context/environment service.
# OK for W4111 - This is not a course on microservices and robust programming.
#
#
_service_factory = {
    "fantasy_team": FantasyTeam({
        "db_name": "aaaaaFantasyBaseball",
        "table_name": "fantasy_team",
        "db_connect_info": {
            "host": "localhost",
            "user": "dbuser",
            "password": "dbuserdbuser",
            "db": "aaaaaFantasyBaseball"
        },
        "key_columns": ["teamID"]
    }),
    "fantasy_manager": FantasyManager({
        "db_name": "aaaaaFantasyBaseball",
        "table_name": "fantasy_manager",
        "db_connect_info": {
            "host": "localhost",
            "user": "dbuser",
            "password": "dbuserdbuser",
            "db": "aaaaaFantasyBaseball"
        },
        "key_columns": ["uni"]
    })
}


# Given the "resource"
def _get_service_by_name(s_name):
    result = _service_factory.get(s_name, None)
    return result


app = Flask(__name__)
CORS(app)

##################################################################################################################


# DFF TODO A real service would have more robust health check methods.
@app.route("/health", methods=["GET"])
def health_check():
    rsp_data = {"status": "healthy", "time": str(datetime.now())}
    rsp_str = json.dumps(rsp_data)
    rsp = Response(rsp_str, status=200, content_type="app/json")
    return rsp


# TODO Remove later. Solely for explanatory purposes.
# The method take any REST request, and produces a response indicating what
# the parameters, headers, etc. are. This is simply for education purposes.
#
@app.route("/api/demo/<parameter1>", methods=["GET", "POST", "PUT", "DELETE"])
@app.route("/api/demo/", methods=["GET", "POST", "PUT", "DELETE"])
def demo(parameter1=None):

    # Mostly for isolation. The rest of the method is isolated from the specifics of Flask.
    inputs = rest_utils.RESTContext(request, {"parameter1": parameter1})

    # DFF TODO -- We should replace with logging.
    r_json = inputs.to_json()
    msg = {
        "/demo received the following inputs": inputs.to_json()
    }
    print("/api/demo/<parameter> received/returned:\n", msg)

    rsp = Response(json.dumps(msg), status=200, content_type="application/json")
    return rsp


##################################################################################################################
# Actual routes begin here.
#
#

@app.route("/api/<resource>/count", methods=["GET"])
def get_resource_count(resource):
    """
    Currently not implemented. Need to revise.
    """
    rsp = Response("NOT IMPLEMENTED", status=501)
    return rsp

    """
    try:
        # DFF TODO Change this to a DTO/object with properties from a dict.
        inputs = rest_utils.RESTContext(request)
        rest_utils.log_request("get_resource_count", inputs)

        service = _get_service_by_name(resource)

        if service is not None:
            res = service.get_count()
            if res is not None:
                res = {"count": res}
                res = json.dumps(res, default=str)
                rsp = Response(res, status=200, content_type="application/JSON")
            else:
                rsp = Response("NOT FOUND", status=404, content_type="text/plain")
        else:
            rsp = Response("NOT FOUND", status=404)

    except Exception as e:
        # TODO Put a common handler to catch excceptions, log the error and return correct
        # HTTP status code.
        print("/api/" + resource + "/count, e = ", e)
        rsp = Response("INTERNAL ERROR", status=500, content_type="text/plain")

    return rsp
    """


@app.route("/api/people/<player_id>/career_batting", methods=["GET"])
def get_career_batting(player_id):

    rsp = Response("NOT IMPLEMENTED", status=501)
    return rsp

    try:
        # DFF TODO Change this to a DTO/object with properties from a dict.
        inputs = rest_utils.RESTContext(request)
        rest_utils.log_request("get_resource_count", inputs)

        service = _get_service_by_name("player_performance")

        if service is not None:
            if inputs.method == "GET":
                res = service.get_career_batting(player_id)
                if res is not None:
                    res = json.dumps(res, default=str)
                    rsp = Response(res, status=200, content_type="application/JSON")
                else:
                    rsp = Response("NOT FOUND", status=404, content_type="text/plain")
            else:
                rsp = Response("NOT IMPLEMENTED", status=501)
        else:
            rsp = Response("NOT FOUND", status=404, content_type="text/plain")

    except Exception as e:
        # TODO Put a common handler to catch excceptions, log the error and return correct
        # HTTP status code.
        print("/api/players/<player_id>/career_batting, e = ", e)
        rsp = Response("INTERNAL ERROR", status=500, content_type="text/plain")

    return rsp


@app.route("/api/<resource>", methods=["GET", "POST"])
def get_resource_by_query(resource):

    rsp = Response("INTERNAL ERROR", status=500, content_type="text/plain")

    try:
        # DFF TODO Change this to a DTO/object with properties from a dict.
        inputs = rest_utils.RESTContext(request)
        rest_utils.log_request("get_resource_by_query", inputs)

        if inputs.method == "GET":

            template = inputs.args
            service = _get_service_by_name(resource)

            if service is not None:
                res = service.find_by_template(template, inputs.fields)
                if res is not None:
                    res = json.dumps(res, default=str)
                    rsp = Response(res, status=200, content_type="application/JSON")
                else:
                    rsp = Response("NOT FOUND", status=404, content_type="text/plain")
        elif inputs.method == "POST":

            service = _get_service_by_name(resource)

            if service is not None:
                res = service.create(inputs.data)
                if res is not None:
                    key = "_".join(res.values())
                    headers = {"location": "/api/" + resource + "/" + key}
                    rsp = Response("CREATED", status=201, content_type="text/plain", headers=headers)
                else:
                    rsp = Response("UNPROCESSABLE ENTITY", status=422, content_type="text/plain")
        else:
            rsp = Response("NOT IMPLEMENTED", status=501)
    except Exception as e:
        # TODO Put a common handler to catch excceptions, log the error and return correct
        # HTTP status code.
        print("/api/<resource>, e = ", e)
        rsp = Response("INTERNAL ERROR", status=500, content_type="text/plain")

    return rsp


@app.route("/api/<resource>/<resource_id>", methods=["GET", "PUT", "DELETE"])
def resource_by_id(resource, resource_id):

    rsp = Response("INTERNAL ERROR", status=500, content_type="text/plain")

    try:
        
        inputs = rest_utils.RESTContext(request)
        rest_utils.log_request("resource_by_id", inputs)

        # The resource_id can map to a single attribute, e.g. SSNO
        # Or map to a composite key, e.g. {countrycode, phoneno}
        # We encode this as "countrycode_phoneno"
        resource_key_columns = rest_utils.split_key_string(resource_id)

        if inputs.method == "GET":
            service = _get_service_by_name(resource)

            if service is not None:
                res = service.find_by_primary_key(resource_key_columns, inputs.fields)
                if res is not None:
                    res = json.dumps(res, default=str)
                    rsp = Response(res, status=200, content_type="application/JSON")
                else:
                    rsp = Response("NOT FOUND", status=404, content_type="text/plain")
        elif inputs.method == "PUT":

            service = _get_service_by_name(resource)

            if service is not None:
                res = service.update(resource_key_columns, inputs.data)
                if res is not None:
                    rsp = Response("OK", status=200, content_type="text/plain")
                else:
                    rsp = Response("NOT FOUND", status=404, content_type="text/plain")
        else:
            rsp = Response("NOT IMPLEMENTED", status=501)

    except Exception as e:
        # DFF TODO -- Need to handle integrity exceptions, etc. more clearly, e.g. 422, etc.
        # TODO Put a common handler to catch excceptions, log the error and return correct
        # HTTP status code.
        print("/api/person, e = ", e)
        rsp = Response("INTERNAL ERROR", status=500, content_type="text/plain")

    return rsp


@app.route("/api/people/search/<pattern>", methods=["GET"])
def get_person_by_pattern(pattern):

    rsp = Response("INTERNAL ERROR", status=500, content_type="text/plain")

    try:
        # DFF TODO Change this to a DTO/object with properties from a dict.

        inputs = rest_utils.RESTContext(request)
        rest_utils.log_request("get_person_by_pattern", inputs)

        #resource_key_columns = rest_utils.split_key_string(resource_id)

        if inputs.method == "GET":
            service = _get_service_by_name("people")

            if service is not None:
                res = service.get_by_pattern("nameLast", pattern)
                if res is not None:
                    res = json.dumps(res, default=str)
                    rsp = Response(res, status=200, content_type="application/JSON")
                else:
                    rsp = Response("NOT FOUND", status=404, content_type="text/plain")
        else:
            rsp = Response("NOT IMPLEMENTED", status=501)
    except Exception as e:
        # TODO Put a common handler to catch excceptions, log the error and return correct
        # HTTP status code.
        print("/api/people/pattern, e = ", e)
        rsp = Response("INTERNAL ERROR", status=500, content_type="text/plain")

    return rsp

if __name__ == '__main__':
    #host, port = ctx.get_host_and_port()

    # DFF TODO We will handle host and SSL certs different in deployments.
    app.run(host="0.0.0.0", port=5001)
