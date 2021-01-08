import copy
import json

import logging

from datetime import datetime

from flask import Flask, Response
from flask import Flask, request, render_template, jsonify
from flask_cors import CORS

import utils.rest_utils as rest_utils

import Services.LahmanService.PersonService as person_service
import Services.LahmanService.player_performance as player_performance_svc
import Services.LahmanService.TeamsService as team_service


_service_factory = {
    "person": person_service,
    "player_performance": player_performance_svc,
    "teams": team_service
}

def _get_service_by_name(s_name):

    result = _service_factory.get(s_name, None)
    return result


app = Flask(__name__)
CORS(app)

##################################################################################################################

# TODO We need a more thorough health check ping and integration with an availability manager
# This function performs a basic health check. We will flesh this out.
@app.route("/health", methods=["GET"])
def health_check():
    rsp_data = {"status": "healthy", "time": str(datetime.now())}
    rsp_str = json.dumps(rsp_data)
    rsp = Response(rsp_str, status=200, content_type="app/json")
    return rsp


# TODO Remove later. Solely for explanatory purposes.
@app.route("/api/demo/<parameter>", methods=["GET", "POST"])
def demo(parameter):
    inputs = rest_utils.log_and_extract_input(demo, {"parameter": parameter})

    msg = {
        "/demo received the following inputs": inputs
    }

    rsp = Response(json.dumps(msg), status=200, content_type="application/json")
    return rsp


##################################################################################################################
# Actual routes begin here.
#
#

@app.route("/api/players/<player_id>/career_batting", methods=["GET"])
def get_career_batting(player_id):

    try:
        # DFF TODO Change this to a DTO/object with properties from a dict.
        inputs = rest_utils.log_and_extract_input(get_career_batting, {"parameters":player_id })

        service = _get_service_by_name("player_performance")

        if service is not None:
            if inputs["method"] == "GET":
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


@app.route("/api/<resource>", methods=["GET"])
def get_resource_by_query(resource):

    try:
        # DFF TODO Change this to a DTO/object with properties from a dict.
        inputs = rest_utils.log_and_extract_input(get_resource_by_query, {"parameter": (resource)})

        if inputs["method"] == "GET":

            template = inputs.get("query_params", None)
            service = _get_service_by_name(resource)
            nameLast = template["nameLast"]

            if service is not None:
                res = person_service.get_person_by_lastName(nameLast)
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
        print("/api/person, e = ", e)
        rsp = Response("INTERNAL ERROR", status=500, content_type="text/plain")

    return rsp


@app.route("/api/<resource>/<resource_id>", methods=["GET"])
def get_resource_by_id(resource, resource_id):

    try:
        # DFF TODO Change this to a DTO/object with properties from a dict.
        inputs = rest_utils.log_and_extract_input(get_resource_by_id, {"parameter": (resource, resource_id)})

        if inputs["method"] == "GET":
            service = _get_service_by_name(resource)

            if service is not None:
                res = person_service.get_person_by_id(resource_id)
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
        print("/api/person, e = ", e)
        rsp = Response("INTERNAL ERROR", status=500, content_type="text/plain")

    return rsp


@app.route("/api/teams/<team_id>", methods=["GET"])
def get_team_by_id(team_id):

    try:
        # DFF TODO Change this to a DTO/object with properties from a dict.
        inputs = rest_utils.log_and_extract_input(get_team_by_id, {"parameter": team_id})

        if inputs["method"] == "GET":
            service = _get_service_by_name("teams")

            if service is not None:
                res = team_service.get_team_by_id(team_id)
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
        print("/api/teams, e = ", e)
        rsp = Response("INTERNAL ERROR", status=500, content_type="text/plain")

    return rsp


if __name__ == '__main__':
    #host, port = ctx.get_host_and_port()

    # DFF TODO We will handle host and SSL certs different in deployments.
    app.run(host="0.0.0.0", port=5001)
