import copy
from flask import request
import json
import logging
from datetime import datetime

logger = logging.getLogger()


_default_limit = 10

def _get_and_remove_arg(args, arg_name):
    val = copy.copy(args.get(arg_name, None))
    if val is not None:
        del args[arg_name]

    return args, val


def _de_array_args(args):
    result = {}

    if args is not None:
        for k, v in args.items():
            if type(v) == list:
                result[k] = ",".join(v)
            else:
                result[k] = v

    return result


# 1. Extract the input information from the requests object.
# 2. Log the information
# 3. Return extracted information.
#
def log_and_extract_input(method, path_params=None):
    path = request.path
    args = dict(request.args)
    args = _de_array_args(args)
    data = None
    headers = dict(request.headers)
    method = request.method
    host_url = request.host_url

    args, limit = _get_and_remove_arg(args, "limit")
    args, offset = _get_and_remove_arg(args, "offset")
    args, order_by = _get_and_remove_arg(args, "order_by")
    args, fields = _get_and_remove_arg(args, "fields")

    if limit is None:
        limit = _default_limit

    try:
        if request.data is not None:
            data = request.json
        else:
            data = None
    except Exception as e:
        # This would fail the request in a more real solution.
        data = "You sent something but I could not get JSON out of it."

    log_message = str(datetime.now()) + ": Method " + method

    inputs = {
        "path": path,
        "method": method,
        "path_params": path_params,
        "query_params": args,
        "headers": headers,
        "body": data,
        "limit": limit,
        "offset": offset,
        "order_by": order_by,
        "url": request.url,
        "base_url": request.base_url,
        "fields": fields,
        "host_url": host_url
    }

    log_message += " received: \n" + json.dumps(inputs, indent=2)
    logger.debug(log_message)

    return inputs


def log_response(method, status, data, txt):
    msg = {
        "method": method,
        "status": status,
        "txt": txt,
        "data": data
    }

    logger.debug(str(datetime.now()) + ": \n" + json.dumps(msg, indent=2, default=str))
