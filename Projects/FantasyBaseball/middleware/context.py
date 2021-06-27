
"""
Convert to environment variables.
"""

context = {
    "MAX_TABLE_ROWS_TO_PRINT": 10
}

def get_context_value(c_name=None):

    if c_name is None:
        result = context
    else:
        result = context.get(c_name)

    return result