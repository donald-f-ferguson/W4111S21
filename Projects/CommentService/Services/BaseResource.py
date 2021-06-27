from Services.DataServices.RDBDataTable import RDBDataTable

class ResourceBase():

    def __init__(self, config_info):
        self._configInfo = config_info
        self._data_table = RDBDataTable(
            config_info["table_name"],
            connect_info=config_info["db_connect_info"],
            key_columns=config_info.get("key_columns", None),
        )

    def _get_key(self, jto):
        c_info = self._configInfo
        key_cols = c_info.get("key_columns", None)

        if key_cols is not None and len(key_cols) > 0:
            result = {k:jto[k] for k in key_cols}
        else:
            result = None

        return result

    def find_by_template(self, template, fields=None, limit=None, offset=None, order_by=None, context=None):

        result = self._data_table.find_by_template(template, fields=fields, limit=limit, offset=offset)
        return result

    def find_by_primary_key(self, key_column_values, fields=None, context=None):

        result = self._data_table.find_by_primary_key(key_column_values, fields)
        if result and len(result) >= 0:
            result = result[0]
        return result

    def get_count(self):
        result = self._data_table.get_count()
        return result

    def get_by_pattern(self, column, pattern):
        result = self._data_table.get_by_pattern(column, pattern)
        return result

    def create(self, transfer_json, context=None):

        result = self._data_table.insert(transfer_json)
        if result:
            result = self._get_key(transfer_json)

        return result

    def update(self, key_values, transfer_json, context=None):

        template = dict(zip(self._configInfo.get("key_columns"), key_values))

        result = self._data_table.update(template, transfer_json)
        if result:
            result = self._get_key(template)

        return result





