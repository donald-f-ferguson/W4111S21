from Services.DataServices.RDBDataTable import RDBDataTable

class ServiceBase():

    def __init__(self, config_info):
        self._configInfo = config_info
        self._data_table = RDBDataTable(
            config_info["db_name"],
            config_info["table_name"],
            connect_info=config_info["db_connect_info"],
            key_columns=config_info.get("key_columns", None),
            debug=False
        )

    def find_by_template(self, template, fields=None, limit=None, offset=None, order_by=None, context=None):

        result = self._data_table.find_by_template(template, field_list=fields, limit=limit, offset=offset,
                                                   order_by=order_by)
        return result

    def find_by_primary_key(self, key_column_values, fields=None, context=None):

        result = self._data_table.find_by_primary_key(key_column_values, fields)
        return result

    def get_count(self):
        result = self._data_table.get_count()
        return result

    def get_by_pattern(self, column, pattern):
        result = self._data_table.get_by_pattern(column, pattern)
        return result




