from Services.BaseResource import ResourceBase

class PersonTO():

    def __init__(self):
        pass


class PersonService(ResourceBase):

    def __init__(self, config_info):
        super().__init__(config_info)