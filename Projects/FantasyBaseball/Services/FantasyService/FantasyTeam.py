from Services.BaseResource import ResourceBase


class FantasyTeamTO():

    def __init__(self, teamID=None, lgID=None, managerID=None, teamName=None, createdTimestamp=None):
        teamID = teamID
        lgID = lgID
        managerID = managerID
        teamName = teamName
        createdTimestamp = createdTimestamp


class FantasyTeam(ResourceBase):

    def __init__(self, config_info):
        super().__init__(config_info)