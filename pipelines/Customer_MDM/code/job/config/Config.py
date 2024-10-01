from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            STAGING_PERM_SCHEMA: str=None,
            STAGING_TEMP_SCHEMA1: str=None,
            PMRepositoryUserName: str=None,
            **kwargs
    ):
        self.spark = None
        self.update(STAGING_PERM_SCHEMA, STAGING_TEMP_SCHEMA1, PMRepositoryUserName)

    def update(self, STAGING_PERM_SCHEMA: str="", STAGING_TEMP_SCHEMA1: str="", PMRepositoryUserName: str="", **kwargs):
        prophecy_spark = self.spark
        self.STAGING_PERM_SCHEMA = STAGING_PERM_SCHEMA
        self.STAGING_TEMP_SCHEMA1 = STAGING_TEMP_SCHEMA1
        self.PMRepositoryUserName = PMRepositoryUserName
        pass
