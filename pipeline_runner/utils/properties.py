from configparser import ConfigParser


class CustomConfigParser:

    def __init__(self, job_properties: ConfigParser):

        self._job_properties = job_properties

    def __getitem__(self, key: str) -> str:

        return self._job_properties["configuration"][key]
