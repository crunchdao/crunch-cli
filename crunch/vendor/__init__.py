class NoVendorModuleException(Exception):

    def __init__(self, competition_name: str):
        super().__init__(f"no vendor module for competition `{competition_name}`")

        self.competition_name = competition_name


def find(competition_name: str):
    if competition_name in ["datacrunch", "datacrunch-rally"]:
        from . import datacrunch

        return datacrunch

    return None


def get(competition_name: str):
    module = find(competition_name)

    if module is None:
        raise NoVendorModuleException(competition_name)

    return module
