import enum


class Language(enum.Enum):

    PYTHON = "PYTHON"
    R = "R"

    def __repr__(self):
        return self.name


class SplitKeyType(enum.Enum):

    INTEGER = "INTEGER"
    STRING = "STRING"

    def __repr__(self):
        return self.name
