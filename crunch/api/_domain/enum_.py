from crunch.api._base import ApiEnum


class Language(ApiEnum):
    PYTHON = "PYTHON"
    R = "R"

    def as_requirement_language(self):
        from crunch_convert import RequirementLanguage

        try:
            return RequirementLanguage[self.name]
        except KeyError:
            raise ValueError(f"no requirement language for {self.name}") from None


class SplitKeyType(ApiEnum):
    INTEGER = "INTEGER"
    STRING = "STRING"
