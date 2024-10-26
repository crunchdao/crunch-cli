# https://pypi.org/project/inflection/

import re


# from inflection/__init.py
def underscore(word: str) -> str:
    word = re.sub(r"([A-Z]+)([A-Z][a-z])", r'\1_\2', word)
    word = re.sub(r"([a-z\d])([A-Z])", r'\1_\2', word)
    word = word.replace("-", "_")
    return word.lower()


# from inflection/__init.py
def camelize(string: str, uppercase_first_letter: bool = True) -> str:
    if uppercase_first_letter:
        return re.sub(r"(?:^|_)(.)", lambda m: m.group(1).upper(), string)
    else:
        return string[0].lower() + camelize(string)[1:]
