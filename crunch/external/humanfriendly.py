# https://pypi.org/project/humanfriendly/

# from humanfriendly/text.py
def format(text, *args, **kw):
    if args:
        text %= args
    if kw:
        text = text.format(**kw)
    return text


# from humanfriendly/__init__.py
def coerce_boolean(value):
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in ('1', 'yes', 'true', 'on'):
            return True
        elif normalized in ('0', 'no', 'false', 'off', ''):
            return False
        else:
            msg = "Failed to coerce string to boolean! (%r)"
            raise ValueError(format(msg, value))
    else:
        return bool(value)
