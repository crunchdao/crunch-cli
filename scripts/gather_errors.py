import crunch.api._errors as errors

for name in list(filter(lambda x: x.endswith('Exception'), vars(errors).keys())):
    print(f"from crunch.api._errors import {name} as {name}")
