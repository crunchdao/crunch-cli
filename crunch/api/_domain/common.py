from crunch.api._base import ApiEnum


class GpuRequirement(ApiEnum):
    OPTIONAL = "OPTIONAL"
    RECOMMENDED = "RECOMMENDED"
    REQUIRED = "REQUIRED"
