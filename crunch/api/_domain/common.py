from enum import Enum


class GpuRequirement(Enum):

    OPTIONAL = "OPTIONAL"
    RECOMMENDED = "RECOMMENDED"
    REQUIRED = "REQUIRED"

    def __repr__(self):
        return self.name
