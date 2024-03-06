import enum


class GpuRequirement(enum.Enum):

    OPTIONAL = "OPTIONAL"
    RECOMMENDED = "RECOMMENDED"
    REQUIRED = "REQUIRED"

    def __repr__(self):
        return self.name
