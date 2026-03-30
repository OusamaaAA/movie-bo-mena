from enum import StrEnum


class SourceCode(StrEnum):
    FILMYARD = "filmyard"
    ELCINEMA = "elcinema"
    BOM = "bom"
    IMDB = "imdb"


class RunStatus(StrEnum):
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"


class ReviewStatus(StrEnum):
    OPEN = "open"
    RESOLVED = "resolved"
    DISMISSED = "dismissed"

