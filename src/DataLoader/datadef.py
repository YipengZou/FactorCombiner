from enum import Enum


class FileType(Enum):
    PARQUET = ".parquet"
    CSV = ".csv"
    FEATHER = ".feather"


class FactorName(Enum):
    JIE = "JIE_factor_||n||/JIE_factor_||n||.v1.parquet"
