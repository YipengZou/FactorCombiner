from enum import Enum

from enum import Enum, auto

class INDEX(Enum):
    def __str__(self):
        return f"{self.__class__.__name__}.{self.name}"

class CNI(INDEX):
    # import akshare as ak
    # ak.index_all_cni()
    GZ_1000 = "399311"
    GZ_300 = "399312"

