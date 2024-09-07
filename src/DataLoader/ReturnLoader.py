from enum import Enum
from typing import List

import pandas as pd

class ReturnType(Enum):
    JIE_O2O = "o2o_label.parquet"
    JIE_V30 = "v30_label.parquet"

class ReturnLoader:
    def __init__(self):
        self.folder = None
        self.return_type: ReturnType | None = None
        self.start = None
        self.end = None
        self.tickers = None

    def load_label(self, path: str) -> pd.DataFrame:
        if self.return_type in [ReturnType.JIE_O2O, ReturnType.JIE_V30]:
            return_name = self.return_type.value.split(".")[0]
            label_df = pd.read_parquet(path, columns=self.tickers)

            label_df["date"] = pd.to_datetime(label_df.index.astype(str))
            label_df = label_df[label_df["date"].between(self.start, self.end)]
            label_df = pd.melt(label_df, id_vars=["date"], value_vars=self.tickers,
                               var_name="ticker", value_name=return_name)
            return label_df

    def set_params(
            self,
            folder: str,
            return_type: ReturnType,
            start: pd.Timestamp,
            end: pd.Timestamp,
            tickers: List[str],
    ):
        self.folder = folder
        self.return_type = return_type
        self.start = start
        self.end = end
        self.tickers = tickers

    def build(
            self,
            folder: str,
            return_type: ReturnType,
            start: pd.Timestamp,
            end: pd.Timestamp,
            tickers: List[str],
    ):
        self.set_params(folder, return_type, start, end, tickers)
        label_df = self.load_label(f"{self.folder}/{self.return_type.value}")
        return label_df


if __name__ == "__main__":
    loader = ReturnLoader()
    labels = loader.build(
        folder="~/Downloads/factor_datas/label",
        return_type=ReturnType.JIE_O2O,
        start=pd.to_datetime("20160101"),
        end=pd.to_datetime("20170101"),
        tickers=['000001', '000002'],
    )