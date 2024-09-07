import pandas as pd
from typing import List
from src.DataLoader.datadef import FileType, FactorName


class FactorLoader:
    def __init__(self) -> None:
        self.folder = None
        self.factor_name_type = None
        self.start = None
        self.end = None
        self.tickers = None
        self.parallel = None
        self.file_type = None
        self.factor_paths = None

    def load_file(self, path: str, file_type: FileType, tickers: List | None) -> pd.DataFrame:
        if self.factor_name_type == FactorName.JIE:
            factor_name = path.split("/")[-1].split(".")[0][4:]
            df = pd.read_parquet(path, columns=tickers)
            df = df.reset_index(level=0).reset_index(drop=True)
            df.columns = ["date"] + tickers
            df["date"] = pd.to_datetime(df["date"].astype(str))
            df = pd.melt(df, id_vars=["date"], value_vars=tickers, var_name="ticker", value_name="factor_value")
            df["factor_name"] = factor_name
            return df
        else:
            return pd.DataFrame()

    def set_params(
            self,
            folder: str,
            file_type: FileType,
            factor_name_type: FactorName,
            start: pd.Timestamp,
            end: pd.Timestamp,
            tickers: List | None,
            factors: List,
            parallel: int,
    ) -> None:
        self.folder = folder
        self.factor_name_type = factor_name_type
        self.start = start
        self.end = end
        self.tickers = tickers
        self.parallel = parallel
        self.file_type = file_type

        if self.factor_name_type == FactorName.JIE:
            pattern = FactorName.JIE.value
            self.factor_paths = [pattern.replace("||n||", str(idx)) for idx in factors]
        else:
            raise NotImplementedError

    def build(
            self,
            folder: str,
            file_type: FileType,
            factor_name_type: FactorName,
            start: pd.Timestamp,
            end: pd.Timestamp,
            tickers: List | None,
            factors: List,
            parallel: int,
    ) -> pd.DataFrame:
        self.set_params(
            folder=folder,
            file_type=file_type,
            factor_name_type=factor_name_type,
            start=start,
            end=end,
            tickers=tickers,
            factors=factors,
            parallel=parallel,
        )
        files = [self.load_file(
            path=f"{self.folder}/{factor}",
            file_type=self.file_type,
            tickers=self.tickers,
        ) for factor in self.factor_paths]
        full_df = pd.concat(files)
        factor_df = pd.pivot_table(full_df, index=["date", "ticker"], columns="factor_name",
                                   values="factor_value", dropna=False)
        return factor_df

# %%
if __name__ == "__main__":
    loader = FactorLoader()
    factor_df = loader.build(
        folder="~/Downloads/factor_datas",
        file_type=FileType.PARQUET,
        factor_name_type=FactorName.JIE,
        start=pd.to_datetime("20160101"),
        end=pd.to_datetime("20170101"),
        tickers=['000001', '000002'],
        factors=[1, 2, 3],
        parallel=0,
    )
    print(factor_df.head())