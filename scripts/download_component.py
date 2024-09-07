import pandas as pd

from src.Downloader.ComponentWeight import ComponentDownloader
from src.Downloader.datadef import CNI


if __name__ == "__main__":
    GZ_300 = ComponentDownloader().build(
        index_name=CNI.GZ_300,
        start=pd.to_datetime('2015-12-01'),
        end=pd.to_datetime('2022-01-01'),
    )
    GZ_300.reset_index(drop=True).to_feather("/Users/yip/Desktop/ComponentDate/Weights/GZ_300.feather")

    GZ_1000 = ComponentDownloader().build(
        index_name=CNI.GZ_1000,
        start=pd.to_datetime('2015-12-01'),
        end=pd.to_datetime('2022-01-01'),
    )
    GZ_1000.reset_index(drop=True).to_feather("/Users/yip/Desktop/ComponentDate/Weights/GZ_1000.feather")