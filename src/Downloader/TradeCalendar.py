import akshare as ak
import pandas as pd


class CalendarDownloader:
    def __init__(self):
        self.start = None
        self.end = None

    def get(self) -> pd.Series:
        calendar = ak.tool_trade_date_hist_sina()
        calendar["trade_date"] = pd.to_datetime(calendar["trade_date"])
        calendar = calendar[calendar["trade_date"].between(self.start, self.end)].reset_index(drop=True)
        return calendar["trade_date"]

    def set_params(
            self,
            start: pd.Timestamp,
            end: pd.Timestamp,
    ):
        self.start = start
        self.end = end

    def build(
            self,
            start: pd.Timestamp,
            end: pd.Timestamp,
    ) -> pd.Series:
        self.set_params(start, end)
        return self.get()


if __name__ == '__main__':
    downloader = CalendarDownloader()
    calendar_series = downloader.build(
        start=pd.to_datetime('2018-01-01'),
        end=pd.to_datetime('2019-01-01'),
    )
    print(calendar_series.head())
