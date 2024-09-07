import akshare as ak
import pandas as pd
from src.Downloader.datadef import CNI, INDEX
import itertools
from tqdm import tqdm
from src.Downloader.TradeCalendar import CalendarDownloader
import time


class ComponentDownloader:
    def __init__(self):
        self.index_name = None
        self.start = None
        self.end = None

    def get(self) -> pd.DataFrame:
        month_ends = pd.date_range(self.start, self.end, freq='ME')
        component_list = []
        for month_end in tqdm(month_ends, desc="Downloading Components Weight"):
            month_start = month_end + pd.offsets.MonthBegin(-1)
            last_month = (month_end - pd.offsets.MonthEnd()).strftime("%Y%m")  # Use last month end data for this month
            component = ak.index_detail_cni(symbol=self.index_name.value, date=last_month)
            component.columns = ["date", "code", "name", "industry", "mktcap", "weight"]
            month_calendar = CalendarDownloader().build(month_start, month_end)
            monthly_component = pd.DataFrame(data=list(itertools.product(month_calendar, component["code"])))
            monthly_component.columns = ["date", "code"]
            component_list.append(monthly_component)
            time.sleep(2)
        component_df = pd.concat(component_list)
        return component_df

    def set_params(
            self,
            index_name: INDEX,
            start: pd.Timestamp,
            end: pd.Timestamp,
    ):
        self.index_name = index_name
        self.start = start
        self.end = end

    def build(
            self,
            index_name: INDEX,
            start: pd.Timestamp,
            end: pd.Timestamp,
    ):
        self.set_params(index_name, start, end)
        component_df = self.get()
        return component_df


if __name__ == '__main__':
    downloader = ComponentDownloader()
    components = downloader.build(
        index_name=CNI.GZ_300,
        start=pd.to_datetime('2018-01-01'),
        end=pd.to_datetime('2018-03-01'),
    )