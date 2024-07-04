import os
import requests
from datetime import datetime, date
from dataclasses import dataclass
from typing import NamedTuple
import logging
import pandas as pd
import pytz
import pathlib

logger = logging.getLogger(__name__)
tz = pytz.timezone('Asia/Taipei')
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s %(levelname)s %(name)s %(message)s',
)

ErrorTickerNotFound = Exception("Ticker not found")

@dataclass(slots=True)
class TickerPrice():
    date: date
    open: float
    high: float
    low: float
    close: float
    volume: int
    adjusted_open: float
    adjusted_high: float
    adjusted_low: float
    adjusted_close: float
    adjusted_volume: int
    dividend_cash: float
    split_factor: float

    @classmethod
    def from_json(cls, json):
        return cls(
            date=datetime.fromisoformat(json["date"]).date(),
            open=json["open"],
            high=json["high"],
            low=json["low"],
            close=json["close"],
            volume=json["volume"],
            adjusted_open=json["adjOpen"],
            adjusted_high=json["adjHigh"],
            adjusted_low=json["adjLow"],
            adjusted_close=json["adjClose"],
            adjusted_volume=json["adjVolume"],
            dividend_cash=json["divCash"],
            split_factor=json["splitFactor"],
        )

class TickerInfo(NamedTuple):
    ticker: str
    name: str
    exchangeCode: str
    description: str
    startDate: date
    endDate: date

class TiingoClient:
    def __init__(self, api_key):
        self.headers = {"Content-Type": "application/json", "Authorization": f"Token {api_key}"}
    
    def get_ticker_prices(self, ticker: str, start_date: date, end_date: date) -> list[TickerPrice]:
        logger.info(f"Getting ticker prices for {ticker} from {start_date} to {end_date}")
        url = f"https://api.tiingo.com/tiingo/daily/{ticker}/prices"
        params = {
            "startDate": start_date.isoformat(),
            "endDate": end_date.isoformat(),
            "resampleFreq": "daily",
        }
        response = requests.get(url, params=params, headers=self.headers)
        return [TickerPrice.from_json(p) for p in response.json()]
    
    def get_ticker_info(self, ticker) -> TickerInfo:
        logger.info(f"Getting ticker info for {ticker}")
        url = f"https://api.tiingo.com/tiingo/daily/{ticker}"
        response = requests.get(url, headers=self.headers)
        if response.status_code != 200:
            logger.error(f"Failed to get ticker info for {ticker}: {response.status_code} {response.text}")
            raise ErrorTickerNotFound
        return TickerInfo(
            ticker=response.json()["ticker"],
            name=response.json()["name"],
            exchangeCode=response.json()["exchangeCode"],
            description=response.json()["description"],
            startDate=date.fromisoformat(response.json()["startDate"]),
            endDate=date.fromisoformat(response.json()["endDate"]),
        )

class TickerPriceStorage:
    def __init__(self, tiingo: TiingoClient, path='.prices'):
        self.tiingo = tiingo
        self.path = pathlib.Path(path)
        self.path.mkdir(parents=True, exist_ok=True)

    def read(self, ticker: str) -> pd.DataFrame:
        ticker_info = self.tiingo.get_ticker_info(ticker)
        filepath = self.path / f'{ticker}.parquet'
        if os.path.exists(filepath):
            df = pd.read_parquet(filepath, engine='pyarrow')
            if df['date'].min() == ticker_info.startDate and df['date'].max() == ticker_info.endDate:
                return df
        prices = self.tiingo.get_ticker_prices(ticker, ticker_info.startDate, ticker_info.endDate)
        df = pd.DataFrame(prices)
        df.to_parquet(filepath, engine='pyarrow')
        return pd.read_parquet(filepath, engine='pyarrow')


if __name__ == "__main__":
    client = TiingoClient(os.environ["TIINGO_API_KEY"])
    TickerPriceStorage(client).read("QQQ")
