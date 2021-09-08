import dateutil.parser
import json
import requests
import time

from alpha_vantage.timeseries import TimeSeries
from decouple import config
from tqdm import tqdm


CALLS_PER_MINUTE = 5
TIME_SLEEP = 60

DATA_DIR = 'data/'
TICKERS = ['MMM', 'ABT', 'ABBV', 'ABMD', 'ACN', 'AAPL', 'GOOG', 'AMC', 'LUV',
           'AMZN']
TOKEN = config('API_KEY')

START_DATE = dateutil.parser.parse('2016-01-01')


def get_stock_data(tickers=TICKERS, start_date=START_DATE, token=TOKEN):
    """
    A function to get financial data for a given list of tickers and save to a
    JSON file

    :param tickers: input tickers
    :param start_date: keep data after this date
    :param token: Alpha Vantage API key
    """
    n_requests = 0
    for ticker in tqdm(tickers):
        n_requests += 1
        if n_requests > CALLS_PER_MINUTE:
            time.sleep(TIME_SLEEP)
            n_requests = 0
        ts = TimeSeries(key=token, output_format='pandas')
        stocks = ts.get_daily_adjusted(ticker, outputsize='full')[0]
        stocks.columns = stocks.columns.map(lambda x: x.split('.')[1].strip())
        stocks = stocks[stocks.index > start_date]
        stocks.reset_index(level=0, inplace=True)
        stocks['date'] = stocks['date'].dt.strftime('%Y-%m-%d')
        stocks.to_json(
            f'{DATA_DIR}{ticker}_financial.json',
            orient='records',
            indent=4,
            date_unit='ns'
        )


def get_fundamentals(tickers=TICKERS, token=TOKEN):
    """
    A function to get fundamental company data for a given list of tickers and
    save to a JSON file

    :param tickers: input tickers
    :param token: Alpha Vantage API key
    """
    fundamentals = []
    n_requests = 0
    for ticker in tqdm(tickers):
        n_requests += 1
        if n_requests > CALLS_PER_MINUTE:
            time.sleep(TIME_SLEEP)
            n_requests = 0
        url = f'https://www.alphavantage.co/query?function=OVERVIEW' \
              f'&symbol={ticker}&apikey={token}'
        r = requests.get(url)
        fundamentals.append(r.json())
    with open(f'{DATA_DIR}fundamentals.json', 'w') as f:
        json.dump(fundamentals, f, indent=4)


if __name__ == '__main__':
    get_stock_data()
    get_fundamentals()
