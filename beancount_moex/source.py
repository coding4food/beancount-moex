import datetime
import decimal
import json
import re
from urllib.request import Request, urlopen


try:
    from beanprice import source
except ImportError:
    from beancount.prices import source


LATEST_URL = "https://iss.moex.com/iss/history/engines/{engine}/markets/{market}/securities/{symbol}.json?limit=5&sort_order=desc"
DATE_URL = "https://iss.moex.com/iss/history/engines/{engine}/markets/{market}/securities/{symbol}.json?from={date}&till={date}"

BOARD_LATEST_URL = "https://iss.moex.com/iss/history/engines/{engine}/markets/{market}/boards/{board}/securities/{symbol}.json?limit=5&sort_order=desc"
BOARD_DATE_URL = "https://iss.moex.com/iss/history/engines/{engine}/markets/{market}/boards/{board}/securities/{symbol}.json?from={date}&till={date}"

def _get_quote(ticker: str, date=None):
    """Fetch a exchangerate from ratesapi."""
    ticker_parts = ticker.split(":")

    if len(ticker_parts) == 4:
        engine, market, board, symbol = ticker_parts
        url_template = BOARD_DATE_URL if date is not None else BOARD_LATEST_URL
        url = url_template.format(engine=engine, market=market, board=board, symbol=symbol, date=date)
    elif len(ticker_parts) == 3:
        engine, market, symbol = ticker_parts
        url_template = DATE_URL if date is not None else LATEST_URL
        url = url_template.format(engine=engine, market=market, symbol=symbol, date=date)
    else:
        raise ValueError("Invalid ticker format")

    request = Request(url)
    response = urlopen(request)
    result = json.loads(response.read().decode())

    columns = result["history"]["columns"]
    data = result["history"]["data"][0]

    price = decimal.Decimal.from_float(data[columns.index("CLOSE")] or 0)

    if market == "bonds":
        price = (price / 100) * decimal.Decimal.from_float(data[columns.index("FACEVALUE")])

    price = price.quantize(decimal.Decimal("1.0000000000"))

    price_time = datetime.datetime.strptime(
        data[columns.index("TRADEDATE")], "%Y-%m-%d"
    ).replace(tzinfo=datetime.timezone.utc)

    return source.SourcePrice(price.normalize(), price_time, None)


class Source(source.Source):
    def get_latest_price(self, ticker):
        return _get_quote(ticker)

    def get_historical_price(self, ticker, time):
        return _get_quote(ticker, time)
