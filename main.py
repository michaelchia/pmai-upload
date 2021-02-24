import itertools
import requests
import json
import time
import io

import pdblp

CCY_MAP = {
    "JPY": "JY",
    "EUR": "EU",
    "AUD": "AD",
    "NZD": "ND",
    "GBP": "BP",
    "HKD": "HD",
    "SGD": "SD",
    "USD": "US",
}
TYPE_MAP = {
    "XCCY": "BS",
    "IRS": "SW",
}
XCCY = {
    "type": "BS",
    "ccys": ["JY", "EU", "AD", "ND", "BP", "HD", "SD"],
    "periods": [1, 2, 3, 4, 5, 7, 10]
}
IRS = {
    "type": "SW",
    "ccys": ["US", "HD", "SD"],
    "periods": [1, 2, 3, 4, 5, 7, 10]
}
FXS = {
    "type": "",
    "ccys": ["HKD", "SGD", "CNH"],
    "periods": ["1M", "2M", "3M", "6M", "9M", "12M"]
}

START_DATE = "20140101"

BUCKET_NAME = "blpdata"
FOLDER = "raw/"

CRED_PATH = "cred.json.txt"

UPLOAD_URL = "https://pmai-297814.et.r.appspot.com/upload"


def main():
    # password
    with open(CRED_PATH) as f:
        auth = tuple(json.load(f))

    # blp
    blp = pdblp.BCon(timeout=10000)
    blp.start()

    tickers = []
    for prod in [XCCY, IRS, FXS]:
        type_, ccys, periods = prod["type"], prod["ccys"], prod["periods"]
        tickers += [
            f"{ccy}{type_}{period} Curncy"
            for ccy, period
            in itertools.product(ccys, periods)
        ]

    def _get_data(ticker):
        print(ticker)
        try:
            df = blp.bdh(ticker, "PX_LAST", start_date=START_DATE, end_date="20990101")
        except (ValueError, RuntimeError):
            print("retry " + ticker)
            time.sleep(2)
            df = blp.bdh(ticker, "PX_LAST", start_date=START_DATE, end_date="20990101")
        df.columns = list(df.columns.get_level_values(0))
        return df

    dfs = {ticker: _get_data(ticker)
           for ticker in tickers}

    for ticker, df in dfs.items():
        _upload_df(df, ticker, auth)

def _upload_df(df, ticker, auth):
    file = io.StringIO()
    file.filename = ticker + ".csv"
    df.to_csv(file, line_terminator="\n")
    file.seek(0)
    _upload_file(file, ticker, auth)


def _upload_file(file, ticker, auth):
    resp = requests.post(
        UPLOAD_URL,
        files={ticker: file},
        auth=auth,
        verify=False)
    if resp.status_code != 200:
        resp.raise_for_status()


if __name__ == "__main__":
    main()
    print("----------------DONE-----------------")
