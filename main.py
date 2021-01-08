import itertools
import requests
import json

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
    "ccys": ["JY", "EU", "AD", "ND", "GB", "HD", "SD"],
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

CRED_PATH = "cred.json"

UPLOAD_URL = "https://pmai-297814.et.r.appspot.com/upload"

def main():
    # password
    with open(CRED_PATH) as f:
        auth = json.load(f)

    # blp
    blp = pdblp.BCon(timeout=5000)
    blp.start()

    tickers = []
    for prod in [XCCY, IRS, FXS]:
        type_, ccys, periods = prod["type"], prod["ccys"], prod["periods"]
        tickers += [
            f"{ccy}{type_}{period} Curncy"
            for ccy, period
            in itertools.product(ccys, periods)
        ]

    df = blp.bdh(tickers, "PX_LAST", start_date=START_DATE, end_date="20990101")
    df.columns = list(df.columns.get_level_values(0))

    for ticker in tickers:
        _upload_column(df, ticker)


def _upload_column(df, ticker):
    df = df[[ticker]]


def _upload_file(file, auth):
    resp = requests.post(
        "http://127.0.0.1:8080/upload",
        files={"file": file},
        auth=auth)
    if resp.status_code != 200:
        resp.raise_for_status()