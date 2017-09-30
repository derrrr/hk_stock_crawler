import re
import requests
import os
from datetime import datetime
import time
import sys
import pandas as pd

"""
last update in 2017_0624
"""

requests.adapters.DEFAULT_RETRIES = 5

# """
# hinet proxy
hinet_proxy = {
    "http": "http://proxy.hinet.net:80",
    "https": "http://proxy.hinet.net:80",
}
# """


def _get_session():
    session = requests.session()
    headers = {
        "user-agent": "mozilla/5.0 (x11; linux x86_64) applewebkit/537.36"
                      "(khtml, like gecko) "
                      "chrome/46.0.2490.86 safari/537.36"}
    session.headers.update(headers)
    return session


def get_market_date():
    # MarketDate data is not up to date
    """
    date_url = "http://data.tsci.com.cn/script_data/MarketDate.js"
    res = rs.get(date_url).text
    res = rs.get(date_url, proxies=hinet_proxy).text
    print(res)
    time = re.search(""(.+)"", res)
    date_market = datetime.strptime(time.group(1), "%Y-%m-%d %H:%M")
    """

    # alternative: get market date form other website(aastocks)
    blocktrade_url = "http://www.aastocks.com/tc/stocks/analysis/blocktrade.aspx?symbol=0{}".format(ticker)
    blocktrade = rs.get(blocktrade_url, proxies=hinet_proxy).text
    str_date = re.search("最後更新\s(\d{4}/\d{2}/\d{2})", blocktrade).group(1)
    print("MarketDate: {}".format(str_date))
    date_market = datetime.strptime(str_date, "%Y/%m/%d")

    return date_market.strftime("%Y-%m-%d")


def hk_broker(ticker):
    date_market_str = get_market_date()

    # "...val={}" in url means the max of data search
    url = "http://data.tsci.com.cn/RDS.aspx?Code=E0{}&PkgType=11036&val={}".format(ticker, 5000)
    # response = rs.get(url)
    response = rs.get(url, proxies=hinet_proxy)

    path = "./hk_raw/{}".format(ticker)
    if not os.path.exists(path):
        os.makedirs(path, mode=0o777)

    raw_file = "{}/{}.HK_raw_{}.html".format(path, ticker, date_market_str)
    with open(raw_file , "w", encoding="utf8") as raw:
        raw.write(response.text)

    with open(raw_file, "r", encoding="utf-8") as file:
        json_like = file.read()

    # split to pandas readable format
    buy_json = json_like.split(",\"Sell\"", 1)[0].split("\"Buy\":", 1)[1]
    sell_json = json_like.split(",\"BrokerBuy\"", 1)[0].split(",\"Sell\":", 1)[1]

    # json to dataframe and replace blank in "BrokerNo"
    df_buy = pd.read_json(buy_json, orient="records")
    df_sell = pd.read_json(sell_json, orient="records")
    df_buy["BrokerNo"] = df_buy["BrokerNo"].replace("\s", "-", regex=True)
    df_sell["BrokerNo"] = df_sell["BrokerNo"].replace("\s", "-", regex=True)

    # split "Broker_no" and "broker", retain columns
    df_buy[["broker_no", "broker"]] = df_buy["BrokerNo"].str.split(".", expand=True)
    df_sell[["broker_no", "broker"]] = df_sell["BrokerNo"].str.split(".", expand=True)
    df_buy = df_buy[["broker_no", "broker", "shares", "AV"]]
    df_sell = df_sell[["broker_no", "broker", "shares", "AV"]]

    # rename columns
    buy_cols = ["buy_share", "buy_avg_price"]
    df_buy.rename(columns=dict(zip(df_buy.columns[-2:], buy_cols)), inplace=True)
    sell_cols = ["sell_share", "sell_avg_price"]
    df_sell.rename(columns=dict(zip(df_sell.columns[-2:], sell_cols)), inplace=True)

    # concat buy and sell frame
    df = pd.concat([df_buy, df_sell], ignore_index=True).fillna(0)

    # K → 1,000; M → 1,000,000, share → k_share
    df.loc[:,["buy_share","sell_share"]] = df[["buy_share","sell_share"]].astype(str)
    df["buy_share"] = df["buy_share"].str.replace("K", "*1000").str.replace("M", "*1000000").apply(lambda x: eval(x)/1000).round(0).astype("int64")
    df["sell_share"] = df["sell_share"].str.replace("K", "*1000").str.replace("M", "*1000000").apply(lambda x: eval(x)/1000).round(0).astype("int64")
    k_cols = ["buy_k_share","sell_k_share"]
    df.rename(columns=dict(zip(df.columns[[3, -1]], k_cols)), inplace=True)

    # groupby and sort
    gs = df.groupby(["broker_no", "broker"]).sum().reset_index()
    gs = gs.sort_values(by=["buy_k_share", "broker_no"], ascending=False)
    gs["net_k_share"] = gs["buy_k_share"] - gs["sell_k_share"]

    # move columns order
    cols = list(df)
    cols.insert(2, cols.pop(cols.index("buy_k_share")))
    cols.insert(4, cols.pop(cols.index("sell_k_share")))
    df = df.loc[:, cols]

    # insert date and ticker
    gs.insert(0, "date", date_market_str)
    gs.insert(1, "ticker", ticker + ".HK")

    output_folder = "./hk_broker/{}".format(ticker)
    if not os.path.exists(output_folder):
        os.makedirs(output_folder, mode=0o777)

    # output to csv
    output_file = "{}/{}.HK_broker_{}.csv".format(output_folder, ticker, date_market_str)
    gs.to_csv(path_or_buf=output_file, index=False, encoding="utf-8-sig")

    print("{} done!".format(output_file[-28:]))


try:
    ticker = str(input("Input the 4-digit HK ticker:"))
    if len(ticker) != 4 or not ticker.isdigit():
        raise ValueError()
except ValueError:
    print("Not a 4-digit ticker!")
    sys.exit(0)


start_time = time.time()

rs = _get_session()
rs.keep_alive = False
hk_broker(ticker)

print("--- {:.2f} sec spent ---".format(time.time() - start_time))