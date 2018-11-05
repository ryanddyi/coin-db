import json
import requests
import pandas as pd
import numpy as np


def get_full_list():
    response = requests.get("https://api.coinmarketcap.com/v2/listings/")
    if response.status_code == 200:
        data_all = json.loads(response.content.decode('utf-8'))['data']
        full_list = []
        for data in data_all:
            full_list.append(data['website_slug'])
        return full_list
    else:
        print("no response from API")
        return None


def get_earlist_list(first_k):
    response = requests.get("https://api.coinmarketcap.com/v2/listings/")
    if response.status_code == 200:
        data_all = json.loads(response.content.decode('utf-8'))['data']
        full_list = []
        for data in data_all[:first_k]:
            full_list.append(data['website_slug'])
        return full_list
    else:
        print("no response from API")
        return None


def get_top_k_list(top_k):
    url = "https://api.coinmarketcap.com/v2/ticker/?start=1&limit=" + str(top_k) + "&sort=market_cap&structure=array"
    response = requests.get(url)
    if response.status_code == 200:
        data_all = json.loads(response.content.decode('utf-8'))['data']
        top_k_list = []
        for data in data_all:
            top_k_list.append(data['website_slug'])
        return top_k_list
    else:
        print("no response from API")
        return None


def name_to_symbol():
    response = requests.get("https://api.coinmarketcap.com/v2/listings/")
    if response.status_code == 200:
        data_all = json.loads(response.content.decode('utf-8'))['data']
        dict_to_symbol = {}
        for data in data_all:
            dict_to_symbol[data['website_slug']] = data['symbol']
        return dict_to_symbol


def volatility_factor(series_ret, window_size):
    series_vol = pd.Series()
    series = series_ret - series_ret.mean()
    expo_weight = 0.99
    weight_vec = [expo_weight ** (window_size - i) for i in range(window_size)]

    for i in range(window_size, len(series)):
        series_vol[series.index[i]] = np.sqrt((weight_vec * series[(i - window_size):i] ** 2).mean())

    return series_vol


def momentum_factor(series_price, series_vol, window_size):
    min_size = int(window_size / 6)
    half_size = int(window_size / 2)
    momentum = pd.Series()
    # ignoring risk-free rate

    for i in range(window_size + min_size, len(series_price)):
        momentum[series_price.index[i]] = (0.5 * series_price[i-min_size] / series_price[i-min_size-window_size] +
                                           0.5 * series_price[i-min_size] / series_price[i-min_size-half_size] - 1) \
                                          / series_vol[series_price.index[i]]
    return momentum

