import json

from dateutil.relativedelta import relativedelta
from pymongo import MongoClient
import matplotlib.pyplot as plt
import pandas as pd
import datetime


def plot_days(item):
    with open('../aggregation/agg_m.json') as f:
        std = json.load(f)

    cl = MongoClient("localhost", 27017)
    db = cl.dwh
    col = db.agg_d

    x = []
    for i in range(375 * (2019 - 2011)):
        x.append(datetime.datetime(year=2019, month=1, day=31) - relativedelta(days=i))
    y = [0] * len(x)

    for doc in col.find({"id": item}):
        print(doc)
        index = x.index(doc["date"])
        y[index] = doc["target"]

    plt.figure(figsize=(20, 3))
    plt.xticks(rotation=70)
    plt.plot(x, y)
    plt.show()


if __name__ == "__main__":
    ITEM = "8e06f74469923d4ef47d5a920363b8b3"
    plot_days(ITEM)
