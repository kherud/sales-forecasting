from dateutil.relativedelta import relativedelta

from datascience.dwh.nn.model import get_model
from pymongo import MongoClient
import matplotlib.pyplot as plt
import numpy as np
import json


ITEM = "ee0831d47371cc82d2fe65a0faf1906c"

with open('../../aggregation/agg_m.json') as f:
    std = json.load(f)

with open('../../aggregation/nn-std.json') as f:
    std2 = json.load(f)


def create_record(item, date):
    cl = MongoClient("localhost", 27017)
    db = cl.dwh
    col = db.histories

    hist = col.find_one({"id": item})
    x = [[], []]

    for i in range(12):
        date_ = date - relativedelta(months=i + 1)
        month = "{:02d}-{:02d}".format(date_.year, date_.month)

        if month in hist:
            x[0].insert(0, [hist[month]])
        else:
            x[0].insert(0, [-std["mean"] / std["std"]])

    # yweek = int(date.strftime("%W")) + 1
    wday = int(date.strftime("%w")) + 1
    # mday = int(date.strftime("%d"))
    x[1].append(np.sin(2 * np.pi * wday / 7))
    x[1].append(np.cos(2 * np.pi * wday / 7))
    x[1].append(np.sin(2 * np.pi * doc["month"] / 12))
    x[1].append(np.cos(2 * np.pi * doc["month"] / 12))
    # x.append(mday / calendar.monthrange(doc["year"], doc["month"])[1])
    # x.append(yweek / 53)
    x[1].append((doc["year"] - 2011) / (2019 - 2011))

    return x


if __name__ == "__main__":
    model = get_model()
    model.load_weights("dwh_nn.hdf5")

    cl = MongoClient("localhost", 27017)
    db = cl.dwh
    col = db.agg_d

    x, y = [], []

    for doc in col.find({"id": ITEM}).sort("date", 1):
        x.append(doc["date"])
        y.append(doc["target"])

    plt.figure(figsize=(20, 3))
    plt.plot(x, y, color="blue")

    last_date = x[-1]
    dates = []
    x = [[], []]
    for i in range(365):
        d = last_date + relativedelta(days=i)
        dates.append(d)
        r = create_record(ITEM, d)
        x[0].append(r[0])
        x[1].append(r[1])

    y = model.predict(x)

    plt.plot(dates, [i[0] * std2["std"] + std2["mean"] for i in y], color="orange")
    # print(y, [i[0] * std2["std"] + std2["mean"] for i in y])

    plt.show()
