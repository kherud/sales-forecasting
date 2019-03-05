import json

from pymongo import MongoClient
import matplotlib.pyplot as plt
import pandas as pd
from datetime import datetime

ITEM = "99cbb54686f8b94caf1382a4c730202a"

if __name__ == "__main__":
    with open('../aggregation/agg_m.json') as f:
        std = json.load(f)

    cl = MongoClient("localhost", 27017)
    db = cl.dwh
    col = db.histories

    doc = col.find_one({"id": ITEM})

    del doc["_id"]
    del doc["id"]

    x = pd.date_range('2011-01-01', '2019-01-01', freq='MS').strftime("%Y-%m").tolist()
    y = [-std["mean"] / std["std"]] * len(x)
    for k, v in doc.items():
        index = x.index(k)
        y[index] = v

    y = [i + abs(-std["mean"] / std["std"]) for i in y]

    fig = plt.figure(figsize=(14, 3))
    plt.xticks(rotation=70)
    plt.plot(x, y)

    plt.axvline(x="2018-01", color="red")
    plt.axvline(x="2019-01", color="red")

    plt.show()
