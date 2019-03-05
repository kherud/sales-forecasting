import _pickle
import json

from pymongo import MongoClient
import matplotlib.pyplot as plt
import pandas as pd

if __name__ == "__main__":
    cl = MongoClient("localhost", 27017)
    db = cl.dwh
    col = db.train

    with open('../aggregation/agg_m.json') as f:
        std = json.load(f)

    x = []
    for doc in col.find():
        for y in _pickle.loads(doc["x"]):
            if y < 5:
                x.append(y)
            # if x == -std["mean"] / std["std"]:
            #     n += 1
            # else:
            #     nn += 1

    # print(nn, n, nn/n)

    plt.hist(x, bins=100)
    plt.show()
