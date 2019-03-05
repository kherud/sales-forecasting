import _pickle
import pandas as pd

from pymongo import MongoClient


if __name__ == "__main__":
    cl = MongoClient("localhost", 27017)
    db = cl.dwh
    col = db.train

    columns = ["Monat{}".format(i) for i in range(1, 13)]
    columns.append("Sin_Day")
    columns.append("Cos_Day")
    columns.append("Sin_Month")
    columns.append("Cos_Month")
    columns.append("Year")

    x = [_pickle.loads(doc["x"]) for doc in col.aggregate([{"$sample": {"size": 100}}])]

    df = pd.DataFrame(x, columns=columns)
    df.to_csv("../example_data/example_train.csv", sep=";", index=None)
