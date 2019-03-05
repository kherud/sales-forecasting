from multiprocessing import cpu_count, Pool

from pymongo import MongoClient
from dateutil.relativedelta import relativedelta
from bson.binary import Binary
from tqdm import tqdm
import numpy as np
import datetime
import _pickle
import json

TRAIN_RATIO = 0.9
CURSOR_MAX_SIZE = 200000


def process_cursor(cursor):
    with open('agg-d-std.json') as f:
        std = json.load(f)

    client = MongoClient("localhost", 27017)
    db = client.dwh

    with tqdm(total=cursor[0], desc="Progress{}".format(cursor[2]), position=cursor[2], leave=True) as pbar:
        for doc in db.agg_d.find({"date": {"$gte": datetime.datetime(year=2012, month=1, day=1)}, "target": {"$lte": 250000}},
                                 no_cursor_timeout=True).skip(cursor[1]).limit(cursor[0]):

            time_filter = {"$and": [{"date": {"$gte": doc["date"] - relativedelta(days=365)}},
                                    {"date": {"$lt": doc["date"]}}]}

            if db.agg_d.find({"id": doc["id"], **time_filter}).count() < 50:
                pbar.update(1)
                continue

            record = {
                "x": np.empty((365, 5)),
                "y": (doc["target"] - std["mean"]) / std["std"]
            }

            for i in range(365):
                date = doc["date"] - relativedelta(days=365-i)
                wday = int(date.strftime("%w")) + 1
                record["x"][i][0] = -std["mean"] / std["std"]
                record["x"][i][1] = np.sin(2 * np.pi * wday / 7)
                record["x"][i][2] = np.cos(2 * np.pi * wday / 7)
                record["x"][i][3] = np.sin(2 * np.pi * date.month / 12)
                record["x"][i][4] = np.cos(2 * np.pi * date.month / 12)

            for day in db.agg_d.find({"id": doc["id"], **time_filter}):
                index = 365 - (doc["date"] - day["date"]).days
                record["x"][index][0] = (day["target"] - std["mean"]) / std["std"]
                wday = int(day["date"].strftime("%w")) + 1
                record["x"][index][1] = np.sin(2 * np.pi * wday / 7)
                record["x"][index][2] = np.cos(2 * np.pi * wday / 7)
                record["x"][index][3] = np.sin(2 * np.pi * doc["month"] / 12)
                record["x"][index][4] = np.cos(2 * np.pi * doc["month"] / 12)

            record["x"] = Binary(_pickle.dumps(record["x"].tolist()))

            if np.random.rand() < TRAIN_RATIO:
                db.train.insert(record)
            else:
                db.test.insert(record)

            pbar.update(1)


def find_cursors(threads):
    client = MongoClient("localhost", 27017)
    db = client.dwh
    col = db.agg_d
    amount_documents = col.find({"date": {"$gte": datetime.datetime(year=2012, month=1, day=1)}, "target": {"$lte": 250000}}).count()
    amounts_cursors = np.ceil(amount_documents / CURSOR_MAX_SIZE).astype(int)
    print(amount_documents, "total")
    print(amounts_cursors, "cursors")
    print(threads, "threads")
    interval = CURSOR_MAX_SIZE
    cursors = [(interval, interval * i, i) for i in range(amounts_cursors)]
    return cursors


if __name__ == "__main__":
    threads = cpu_count()

    cursors = find_cursors(threads)
    with Pool(processes=threads) as pool:
        pool.map(process_cursor, cursors)
