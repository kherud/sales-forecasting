from pymongo import MongoClient
from tqdm import tqdm

pipeline = [
    {"$group": {
        "_id": "$id",
        "hist": {"$push": {"date": "$date", "target": "$target_std"}},
    }}
]

cl = MongoClient("localhost", 27017)
db = cl.dwh

total = len(db.agg_m.distinct("id"))

db.histories.create_index("id", unique=False)

with tqdm(total=total, desc="Progress") as pbar:
    for doc in db.agg_m.aggregate(pipeline, allowDiskUse=True):
        history = {"id": doc["_id"]}
        for x in doc["hist"]:
            date = "{:02d}-{:02d}".format(x["date"].year, x["date"].month)
            history[date] = x["target"]

        db.histories.insert(history)
        pbar.update(1)
